//! Ozone-specific Reed-Solomon Cauchy EC layer.
//!
//! Wraps `isa-l-safe`'s low-level `(k, p)` encoder with the Ozone
//! stripe-layout conventions (chunk size, partial-stripe zero-pad rule,
//! per-replica chunk routing). Targets byte-equivalence with Apache Ozone's
//! existing Java EC implementation.
//!
//! See: notetaker/Projects/Apache Ozone/S3 Gateway Rust/
//!      2026-05-23 Erasure Coding Implementation.md (Phase 1 ground truth)
//!      2026-05-23 EC Implementation Spec ISA-L FFI.md (Phase 2 design)

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use isa_l_safe::{EcConfig as InnerCfg, EcError as InnerError, Encoder as InnerEncoder};
use thiserror::Error;

/// Ozone EC profile: the `(data, parity, ec_chunk_size)` triple.
///
/// Production defaults from Apache Ozone:
/// - `rs-3-2-1024k` — small clusters
/// - `rs-6-3-1024k` — most common
/// - `rs-10-4-1024k` — large clusters
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Profile {
    /// `k` — data shards per stripe.
    pub data: usize,
    /// `p` — parity shards per stripe.
    pub parity: usize,
    /// Per-chunk byte size. Ozone default: 1 MiB.
    pub chunk_size: usize,
}

impl Profile {
    /// `rs-3-2-1024k`.
    pub const RS_3_2_1MIB: Self = Self {
        data: 3,
        parity: 2,
        chunk_size: 1024 * 1024,
    };
    /// `rs-6-3-1024k`. The most common production setting.
    pub const RS_6_3_1MIB: Self = Self {
        data: 6,
        parity: 3,
        chunk_size: 1024 * 1024,
    };
    /// `rs-10-4-1024k`.
    pub const RS_10_4_1MIB: Self = Self {
        data: 10,
        parity: 4,
        chunk_size: 1024 * 1024,
    };

    /// Number of bytes in a *full* stripe across all data shards.
    #[inline]
    pub const fn stripe_size(&self) -> usize {
        self.data * self.chunk_size
    }

    /// Total shards per stripe (data + parity).
    #[inline]
    pub const fn total(&self) -> usize {
        self.data + self.parity
    }
}

/// Errors from the Ozone EC layer.
#[derive(Debug, Error)]
pub enum EcError {
    /// Underlying ISA-L wrapper error.
    #[error(transparent)]
    Inner(#[from] InnerError),
    /// Caller passed a stripe with too many bytes (>= one full stripe).
    /// Use [`Encoder::encode_stripe`] for full stripes and reserve
    /// [`Encoder::encode_partial`] for the trailing partial.
    #[error("partial-stripe byte count {got} >= full stripe {full}; use encode_stripe")]
    NotPartial {
        /// Bytes supplied for the partial.
        got: usize,
        /// Bytes in a full stripe.
        full: usize,
    },
}

/// Reed-Solomon stripe encoder for a single profile.
pub struct Encoder {
    profile: Profile,
    inner: InnerEncoder,
}

impl Encoder {
    /// Build an encoder for `profile`.
    pub fn new(profile: Profile) -> Result<Self, EcError> {
        let inner = InnerEncoder::new(InnerCfg {
            data: profile.data,
            parity: profile.parity,
        })?;
        Ok(Self { profile, inner })
    }

    /// Profile this encoder was built for.
    #[inline]
    pub fn profile(&self) -> Profile {
        self.profile
    }

    /// Encode one *full* stripe. `data` is `data×chunk_size` bytes;
    /// `parity` receives `parity×chunk_size` bytes.
    pub fn encode_stripe(
        &self,
        data: &[&[u8]],
        parity: &mut [&mut [u8]],
    ) -> Result<(), EcError> {
        self.inner.encode(self.profile.chunk_size, data, parity)?;
        Ok(())
    }

    /// Encode a *partial* trailing stripe.
    ///
    /// The Ozone partial-stripe rule (matching the Java implementation):
    ///
    /// 1. The first `n_full_data` data shards are full (each `chunk_size` bytes).
    /// 2. The next data shard, if any, holds `partial_bytes < chunk_size` bytes.
    /// 3. The remaining data shards are entirely empty.
    /// 4. All shards (including the empty ones) are zero-padded to `chunk_size`
    ///    BEFORE encoding.
    /// 5. Parity is computed against the padded data.
    /// 6. The caller is responsible for truncating data shards back to their
    ///    actual byte counts before writing to disk; parity shards are stored
    ///    at full `chunk_size`.
    ///
    /// This API enforces (4) and (5) by accepting zero-padded inputs and
    /// computing parity against them. The truncation in step (6) is the
    /// caller's responsibility.
    pub fn encode_partial(
        &self,
        data_zero_padded: &[&[u8]],
        parity: &mut [&mut [u8]],
    ) -> Result<(), EcError> {
        // All data shards must be exactly chunk_size, zero-padded as needed.
        for d in data_zero_padded {
            if d.len() != self.profile.chunk_size {
                return Err(EcError::Inner(InnerError::BufferLen {
                    expected: self.profile.chunk_size,
                    got: d.len(),
                }));
            }
        }
        self.inner
            .encode(self.profile.chunk_size, data_zero_padded, parity)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_constants_are_what_they_look_like() {
        assert_eq!(Profile::RS_6_3_1MIB.total(), 9);
        assert_eq!(Profile::RS_6_3_1MIB.stripe_size(), 6 * 1024 * 1024);
        assert_eq!(Profile::RS_10_4_1MIB.total(), 14);
    }

    #[test]
    fn encoder_builds_for_each_production_profile() {
        for p in [
            Profile::RS_3_2_1MIB,
            Profile::RS_6_3_1MIB,
            Profile::RS_10_4_1MIB,
        ] {
            let enc = Encoder::new(p).unwrap();
            assert_eq!(enc.profile(), p);
        }
    }

    #[test]
    fn full_stripe_rs_6_3_produces_non_zero_parity() {
        // Use a tiny chunk size for speed; bypass the 1 MiB constant.
        let profile = Profile {
            data: 6,
            parity: 3,
            chunk_size: 4096,
        };
        let enc = Encoder::new(profile).unwrap();

        // Data: shard i filled with byte (i + 1).
        let data_storage: Vec<Vec<u8>> = (0..6)
            .map(|i| vec![(i + 1) as u8; profile.chunk_size])
            .collect();
        let data: Vec<&[u8]> = data_storage.iter().map(|v| v.as_slice()).collect();
        let mut parity_storage = vec![vec![0u8; profile.chunk_size]; 3];
        let mut parity: Vec<&mut [u8]> = parity_storage
            .iter_mut()
            .map(|v| v.as_mut_slice())
            .collect();

        enc.encode_stripe(&data, &mut parity).unwrap();

        for (i, p) in parity_storage.iter().enumerate() {
            assert!(
                p.iter().any(|&b| b != 0),
                "parity slot {i} unexpectedly all-zero"
            );
        }
    }

    #[test]
    fn partial_stripe_rejects_unpadded_data() {
        let profile = Profile {
            data: 3,
            parity: 2,
            chunk_size: 4096,
        };
        let enc = Encoder::new(profile).unwrap();

        // Caller forgets to zero-pad: shard sizes are wrong.
        let short = vec![0u8; 100];
        let data: Vec<&[u8]> = vec![short.as_slice(); 3];
        let mut parity_storage = vec![vec![0u8; profile.chunk_size]; 2];
        let mut parity: Vec<&mut [u8]> = parity_storage.iter_mut().map(|v| v.as_mut_slice()).collect();

        let err = enc.encode_partial(&data, &mut parity).unwrap_err();
        assert!(matches!(
            err,
            EcError::Inner(InnerError::BufferLen { .. })
        ));
    }
}
