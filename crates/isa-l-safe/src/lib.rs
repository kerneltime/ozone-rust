//! Lifetime-safe wrappers over `isa-l-sys`.
//!
//! Reed-Solomon erasure coding with Cauchy generator matrices over GF(2^8),
//! byte-equivalent to Apache Ozone's existing Java EC implementation
//! (which is itself a port of ISA-L).
//!
//! See: notetaker/Projects/Apache Ozone/S3 Gateway Rust/
//!      2026-05-23 EC Implementation Spec ISA-L FFI.md
//!      2026-05-23 Erasure Coding Implementation.md

#![deny(missing_docs)]

use thiserror::Error;

/// Configuration for a Reed-Solomon `(k, p)` coder.
///
/// Production Ozone profiles: RS-3-2, RS-6-3, RS-10-4. Other valid `(k, p)`
/// shapes are accepted but not officially supported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EcConfig {
    /// `k` — number of data shards per stripe.
    pub data: usize,
    /// `p` — number of parity shards per stripe.
    pub parity: usize,
}

impl EcConfig {
    /// Total number of shards per stripe (data + parity).
    #[inline]
    pub const fn total(&self) -> usize {
        self.data + self.parity
    }
}

/// Errors raised by encoder/decoder construction or invocation.
#[derive(Debug, Error)]
pub enum EcError {
    /// Caller passed the wrong number of input or output buffers.
    #[error("buffer count mismatch: expected {expected}, got {got}")]
    BufferCount {
        /// Number of buffers the API expected.
        expected: usize,
        /// Number of buffers the caller passed.
        got: usize,
    },
    /// Caller passed buffers of unequal length.
    #[error("buffer length mismatch: expected {expected}, got {got}")]
    BufferLen {
        /// Expected per-buffer length in bytes.
        expected: usize,
        /// Actual per-buffer length the caller passed.
        got: usize,
    },
    /// `(k, p)` outside ISA-L's supported range.
    #[error("invalid EC config: data={data} parity={parity} (must satisfy 1<=data<=32, 1<=parity<=32)")]
    InvalidConfig {
        /// `k`.
        data: usize,
        /// `p`.
        parity: usize,
    },
}

/// Reed-Solomon encoder. Holds the precomputed Cauchy generator matrix and
/// per-element multiplication tables for the configured `(k, p)`.
///
/// Thread-safe for shared `&self` use: `encode` does not mutate internal
/// state. Wrap in `Arc<Encoder>` to share across tokio tasks.
pub struct Encoder {
    cfg: EcConfig,
    /// (k+p) × k bytes. Top k×k is the identity; bottom p×k is the Cauchy
    /// parity matrix.
    encode_matrix: Vec<u8>,
    /// k × p × 32 bytes. ISA-L's precomputed GF multiplication tables, used
    /// by `ec_encode_data` for SIMD-friendly inner loops.
    g_tbls: Vec<u8>,
}

impl Encoder {
    /// Build a new encoder for `cfg`. Cheap (~hundreds of bytes of state); a
    /// few microseconds. Construct once and reuse across stripes.
    pub fn new(cfg: EcConfig) -> Result<Self, EcError> {
        validate_cfg(cfg)?;

        let m = cfg.total();
        let mut encode_matrix = vec![0u8; m * cfg.data];
        let mut g_tbls = vec![0u8; cfg.data * cfg.parity * 32];

        // SAFETY: buffers are sized as ISA-L requires; ISA-L only writes
        // within its declared output size; `gf_gen_cauchy1_matrix` has no
        // failure modes other than invalid arguments (which we validated).
        unsafe {
            isa_l_sys::gf_gen_cauchy1_matrix(
                encode_matrix.as_mut_ptr(),
                m as i32,
                cfg.data as i32,
            );

            // ec_init_tables consumes only the *parity rows* of the encode
            // matrix (k*k bytes in, p*k bytes from there on are the parity
            // rows).
            let parity_rows_offset = cfg.data * cfg.data;
            isa_l_sys::ec_init_tables(
                cfg.data as i32,
                cfg.parity as i32,
                encode_matrix[parity_rows_offset..].as_mut_ptr(),
                g_tbls.as_mut_ptr(),
            );
        }

        Ok(Self {
            cfg,
            encode_matrix,
            g_tbls,
        })
    }

    /// The `(k, p)` configuration this encoder was built for.
    #[inline]
    pub fn config(&self) -> EcConfig {
        self.cfg
    }

    /// Encode `data` (k chunks each `len` bytes) into `parity` (p chunks each
    /// `len` bytes).
    ///
    /// All chunks must be `len` bytes (the caller pre-pads partial last
    /// stripes per Ozone's zero-pad-then-truncate rule — see EC spec).
    pub fn encode(
        &self,
        len: usize,
        data: &[&[u8]],
        parity: &mut [&mut [u8]],
    ) -> Result<(), EcError> {
        if data.len() != self.cfg.data {
            return Err(EcError::BufferCount {
                expected: self.cfg.data,
                got: data.len(),
            });
        }
        if parity.len() != self.cfg.parity {
            return Err(EcError::BufferCount {
                expected: self.cfg.parity,
                got: parity.len(),
            });
        }
        for d in data {
            if d.len() != len {
                return Err(EcError::BufferLen {
                    expected: len,
                    got: d.len(),
                });
            }
        }
        for p in &*parity {
            if p.len() != len {
                return Err(EcError::BufferLen {
                    expected: len,
                    got: p.len(),
                });
            }
        }

        // ISA-L's signature takes `*mut *mut u8` for both inputs and outputs.
        // The inputs are not mutated; we cast our `&[u8]` pointers to mut to
        // satisfy the FFI signature, but the C side does not write to them.
        let data_ptrs: Vec<*mut u8> = data.iter().map(|d| d.as_ptr() as *mut u8).collect();
        let parity_ptrs: Vec<*mut u8> = parity.iter_mut().map(|p| p.as_mut_ptr()).collect();

        // SAFETY:
        // - All data/parity pointers live for the duration of this call (we
        //   collected them from refs inside this function).
        // - The Vec<*mut u8> arrays are at least cfg.data / cfg.parity long.
        // - g_tbls is valid for the lifetime of self.
        unsafe {
            isa_l_sys::ec_encode_data(
                len as i32,
                self.cfg.data as i32,
                self.cfg.parity as i32,
                self.g_tbls.as_ptr() as *mut u8,
                data_ptrs.as_ptr() as *mut *mut u8,
                parity_ptrs.as_ptr() as *mut *mut u8,
            );
        }
        Ok(())
    }
}

fn validate_cfg(cfg: EcConfig) -> Result<(), EcError> {
    if cfg.data == 0 || cfg.data > 32 || cfg.parity == 0 || cfg.parity > 32 {
        return Err(EcError::InvalidConfig {
            data: cfg.data,
            parity: cfg.parity,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_config_rejected() {
        assert!(matches!(
            Encoder::new(EcConfig { data: 0, parity: 1 }),
            Err(EcError::InvalidConfig { .. })
        ));
        assert!(matches!(
            Encoder::new(EcConfig { data: 1, parity: 0 }),
            Err(EcError::InvalidConfig { .. })
        ));
        assert!(matches!(
            Encoder::new(EcConfig { data: 33, parity: 1 }),
            Err(EcError::InvalidConfig { .. })
        ));
    }

    #[test]
    fn encoder_constructs_for_production_profiles() {
        for cfg in [
            EcConfig { data: 3, parity: 2 },   // RS-3-2
            EcConfig { data: 6, parity: 3 },   // RS-6-3
            EcConfig { data: 10, parity: 4 },  // RS-10-4
        ] {
            let enc = Encoder::new(cfg).unwrap_or_else(|_| panic!("construct {cfg:?}"));
            assert_eq!(enc.config(), cfg);
            // Top k×k is identity matrix.
            for row in 0..cfg.data {
                for col in 0..cfg.data {
                    let expected = if row == col { 1 } else { 0 };
                    assert_eq!(
                        enc.encode_matrix[row * cfg.data + col],
                        expected,
                        "encode_matrix[{row},{col}] not identity for {cfg:?}"
                    );
                }
            }
            // Parity rows must be non-trivial.
            let parity_rows = &enc.encode_matrix[(cfg.data * cfg.data)..];
            assert!(
                parity_rows.iter().any(|&b| b != 0),
                "parity rows all zero for {cfg:?}"
            );
        }
    }

    #[test]
    fn all_zero_data_yields_all_zero_parity_rs_6_3() {
        let enc = Encoder::new(EcConfig { data: 6, parity: 3 }).unwrap();
        let zeros = vec![0u8; 1024];
        let data: Vec<&[u8]> = (0..6).map(|_| zeros.as_slice()).collect();
        let mut parity_storage = vec![vec![0u8; 1024]; 3];
        let mut parity: Vec<&mut [u8]> = parity_storage
            .iter_mut()
            .map(|v| v.as_mut_slice())
            .collect();
        enc.encode(1024, &data, &mut parity).unwrap();
        for (i, p) in parity_storage.iter().enumerate() {
            assert!(
                p.iter().all(|&b| b == 0),
                "parity slot {i} not all-zero for all-zero input"
            );
        }
    }

    #[test]
    fn non_zero_data_yields_deterministic_parity_rs_6_3() {
        // Two runs over the same input should give byte-identical parity.
        // Establishes the encoder is stateless across calls.
        let enc = Encoder::new(EcConfig { data: 6, parity: 3 }).unwrap();
        let make_input = || -> Vec<Vec<u8>> {
            (0..6u8)
                .map(|i| (0..1024).map(|j| ((i as u32 * 17 + j) & 0xff) as u8).collect())
                .collect()
        };

        let make_outputs = |enc: &Encoder, data_vec: &[Vec<u8>]| -> Vec<Vec<u8>> {
            let data: Vec<&[u8]> = data_vec.iter().map(|v| v.as_slice()).collect();
            let mut parity_storage = vec![vec![0u8; 1024]; 3];
            let mut parity: Vec<&mut [u8]> = parity_storage
                .iter_mut()
                .map(|v| v.as_mut_slice())
                .collect();
            enc.encode(1024, &data, &mut parity).unwrap();
            parity_storage
        };

        let in_a = make_input();
        let in_b = make_input();
        let out_a = make_outputs(&enc, &in_a);
        let out_b = make_outputs(&enc, &in_b);
        assert_eq!(out_a, out_b, "encoder not deterministic across calls");

        for (i, p) in out_a.iter().enumerate() {
            assert!(
                p.iter().any(|&b| b != 0),
                "parity slot {i} unexpectedly all-zero for non-zero input"
            );
        }
    }

    #[test]
    fn buffer_count_mismatch_rejected() {
        let enc = Encoder::new(EcConfig { data: 3, parity: 2 }).unwrap();
        let zeros = vec![0u8; 16];
        let data_short: Vec<&[u8]> = vec![zeros.as_slice(); 2]; // need 3
        let mut parity_storage = vec![vec![0u8; 16]; 2];
        let mut parity: Vec<&mut [u8]> = parity_storage.iter_mut().map(|v| v.as_mut_slice()).collect();
        assert!(matches!(
            enc.encode(16, &data_short, &mut parity),
            Err(EcError::BufferCount { .. })
        ));
    }
}
