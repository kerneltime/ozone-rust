//! Lifetime-safe wrappers over `isa-l-sys`.
//!
//! Reed-Solomon erasure coding with Cauchy generator matrices over GF(2^8),
//! byte-equivalent to Apache Ozone's existing Java EC implementation
//! (which is itself a port of ISA-L).
//!
//! Two operations:
//! - [`Encoder`] computes parity for a full stripe (the write path).
//! - [`Decoder`] rebuilds erased shards from any `k` survivors (the
//!   degraded-read path and the reconstruction path).
//!
//! Both MUST derive their generator matrix from the same [`build_encode_matrix`]
//! so that recovered bytes match what the encoder originally produced — and,
//! transitively, what Ozone's Java coder produces.
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
    /// A shard index was out of range (`>= k+p`).
    #[error("shard index {index} out of range (must be < {bound})")]
    BadIndex {
        /// The offending index.
        index: usize,
        /// Exclusive upper bound (`k+p`).
        bound: usize,
    },
    /// The same shard index appeared twice in the source list.
    #[error("duplicate shard index {index} in source list")]
    DuplicateIndex {
        /// The repeated index.
        index: usize,
    },
    /// A shard index appeared in both the source list and the erased list.
    #[error("shard index {index} listed as both a source and erased")]
    Overlap {
        /// The conflicting index.
        index: usize,
    },
    /// The chosen `k` sources do not form an invertible matrix. For a Cauchy
    /// generator matrix with `k` distinct survivor rows this is unreachable;
    /// surfaced rather than panicked for defense in depth.
    #[error("decode matrix is singular for the given source set")]
    Singular,
}

/// Build the `(k+p) × k` Cauchy generator matrix for `cfg`.
///
/// The top `k×k` block is the identity (so the first `k` "encoded" shards are
/// the data verbatim); the bottom `p×k` block holds the Cauchy parity
/// coefficients. Both [`Encoder`] and [`Decoder`] derive their tables from this
/// identical matrix — byte-equivalence with Ozone's Java EC depends on it.
///
/// `cfg` must already be validated (`validate_cfg`); this calls into ISA-L
/// directly and trusts its `(m, k)` arguments.
fn build_encode_matrix(cfg: EcConfig) -> Vec<u8> {
    let m = cfg.total();
    let mut encode_matrix = vec![0u8; m * cfg.data];
    // SAFETY: encode_matrix is exactly m*k bytes, the size
    // `gf_gen_cauchy1_matrix` writes; the function has no failure modes for
    // in-range (m, k), which the caller validated.
    unsafe {
        isa_l_sys::gf_gen_cauchy1_matrix(encode_matrix.as_mut_ptr(), m as i32, cfg.data as i32);
    }
    encode_matrix
}

/// Reed-Solomon encoder. Holds the precomputed GF multiplication tables for the
/// configured `(k, p)`.
///
/// Thread-safe for shared `&self` use: `encode` does not mutate internal
/// state. Wrap in `Arc<Encoder>` to share across tokio tasks.
pub struct Encoder {
    cfg: EcConfig,
    /// k × p × 32 bytes. ISA-L's precomputed GF multiplication tables, used
    /// by `ec_encode_data` for SIMD-friendly inner loops.
    g_tbls: Vec<u8>,
}

impl Encoder {
    /// Build a new encoder for `cfg`. Cheap (~hundreds of bytes of state); a
    /// few microseconds. Construct once and reuse across stripes.
    pub fn new(cfg: EcConfig) -> Result<Self, EcError> {
        validate_cfg(cfg)?;

        let mut encode_matrix = build_encode_matrix(cfg);
        let mut g_tbls = vec![0u8; cfg.data * cfg.parity * 32];

        // SAFETY: g_tbls is sized k*p*32 as ISA-L requires. `ec_init_tables`
        // consumes only the *parity rows* of the encode matrix: the first k*k
        // bytes are the identity block, so the coding matrix starts at offset
        // k*k and is p*k bytes long, exactly the slice we hand it.
        let parity_rows_offset = cfg.data * cfg.data;
        unsafe {
            isa_l_sys::ec_init_tables(
                cfg.data as i32,
                cfg.parity as i32,
                encode_matrix[parity_rows_offset..].as_mut_ptr(),
                g_tbls.as_mut_ptr(),
            );
        }

        Ok(Self { cfg, g_tbls })
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

/// Reed-Solomon decoder / reconstructor. Rebuilds erased shards (data or
/// parity) from any `k` surviving shards of the same `(k, p)` stripe.
///
/// Serves both the **degraded-read** path (recover erased *data* shards to
/// satisfy a client read) and the **reconstruction** path (rebuild any erased
/// shards to restore on-disk redundancy after a datanode loss). The two are the
/// same GF(2^8) operation — they differ only in which shards the caller
/// requests and what is done with the result.
///
/// Unlike [`Encoder`], the decode tables depend on *which* shards survived, so
/// they cannot be precomputed in `new`; each [`Decoder::reconstruct`] call
/// builds the decode matrix for its specific erasure pattern. The inversion is
/// over a `k×k` matrix (`k ≤ 10` in production), negligible next to the GF SIMD
/// kernel that follows.
pub struct Decoder {
    cfg: EcConfig,
    /// (k+p) × k Cauchy generator matrix — identical to the encoder's.
    encode_matrix: Vec<u8>,
}

impl Decoder {
    /// Build a decoder for `cfg`.
    pub fn new(cfg: EcConfig) -> Result<Self, EcError> {
        validate_cfg(cfg)?;
        Ok(Self {
            cfg,
            encode_matrix: build_encode_matrix(cfg),
        })
    }

    /// The `(k, p)` configuration this decoder was built for.
    #[inline]
    pub fn config(&self) -> EcConfig {
        self.cfg
    }

    /// Reconstruct the `erased` shards from `k` surviving shards.
    ///
    /// All shard indices are in `0..k+p`, with data shards first (`0..k`) and
    /// parity shards after (`k..k+p`).
    ///
    /// - `len`: bytes per shard. Every source and output buffer must be exactly
    ///   this long. (The caller re-pads any originally-partial trailing shard to
    ///   the stripe's shard length before calling — the GF math operates on
    ///   fixed-width shards.)
    /// - `source_indices`: original indices of the surviving shards being
    ///   supplied. Exactly `k` distinct indices, none also in `erased`.
    /// - `sources`: the surviving shard buffers, parallel to `source_indices`
    ///   (`sources[i]` carries shard `source_indices[i]`). The pairing order is
    ///   free, but the two slices must agree.
    /// - `erased`: indices of the shards to rebuild. Order is caller-chosen but
    ///   must match `outputs`.
    /// - `outputs`: output buffers, parallel to `erased`.
    ///
    /// A call with an empty `erased` list is a no-op success.
    ///
    /// # Errors
    /// - [`EcError::BufferCount`] if `source_indices.len() != k`,
    ///   `sources.len() != k`, or `erased.len() != outputs.len()`.
    /// - [`EcError::BufferLen`] if any buffer is not `len` bytes.
    /// - [`EcError::BadIndex`] / [`EcError::DuplicateIndex`] /
    ///   [`EcError::Overlap`] for malformed index sets.
    /// - [`EcError::Singular`] if the chosen sources cannot be inverted
    ///   (unreachable for a valid distinct survivor set with a Cauchy matrix).
    pub fn reconstruct(
        &self,
        len: usize,
        source_indices: &[usize],
        sources: &[&[u8]],
        erased: &[usize],
        outputs: &mut [&mut [u8]],
    ) -> Result<(), EcError> {
        let k = self.cfg.data;
        let m = self.cfg.total();

        // --- shape validation ---
        if source_indices.len() != k {
            return Err(EcError::BufferCount {
                expected: k,
                got: source_indices.len(),
            });
        }
        if sources.len() != k {
            return Err(EcError::BufferCount {
                expected: k,
                got: sources.len(),
            });
        }
        if erased.len() != outputs.len() {
            return Err(EcError::BufferCount {
                expected: erased.len(),
                got: outputs.len(),
            });
        }
        if erased.is_empty() {
            return Ok(());
        }
        for b in sources {
            if b.len() != len {
                return Err(EcError::BufferLen {
                    expected: len,
                    got: b.len(),
                });
            }
        }
        for b in &*outputs {
            if b.len() != len {
                return Err(EcError::BufferLen {
                    expected: len,
                    got: b.len(),
                });
            }
        }

        // --- index validation: in range, distinct sources, disjoint from erased ---
        let mut is_source = vec![false; m];
        for &s in source_indices {
            if s >= m {
                return Err(EcError::BadIndex { index: s, bound: m });
            }
            if is_source[s] {
                return Err(EcError::DuplicateIndex { index: s });
            }
            is_source[s] = true;
        }
        for &e in erased {
            if e >= m {
                return Err(EcError::BadIndex { index: e, bound: m });
            }
            if is_source[e] {
                return Err(EcError::Overlap { index: e });
            }
        }

        // --- b = the k×k submatrix of encode_matrix for the chosen sources ---
        // Row i of b is the generator-matrix row that produced source i.
        let mut b = vec![0u8; k * k];
        for (i, &src) in source_indices.iter().enumerate() {
            for j in 0..k {
                b[k * i + j] = self.encode_matrix[k * src + j];
            }
        }

        // --- invert b: maps the k survivors back to the k original data shards ---
        let mut invert = vec![0u8; k * k];
        // SAFETY: b and invert are each k*k bytes; `gf_invert_matrix` reads and
        // writes only within n*n for n=k, and returns < 0 iff `b` is singular.
        // It mutates `b` in place (Gaussian elimination scratch), which is fine
        // — `b` is a local we discard.
        let rc =
            unsafe { isa_l_sys::gf_invert_matrix(b.as_mut_ptr(), invert.as_mut_ptr(), k as i32) };
        if rc < 0 {
            return Err(EcError::Singular);
        }

        // --- decode_matrix: one row per erased shard, in `erased` order ---
        let nerrs = erased.len();
        let mut decode_matrix = vec![0u8; nerrs * k];
        for (i, &e) in erased.iter().enumerate() {
            if e < k {
                // Erased *data* shard e: row e of the inverse is precisely the
                // linear combination of the survivors that yields data shard e.
                for j in 0..k {
                    decode_matrix[k * i + j] = invert[k * e + j];
                }
            } else {
                // Erased *parity* shard e: the original data is `invert ·
                // sources`; the parity is `encode_row_e · data`. Compose them
                // into coefficients on the survivors:
                //   decode_row[col] = XOR_row gf_mul(invert[row][col],
                //                                    encode_matrix[e][row]).
                for col in 0..k {
                    let mut acc = 0u8;
                    for row in 0..k {
                        // SAFETY: `gf_mul` is a pure GF(2^8) multiply with no
                        // memory effects.
                        let prod = unsafe {
                            isa_l_sys::gf_mul(invert[k * row + col], self.encode_matrix[k * e + row])
                        };
                        acc ^= prod;
                    }
                    decode_matrix[k * i + col] = acc;
                }
            }
        }

        // --- init decode tables and run the GF kernel over the survivors ---
        let mut g_tbls = vec![0u8; k * nerrs * 32];
        // SAFETY: decode_matrix is exactly nerrs*k bytes (the coding matrix with
        // no identity prefix), and g_tbls is k*nerrs*32, the sizes ISA-L
        // requires for (k, nerrs).
        unsafe {
            isa_l_sys::ec_init_tables(
                k as i32,
                nerrs as i32,
                decode_matrix.as_mut_ptr(),
                g_tbls.as_mut_ptr(),
            );
        }

        let src_ptrs: Vec<*mut u8> = sources.iter().map(|s| s.as_ptr() as *mut u8).collect();
        let out_ptrs: Vec<*mut u8> = outputs.iter_mut().map(|o| o.as_mut_ptr()).collect();
        // SAFETY:
        // - src_ptrs has exactly k entries, out_ptrs exactly nerrs; every buffer
        //   is `len` bytes (validated above).
        // - g_tbls matches (k, nerrs).
        // - ISA-L reads from the source buffers and writes only the outputs.
        unsafe {
            isa_l_sys::ec_encode_data(
                len as i32,
                k as i32,
                nerrs as i32,
                g_tbls.as_mut_ptr(),
                src_ptrs.as_ptr() as *mut *mut u8,
                out_ptrs.as_ptr() as *mut *mut u8,
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
    fn encode_matrix_is_identity_over_cauchy_for_production_profiles() {
        for cfg in [
            EcConfig { data: 3, parity: 2 },  // RS-3-2
            EcConfig { data: 6, parity: 3 },  // RS-6-3
            EcConfig { data: 10, parity: 4 }, // RS-10-4
        ] {
            let enc = Encoder::new(cfg).unwrap_or_else(|_| panic!("construct {cfg:?}"));
            assert_eq!(enc.config(), cfg);

            let matrix = build_encode_matrix(cfg);
            // Top k×k is the identity matrix.
            for row in 0..cfg.data {
                for col in 0..cfg.data {
                    let expected = if row == col { 1 } else { 0 };
                    assert_eq!(
                        matrix[row * cfg.data + col],
                        expected,
                        "encode_matrix[{row},{col}] not identity for {cfg:?}"
                    );
                }
            }
            // Parity rows must be non-trivial.
            let parity_rows = &matrix[(cfg.data * cfg.data)..];
            assert!(
                parity_rows.iter().any(|&b| b != 0),
                "parity rows all zero for {cfg:?}"
            );
        }
    }

    /// Byte-equivalence with Apache Ozone's Java EC coder, proven WITHOUT a JVM.
    ///
    /// Ozone's `RSRawEncoder` builds its generator matrix with
    /// `RSUtil.genCauchyMatrix` (hadoop-hdds/erasurecode) -- identity rows, then
    /// parity rows `a[pos] = GF256.gfInv(i ^ j)` over GF(2^8) with primitive
    /// polynomial 285 (0x11D). Its native coder is literally ISA-L. We port that
    /// exact construction here and assert it yields the SAME matrix as ISA-L's
    /// `gf_gen_cauchy1_matrix`. Because both encode by multiplying that matrix
    /// over the same field, identical matrices imply byte-identical parity --
    /// so our EC output equals Ozone's, for both its native and pure-Java coders.
    #[test]
    fn matrix_byte_identical_to_ozone_java_cauchy() {
        // GF(2^8) multiply, reduction polynomial 0x11D (low byte 0x1D).
        fn gf_mul(mut a: u8, mut b: u8) -> u8 {
            let mut product = 0u8;
            for _ in 0..8 {
                if b & 1 != 0 {
                    product ^= a;
                }
                let high = a & 0x80;
                a <<= 1;
                if high != 0 {
                    a ^= 0x1d;
                }
                b >>= 1;
            }
            product
        }
        // Multiplicative inverse in GF(2^8): x^254 (unique, generator-agnostic).
        fn gf_inv(x: u8) -> u8 {
            let (mut result, mut base, mut exp) = (1u8, x, 254u32);
            while exp > 0 {
                if exp & 1 == 1 {
                    result = gf_mul(result, base);
                }
                base = gf_mul(base, base);
                exp >>= 1;
            }
            result
        }
        // Direct port of Ozone's RSUtil.genCauchyMatrix(a, m, k).
        fn ozone_cauchy(m: usize, k: usize) -> Vec<u8> {
            let mut a = vec![0u8; m * k];
            for i in 0..k {
                a[k * i + i] = 1;
            }
            let mut pos = k * k;
            for i in k..m {
                for j in 0..k {
                    a[pos] = gf_inv((i ^ j) as u8);
                    pos += 1;
                }
            }
            a
        }

        for cfg in [
            EcConfig { data: 3, parity: 2 },
            EcConfig { data: 6, parity: 3 },
            EcConfig { data: 10, parity: 4 },
        ] {
            let isal = build_encode_matrix(cfg);
            let ozone = ozone_cauchy(cfg.total(), cfg.data);
            assert_eq!(
                isal, ozone,
                "ISA-L generator matrix differs from Ozone's Java Cauchy matrix for {cfg:?}"
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
        let mut parity: Vec<&mut [u8]> =
            parity_storage.iter_mut().map(|v| v.as_mut_slice()).collect();
        assert!(matches!(
            enc.encode(16, &data_short, &mut parity),
            Err(EcError::BufferCount { .. })
        ));
    }
}

#[cfg(test)]
mod decoder_tests {
    use super::*;
    use std::collections::HashSet;

    /// Build a full m-shard stripe: `k` pseudo-random data shards followed by
    /// `p` parity shards. Index `0..k` is data, `k..m` is parity.
    fn make_stripe(cfg: EcConfig, len: usize) -> Vec<Vec<u8>> {
        let enc = Encoder::new(cfg).unwrap();
        let data: Vec<Vec<u8>> = (0..cfg.data)
            .map(|i| {
                (0..len)
                    .map(|j| ((i * 31 + j * 7 + 11) & 0xff) as u8)
                    .collect()
            })
            .collect();
        let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
        let mut parity = vec![vec![0u8; len]; cfg.parity];
        {
            let mut parity_refs: Vec<&mut [u8]> =
                parity.iter_mut().map(|v| v.as_mut_slice()).collect();
            enc.encode(len, &data_refs, &mut parity_refs).unwrap();
        }
        let mut all = data;
        all.extend(parity);
        all
    }

    /// Reconstruct `erased` from the first `k` survivors and assert the rebuilt
    /// bytes equal the originals. This is the real proof of decode correctness:
    /// a wrong inverse or decode matrix yields mismatched bytes.
    fn assert_reconstructs(cfg: EcConfig, len: usize, erased: &[usize]) {
        let stripe = make_stripe(cfg, len);
        let m = cfg.total();
        let k = cfg.data;

        let erased_set: HashSet<usize> = erased.iter().copied().collect();
        let source_indices: Vec<usize> =
            (0..m).filter(|i| !erased_set.contains(i)).take(k).collect();
        assert_eq!(source_indices.len(), k, "not enough survivors for {cfg:?}");
        let sources: Vec<&[u8]> = source_indices.iter().map(|&i| stripe[i].as_slice()).collect();

        let dec = Decoder::new(cfg).unwrap();
        let mut outputs_storage = vec![vec![0u8; len]; erased.len()];
        {
            let mut outputs: Vec<&mut [u8]> = outputs_storage
                .iter_mut()
                .map(|v| v.as_mut_slice())
                .collect();
            dec.reconstruct(len, &source_indices, &sources, erased, &mut outputs)
                .unwrap();
        }
        for (out, &e) in outputs_storage.iter().zip(erased) {
            assert_eq!(
                out, &stripe[e],
                "reconstruction of shard {e} mismatch for {cfg:?}"
            );
        }
    }

    #[test]
    fn reconstruct_single_data_shard_rs_6_3() {
        assert_reconstructs(EcConfig { data: 6, parity: 3 }, 4096, &[2]);
    }

    #[test]
    fn reconstruct_first_and_last_data_shard_rs_6_3() {
        assert_reconstructs(EcConfig { data: 6, parity: 3 }, 4096, &[0, 5]);
    }

    #[test]
    fn reconstruct_single_parity_shard_rs_6_3() {
        // index 6 = parity 0, 7 = parity 1, 8 = parity 2
        assert_reconstructs(EcConfig { data: 6, parity: 3 }, 4096, &[7]);
    }

    #[test]
    fn reconstruct_data_and_parity_rs_3_2() {
        // data shard 0 + parity shard 1 (index 4) gone; rebuild from 3 survivors.
        assert_reconstructs(EcConfig { data: 3, parity: 2 }, 1024, &[0, 4]);
    }

    #[test]
    fn reconstruct_max_erasures_rs_10_4() {
        // Lose the maximum tolerable (p=4): a mix of data and parity. Only k=10
        // survivors remain, so the source matrix is square and must invert.
        assert_reconstructs(EcConfig { data: 10, parity: 4 }, 2048, &[1, 5, 9, 12]);
    }

    #[test]
    fn reconstruct_partial_stripe_len_rs_6_3() {
        // Shard length need not be a power of two or the 1 MiB chunk size; the
        // GF kernel handles arbitrary lengths. Exercises the partial-trailing
        // path's eventual shard width.
        assert_reconstructs(EcConfig { data: 6, parity: 3 }, 1500, &[3]);
    }

    #[test]
    fn reconstruct_empty_erased_is_noop() {
        let cfg = EcConfig { data: 3, parity: 2 };
        let dec = Decoder::new(cfg).unwrap();
        let buf = vec![7u8; 64];
        let source_indices = vec![0usize, 1, 2];
        let sources: Vec<&[u8]> = vec![buf.as_slice(); 3];
        let erased: Vec<usize> = vec![];
        let mut outputs: Vec<&mut [u8]> = vec![];
        assert!(dec
            .reconstruct(64, &source_indices, &sources, &erased, &mut outputs)
            .is_ok());
    }

    #[test]
    fn reconstruct_rejects_wrong_source_count() {
        let cfg = EcConfig { data: 3, parity: 2 };
        let dec = Decoder::new(cfg).unwrap();
        let buf = vec![0u8; 64];
        let source_indices = vec![0usize, 1]; // only 2, need 3
        let sources: Vec<&[u8]> = vec![buf.as_slice(); 2];
        let erased = vec![2usize];
        let mut out = vec![vec![0u8; 64]; 1];
        let mut outputs: Vec<&mut [u8]> = out.iter_mut().map(|v| v.as_mut_slice()).collect();
        assert!(matches!(
            dec.reconstruct(64, &source_indices, &sources, &erased, &mut outputs),
            Err(EcError::BufferCount { .. })
        ));
    }

    #[test]
    fn reconstruct_rejects_source_erased_overlap() {
        let cfg = EcConfig { data: 3, parity: 2 };
        let dec = Decoder::new(cfg).unwrap();
        let buf = vec![0u8; 64];
        let source_indices = vec![0usize, 1, 2];
        let sources: Vec<&[u8]> = vec![buf.as_slice(); 3];
        let erased = vec![2usize]; // 2 is also a source
        let mut out = vec![vec![0u8; 64]; 1];
        let mut outputs: Vec<&mut [u8]> = out.iter_mut().map(|v| v.as_mut_slice()).collect();
        assert!(matches!(
            dec.reconstruct(64, &source_indices, &sources, &erased, &mut outputs),
            Err(EcError::Overlap { .. })
        ));
    }

    #[test]
    fn reconstruct_rejects_out_of_range_index() {
        let cfg = EcConfig { data: 3, parity: 2 }; // m = 5, valid 0..5
        let dec = Decoder::new(cfg).unwrap();
        let buf = vec![0u8; 64];
        let source_indices = vec![0usize, 1, 9]; // 9 is out of range
        let sources: Vec<&[u8]> = vec![buf.as_slice(); 3];
        let erased = vec![2usize];
        let mut out = vec![vec![0u8; 64]; 1];
        let mut outputs: Vec<&mut [u8]> = out.iter_mut().map(|v| v.as_mut_slice()).collect();
        assert!(matches!(
            dec.reconstruct(64, &source_indices, &sources, &erased, &mut outputs),
            Err(EcError::BadIndex { .. })
        ));
    }

    #[test]
    fn reconstruct_rejects_len_mismatch() {
        let cfg = EcConfig { data: 3, parity: 2 };
        let dec = Decoder::new(cfg).unwrap();
        let good = vec![0u8; 64];
        let short = vec![0u8; 8];
        let source_indices = vec![0usize, 1, 2];
        let sources: Vec<&[u8]> = vec![good.as_slice(), good.as_slice(), short.as_slice()];
        let erased = vec![3usize];
        let mut out = vec![vec![0u8; 64]; 1];
        let mut outputs: Vec<&mut [u8]> = out.iter_mut().map(|v| v.as_mut_slice()).collect();
        assert!(matches!(
            dec.reconstruct(64, &source_indices, &sources, &erased, &mut outputs),
            Err(EcError::BufferLen { .. })
        ));
    }
}
