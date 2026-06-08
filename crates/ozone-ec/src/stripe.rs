//! Object <-> EC shard pipeline: the stripe/cell layout over the per-stripe
//! [`Encoder`]/[`Reconstructor`].
//!
//! # Layout
//! An object (block group) of `len` bytes is cut into `chunk_size`-byte *cells*
//! distributed round-robin across the `k` data shards, stripe by stripe:
//!
//! ```text
//! stripe 0:  data[0]=cell0  data[1]=cell1  ... data[k-1]=cell(k-1)
//! stripe 1:  data[0]=cell k data[1]=cell k+1 ...
//! ```
//!
//! Cell `(shard i, stripe s)` covers object bytes
//! `[s*k*C + i*C , +C)` (clamped to `len`). A data shard stores its cells
//! concatenated and *truncated* to their real lengths; only the final stripe can
//! contain a partial (or empty) cell. Parity is computed per stripe over the `k`
//! data cells **zero-padded to `C`**, and each parity shard stores `num_stripes`
//! full `C`-byte cells.
//!
//! # Byte-equivalence caveat
//! This layout round-trips and reconstructs correctly (proven by tests). Exact
//! byte-equivalence with Apache Ozone's Java parity-cell truncation for the
//! trailing partial stripe is intentionally NOT asserted here; that detail is
//! pinned later by the golden-vector harness (M1). The GF math itself
//! (Cauchy matrix encode/decode) is already byte-identical via `isa-l-safe`.

use crate::{EcError, Encoder, Profile, Reconstructor};

/// The `k+p` shard byte-vectors produced by [`encode_object`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedShards {
    /// `k` data shards, each truncated to its real cell bytes.
    pub data: Vec<Vec<u8>>,
    /// `p` parity shards, each `num_stripes * chunk_size` bytes.
    pub parity: Vec<Vec<u8>>,
}

/// Number of stripes an object of `len` bytes occupies under `profile`.
#[inline]
pub fn num_stripes(profile: Profile, len: usize) -> usize {
    if len == 0 {
        0
    } else {
        len.div_ceil(profile.stripe_size())
    }
}

/// Length of cell `(shard i, stripe s)` — the real (untruncated) byte count.
#[inline]
fn cell_len(profile: Profile, len: usize, i: usize, s: usize) -> usize {
    let off = s * profile.stripe_size() + i * profile.chunk_size;
    if off >= len {
        0
    } else {
        (len - off).min(profile.chunk_size)
    }
}

/// The exact stored byte length of shard `idx` for an object of `len` bytes — the
/// length [`encode_object`] produces. A data shard holds `full_stripes * C` plus
/// its slice of the trailing partial stripe; a parity shard holds
/// `num_stripes * C`. Used by the decoder to reject a shard whose length is
/// inconsistent with `len` before slicing it.
///
/// Computed in O(1) (NOT by summing per-stripe cell lengths): `len` comes from an
/// untrusted `block_group_len`, so an O(num_stripes) form would let a huge `len`
/// burn billions of iterations — a CPU DoS. Saturating arithmetic keeps a huge
/// `len` from overflowing; it just yields a large value that no real shard matches,
/// so the shard is dropped.
#[inline]
fn expected_shard_len(profile: Profile, len: usize, idx: usize) -> usize {
    let c = profile.chunk_size;
    if idx < profile.data {
        let stripe = profile.stripe_size();
        let full = (len / stripe).saturating_mul(c);
        let rem = len % stripe; // bytes in the trailing partial stripe
        full + rem.saturating_sub(idx * c).min(c)
    } else {
        num_stripes(profile, len).saturating_mul(c)
    }
}

/// Extract cell `(idx, s)` from a surviving shard, zero-padded to `chunk_size`.
/// `idx < k` is a data shard (cells stored truncated, contiguous); `idx >= k`
/// is a parity shard (cells stored at full `chunk_size`, offset `s*C`).
fn padded_cell(profile: Profile, len: usize, idx: usize, s: usize, shard: &[u8]) -> Vec<u8> {
    let c = profile.chunk_size;
    let mut cell = vec![0u8; c];
    if idx < profile.data {
        let off: usize = (0..s).map(|t| cell_len(profile, len, idx, t)).sum();
        let cl = cell_len(profile, len, idx, s);
        cell[..cl].copy_from_slice(&shard[off..off + cl]);
    } else {
        let start = s * c;
        cell.copy_from_slice(&shard[start..start + c]);
    }
    cell
}

/// Encode an object's bytes into `k` data shards (truncated) and `p` parity
/// shards (`num_stripes * chunk_size` each).
pub fn encode_object(profile: Profile, data: &[u8]) -> Result<EncodedShards, EcError> {
    if profile.data == 0 || profile.chunk_size == 0 {
        return Err(EcError::InvalidProfile {
            data: profile.data,
            chunk_size: profile.chunk_size,
        });
    }
    let k = profile.data;
    let p = profile.parity;
    let c = profile.chunk_size;
    let len = data.len();
    let stripes = num_stripes(profile, len);

    let enc = Encoder::new(profile)?;
    let mut data_shards = vec![Vec::new(); k];
    let mut parity_shards: Vec<Vec<u8>> = vec![Vec::with_capacity(stripes * c); p];

    for s in 0..stripes {
        // Build k data cells, zero-padded to C, and record the real bytes.
        let mut cells: Vec<Vec<u8>> = Vec::with_capacity(k);
        for (i, shard) in data_shards.iter_mut().enumerate() {
            let off = s * profile.stripe_size() + i * c;
            let cl = cell_len(profile, len, i, s);
            let mut cell = vec![0u8; c];
            if cl > 0 {
                cell[..cl].copy_from_slice(&data[off..off + cl]);
                shard.extend_from_slice(&data[off..off + cl]);
            }
            cells.push(cell);
        }

        let data_refs: Vec<&[u8]> = cells.iter().map(|v| v.as_slice()).collect();
        let mut par_storage = vec![vec![0u8; c]; p];
        {
            let mut par_refs: Vec<&mut [u8]> =
                par_storage.iter_mut().map(|v| v.as_mut_slice()).collect();
            enc.encode_stripe(&data_refs, &mut par_refs)?;
        }
        for (j, par) in par_storage.into_iter().enumerate() {
            parity_shards[j].extend_from_slice(&par);
        }
    }

    Ok(EncodedShards {
        data: data_shards,
        parity: parity_shards,
    })
}

/// Reassemble the object from all `k` data shards (the non-degraded read path).
/// `data_shards[i]` must be data shard `i`'s stored (truncated) bytes.
fn reassemble(profile: Profile, len: usize, data_shards: &[Vec<u8>]) -> Vec<u8> {
    let k = profile.data;
    let stripes = num_stripes(profile, len);
    let mut out = Vec::with_capacity(len);
    let mut cursor = vec![0usize; k];
    for s in 0..stripes {
        for (i, cur) in cursor.iter_mut().enumerate() {
            let cl = cell_len(profile, len, i, s);
            out.extend_from_slice(&data_shards[i][*cur..*cur + cl]);
            *cur += cl;
        }
    }
    out
}

/// Decode an object of length `len` from whatever shards survive.
///
/// `shards` is exactly `k+p` slots, data shards first; `None` marks a missing
/// shard. If any *data* shard is missing it is reconstructed from any `k`
/// survivors via the per-stripe [`Reconstructor`]; then all `k` data shards are
/// reassembled into the object. Missing *parity* shards are ignored (they are
/// not needed to read).
///
/// # Errors
/// - [`EcError::ShardCount`] if `shards.len() != k+p`.
/// - [`EcError::NotEnoughShards`] if fewer than `k` shards survive while a data
///   shard needs rebuilding.
pub fn decode_object(
    profile: Profile,
    len: usize,
    shards: &[Option<&[u8]>],
) -> Result<Vec<u8>, EcError> {
    if profile.data == 0 || profile.chunk_size == 0 {
        return Err(EcError::InvalidProfile {
            data: profile.data,
            chunk_size: profile.chunk_size,
        });
    }
    let k = profile.data;
    let total = profile.total();
    if shards.len() != total {
        return Err(EcError::ShardCount {
            expected: total,
            got: shards.len(),
        });
    }
    // Defense against an inconsistent `len` (e.g. a malformed `block_group_len`
    // from an external SCM repair command) or a truncated shard: the cell slicing
    // below derives offsets purely from `len`, so a shard shorter or longer than
    // `len` implies would slice out of bounds and PANIC. Drop any shard whose byte
    // length disagrees with `len`; reconstruction then proceeds from the consistent
    // survivors, and falls through to `NotEnoughShards` if too few remain. This
    // turns a panic (and a silent-truncation read) into a graceful error.
    let mut shards: Vec<Option<&[u8]>> = shards.to_vec();
    for (idx, slot) in shards.iter_mut().enumerate() {
        if let Some(b) = slot {
            if b.len() != expected_shard_len(profile, len, idx) {
                *slot = None;
            }
        }
    }
    let shards = shards.as_slice();
    let stripes = num_stripes(profile, len);

    // Start with whatever data shards we already have.
    let mut data_bufs: Vec<Vec<u8>> = vec![Vec::new(); k];
    for (i, buf) in data_bufs.iter_mut().enumerate() {
        if let Some(b) = shards[i] {
            *buf = b.to_vec();
        }
    }
    let missing_data: Vec<usize> = (0..k).filter(|&i| shards[i].is_none()).collect();

    if !missing_data.is_empty() {
        let present: Vec<usize> = (0..total).filter(|&x| shards[x].is_some()).collect();
        if present.len() < k {
            return Err(EcError::NotEnoughShards {
                have: present.len(),
                need: k,
            });
        }
        let sources_idx: Vec<usize> = present[..k].to_vec();
        let recon = Reconstructor::new(profile)?;

        // Reconstruct the missing data shards one stripe at a time, then
        // truncate each recovered cell back to its real length.
        for &i in &missing_data {
            data_bufs[i] = Vec::new();
        }
        for s in 0..stripes {
            let src_cells: Vec<Vec<u8>> = sources_idx
                .iter()
                .map(|&idx| padded_cell(profile, len, idx, s, shards[idx].unwrap()))
                .collect();
            let src_refs: Vec<&[u8]> = src_cells.iter().map(|v| v.as_slice()).collect();

            let mut out_storage = vec![vec![0u8; profile.chunk_size]; missing_data.len()];
            {
                let mut out_refs: Vec<&mut [u8]> =
                    out_storage.iter_mut().map(|v| v.as_mut_slice()).collect();
                recon.reconstruct(
                    profile.chunk_size,
                    &sources_idx,
                    &src_refs,
                    &missing_data,
                    &mut out_refs,
                )?;
            }
            for (mi, &i) in missing_data.iter().enumerate() {
                let cl = cell_len(profile, len, i, s);
                data_bufs[i].extend_from_slice(&out_storage[mi][..cl]);
            }
        }
    }

    Ok(reassemble(profile, len, &data_bufs))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tiny profile for fast tests: k=3, p=2, 8-byte cells (stripe = 24 bytes).
    fn tiny() -> Profile {
        Profile {
            data: 3,
            parity: 2,
            chunk_size: 8,
        }
    }

    fn sample(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i * 7 + 3) as u8).collect()
    }

    /// Build the `k+p` `Option<&[u8]>` view, dropping `dropped` indices.
    fn views<'a>(shards: &'a EncodedShards, dropped: &[usize]) -> Vec<Option<&'a [u8]>> {
        let k = shards.data.len();
        let total = k + shards.parity.len();
        (0..total)
            .map(|idx| {
                if dropped.contains(&idx) {
                    None
                } else if idx < k {
                    Some(shards.data[idx].as_slice())
                } else {
                    Some(shards.parity[idx - k].as_slice())
                }
            })
            .collect()
    }

    /// A `len` far larger than the shards actually hold must NOT panic via an
    /// out-of-bounds cell slice (reachable from an external SCM repair command's
    /// `block_group_len`); it must degrade to a graceful error.
    #[test]
    fn decode_with_oversized_len_errors_not_panics() {
        let p = tiny();
        let shards = encode_object(p, &sample(10)).unwrap();
        let v = views(&shards, &[]);
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| decode_object(p, 100, &v)));
        match r {
            Ok(Err(_)) => {} // graceful error: correct
            Ok(Ok(_)) => panic!("oversized len silently returned (truncated) bytes"),
            Err(_) => panic!("oversized len PANICKED instead of returning an error"),
        }
    }

    /// A single shard whose byte length is inconsistent with `len` is dropped and
    /// reconstructed from the consistent survivors — no panic, no corruption.
    #[test]
    fn decode_drops_length_inconsistent_shard_and_recovers() {
        let p = tiny();
        let obj = sample(50); // 3 stripes
        let shards = encode_object(p, &obj).unwrap();
        let mut v = views(&shards, &[]);
        let short = &shards.data[0][..shards.data[0].len() - 1]; // truncated shard 0
        v[0] = Some(short);
        let got = decode_object(p, 50, &v).expect("recoverable from the other 4 shards");
        assert_eq!(got, obj, "inconsistent shard dropped and reconstructed exactly");
    }

    /// Advance `combo` (a sorted k-subset of `0..n`) to the next combination in
    /// lexicographic order; returns false when the last subset is passed.
    fn next_combination(combo: &mut [usize], n: usize) -> bool {
        let k = combo.len();
        let mut i = k;
        while i > 0 {
            i -= 1;
            if combo[i] < n - (k - i) {
                combo[i] += 1;
                for j in i + 1..k {
                    combo[j] = combo[j - 1] + 1;
                }
                return true;
            }
        }
        false
    }

    /// EXHAUSTIVE MDS recoverability: for each production EC profile, the object
    /// must reconstruct from EVERY possible set of exactly `k` surviving shards
    /// (i.e. every choice of which `p` to lose). This fully closes the
    /// "any k distinct survivors invert; the Cauchy submatrix is never singular"
    /// claim for the profiles we ship — by enumeration, not sampling.
    #[test]
    fn reconstruct_from_every_k_subset_per_profile() {
        let profiles = [
            Profile { data: 3, parity: 2, chunk_size: 16 },
            Profile { data: 6, parity: 3, chunk_size: 16 },
            Profile { data: 10, parity: 4, chunk_size: 16 },
        ];
        for profile in profiles {
            let total = profile.total();
            let k = profile.data;
            // Several stripes plus a partial trailing stripe.
            let len = 3 * profile.stripe_size() + profile.chunk_size + 5;
            let data: Vec<u8> = (0..len).map(|i| (i * 31 + 7) as u8).collect();
            let shards = encode_object(profile, &data).unwrap();

            let mut combo: Vec<usize> = (0..k).collect();
            let mut subsets = 0usize;
            loop {
                let present: std::collections::HashSet<usize> = combo.iter().copied().collect();
                let views: Vec<Option<&[u8]>> = (0..total)
                    .map(|i| {
                        if !present.contains(&i) {
                            None
                        } else if i < k {
                            Some(shards.data[i].as_slice())
                        } else {
                            Some(shards.parity[i - k].as_slice())
                        }
                    })
                    .collect();
                let recovered = decode_object(profile, len, &views)
                    .unwrap_or_else(|e| panic!("k-subset {combo:?} failed to decode: {e}"));
                assert_eq!(
                    recovered, data,
                    "profile k={} p={} failed for surviving subset {:?}",
                    profile.data, profile.parity, combo
                );
                subsets += 1;
                if !next_combination(&mut combo, total) {
                    break;
                }
            }
            // C(total, k) subsets: C(5,3)=10, C(9,6)=84, C(14,10)=1001.
            let expected = match (k, total) {
                (3, 5) => 10,
                (6, 9) => 84,
                (10, 14) => 1001,
                _ => subsets,
            };
            assert_eq!(subsets, expected, "must cover every k-subset for k={k}, total={total}");
        }
    }

    #[test]
    fn happy_path_round_trip_full_and_partial() {
        for len in [0usize, 5, 24, 25, 48, 50, 100] {
            let data = sample(len);
            let shards = encode_object(tiny(), &data).unwrap();
            // No drops: decode reassembles the object.
            let out = decode_object(tiny(), len, &views(&shards, &[])).unwrap();
            assert_eq!(out, data, "round trip failed at len={len}");
        }
    }

    #[test]
    fn parity_shard_length_is_num_stripes_times_chunk() {
        let len = 50; // 3 stripes for stripe_size 24
        let shards = encode_object(tiny(), &sample(len)).unwrap();
        assert_eq!(num_stripes(tiny(), len), 3);
        for par in &shards.parity {
            assert_eq!(par.len(), 3 * 8);
        }
    }

    #[test]
    fn degraded_read_one_data_one_parity_dropped() {
        // Partial last stripe (len=50). Drop data shard 1 and parity shard 0
        // (index 3). Survivors: data0, data2, parity1 = exactly k=3.
        let len = 50;
        let data = sample(len);
        let shards = encode_object(tiny(), &data).unwrap();
        let out = decode_object(tiny(), len, &views(&shards, &[1, 3])).unwrap();
        assert_eq!(out, data);
    }

    #[test]
    fn degraded_read_two_data_shards_dropped() {
        // Drop the maximum recoverable data shards (p=2): data0 and data1.
        // Survivors: data2 + both parity = 3 = k.
        let len = 48; // exactly 2 full stripes
        let data = sample(len);
        let shards = encode_object(tiny(), &data).unwrap();
        let out = decode_object(tiny(), len, &views(&shards, &[0, 1])).unwrap();
        assert_eq!(out, data);
    }

    #[test]
    fn not_enough_shards_errors() {
        let len = 48;
        let shards = encode_object(tiny(), &sample(len)).unwrap();
        // Drop 3 shards (k+p=5, leaving 2 < k=3) including a data shard.
        let err = decode_object(tiny(), len, &views(&shards, &[0, 1, 3])).unwrap_err();
        assert!(matches!(err, EcError::NotEnoughShards { have: 2, need: 3 }));
    }

    #[test]
    fn wrong_shard_count_errors() {
        let shards = vec![None; 4]; // tiny() expects 5 slots
        assert!(matches!(
            decode_object(tiny(), 10, &shards),
            Err(EcError::ShardCount {
                expected: 5,
                got: 4
            })
        ));
    }

    #[test]
    fn realistic_profile_round_trip_and_recovery() {
        // RS-6-3 with a 1 KiB cell; object spans several stripes with a partial.
        let profile = Profile {
            data: 6,
            parity: 3,
            chunk_size: 1024,
        };
        let len = 6 * 1024 * 2 + 500; // 2 full stripes + a 500-byte partial
        let data = sample(len);
        let shards = encode_object(profile, &data).unwrap();
        // Drop 3 shards (max p): two data + one parity.
        let dropped = [0usize, 4, 7];
        let views: Vec<Option<&[u8]>> = (0..9)
            .map(|idx| {
                if dropped.contains(&idx) {
                    None
                } else if idx < 6 {
                    Some(shards.data[idx].as_slice())
                } else {
                    Some(shards.parity[idx - 6].as_slice())
                }
            })
            .collect();
        let out = decode_object(profile, len, &views).unwrap();
        assert_eq!(out, data);
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(150))]

        /// For any data length, any production-shaped profile, and any erasure
        /// of up to `p` shards, the object decodes back byte-for-byte. Small
        /// chunk sizes keep each case cheap while still exercising multi-stripe
        /// and partial-stripe layouts.
        #[test]
        fn encode_then_decode_survives_any_p_erasures(
            data in proptest::collection::vec(proptest::prelude::any::<u8>(), 0usize..1500),
            profile_idx in 0usize..3,
            drop_seed in proptest::prelude::any::<u64>(),
        ) {
            use std::collections::HashSet;
            let profiles = [
                Profile { data: 3, parity: 2, chunk_size: 16 },
                Profile { data: 6, parity: 3, chunk_size: 16 },
                Profile { data: 10, parity: 4, chunk_size: 16 },
            ];
            let profile = profiles[profile_idx];
            let total = profile.total();
            let p = profile.parity;

            let shards = encode_object(profile, &data).unwrap();

            // Deterministically drop up to p distinct shards (always recoverable
            // since at least k survive).
            let mut seed = drop_seed;
            let mut next = || {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                seed
            };
            let n_drop = (next() % (p as u64 + 1)) as usize;
            let mut dropped = HashSet::new();
            while dropped.len() < n_drop {
                dropped.insert((next() % total as u64) as usize);
            }

            let views: Vec<Option<&[u8]>> = (0..total)
                .map(|i| {
                    if dropped.contains(&i) {
                        None
                    } else if i < profile.data {
                        Some(shards.data[i].as_slice())
                    } else {
                        Some(shards.parity[i - profile.data].as_slice())
                    }
                })
                .collect();

            let recovered = decode_object(profile, data.len(), &views).unwrap();
            proptest::prop_assert_eq!(recovered, data);
        }

        /// The decoder's O(1) `expected_shard_len` must EXACTLY equal the byte
        /// length `encode_object` actually produces for every shard. If it ever
        /// disagreed, the decoder would wrongly drop a VALID shard as
        /// "inconsistent" and spuriously fail. Pins the closed form across fuzzed
        /// lengths and profiles.
        #[test]
        fn expected_shard_len_matches_encoded_shards(
            data in proptest::collection::vec(proptest::prelude::any::<u8>(), 0usize..2000),
            profile_idx in 0usize..3,
        ) {
            let profiles = [
                Profile { data: 3, parity: 2, chunk_size: 16 },
                Profile { data: 6, parity: 3, chunk_size: 16 },
                Profile { data: 10, parity: 4, chunk_size: 16 },
            ];
            let profile = profiles[profile_idx];
            let len = data.len();
            let shards = encode_object(profile, &data).unwrap();
            for i in 0..profile.data {
                proptest::prop_assert_eq!(
                    shards.data[i].len(),
                    expected_shard_len(profile, len, i),
                    "data shard {} length mismatch at len {}", i, len
                );
            }
            for j in 0..profile.parity {
                proptest::prop_assert_eq!(
                    shards.parity[j].len(),
                    expected_shard_len(profile, len, profile.data + j),
                    "parity shard {} length mismatch at len {}", j, len
                );
            }
        }

        /// `decode_object` must NEVER panic, for ANY profile, length, and shard set
        /// -- including wrong-length shards, all-missing, and an oversized length.
        /// This is the generalization of the out-of-bounds-slice bug: a malformed
        /// `block_group_len` must degrade to a graceful error, never an OOB panic.
        /// When it DOES succeed it must yield exactly `len` bytes (no silent
        /// truncation).
        #[test]
        fn decode_never_panics_on_arbitrary_inputs(
            data_k in 0usize..11,
            parity_p in 0usize..5,
            chunk in 0usize..40,
            len in 0usize..5000,
            raw in proptest::collection::vec(
                proptest::option::of(
                    proptest::collection::vec(proptest::prelude::any::<u8>(), 0usize..200)
                ),
                16,
            ),
        ) {
            // Fuzz the PROFILE too -- including degenerate (data=0 / chunk_size=0)
            // ones, which must be a graceful InvalidProfile error, never a
            // divide-by-zero in num_stripes.
            let profile = Profile { data: data_k, parity: parity_p, chunk_size: chunk };
            let total = profile.total();
            let views: Vec<Option<&[u8]>> = (0..total).map(|i| raw[i].as_deref()).collect();
            // A panic (OOB slice or divide-by-zero) fails the proptest.
            if let Ok(obj) = decode_object(profile, len, &views) {
                proptest::prop_assert_eq!(obj.len(), len, "a successful decode must yield exactly len bytes");
            }
        }
    }
}
