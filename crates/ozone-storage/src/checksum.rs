//! Chunk-data checksums.
//!
//! The datanode verifies chunk bytes with CRC32C (Ozone's default), computing
//! one digest per `bytes_per_checksum`-sized window. SHA256/MD5/CRC32 are
//! recognized by [`ozone_types::ChecksumType`] but not implemented here — object
//! ETags (MD5) are computed in the gateway, not as chunk checksums.
//!
//! # Wire convention
//! Each CRC32C digest is stored big-endian as 4 bytes. This is an internal
//! convention of the Rust datanode (which is the only reader/writer of its own
//! chunk files); it does not need to match Java Ozone's on-disk checksum
//! encoding because Rust and Java datanodes never share chunk files.

use ozone_types::{ChecksumData, ChecksumType};

use crate::error::ChecksumError;

/// Number of `bytes_per_checksum`-sized windows `len` bytes divide into.
///
/// Zero-length data yields zero windows. `bytes_per_checksum` is clamped to at
/// least 1 to avoid a divide-by-zero on a malformed input.
#[inline]
fn window_count(len: usize, bytes_per_checksum: u32) -> usize {
    let bpc = (bytes_per_checksum.max(1)) as usize;
    len.div_ceil(bpc)
}

/// Compute a [`ChecksumData`] over `data`.
///
/// For [`ChecksumType::None`] this returns an empty bundle regardless of
/// `bytes_per_checksum`. For [`ChecksumType::Crc32c`] it produces one big-endian
/// 4-byte digest per window. Other types return [`ChecksumError::Unsupported`].
pub fn compute(
    data: &[u8],
    bytes_per_checksum: u32,
    checksum_type: ChecksumType,
) -> Result<ChecksumData, ChecksumError> {
    match checksum_type {
        ChecksumType::None => Ok(ChecksumData::none()),
        ChecksumType::Crc32c => {
            let bpc = bytes_per_checksum.max(1) as usize;
            let checksums = data
                .chunks(bpc)
                .map(|window| crc32c::crc32c(window).to_be_bytes().to_vec())
                .collect();
            Ok(ChecksumData {
                checksum_type,
                bytes_per_checksum,
                checksums,
            })
        }
        other => Err(ChecksumError::Unsupported(other)),
    }
}

/// Verify `data` against a previously-computed [`ChecksumData`].
///
/// [`ChecksumType::None`] always passes. For CRC32C, the digest count must match
/// the window count and every per-window digest must match. The first
/// mismatching window short-circuits with [`ChecksumError::Mismatch`].
pub fn verify(data: &[u8], cd: &ChecksumData) -> Result<(), ChecksumError> {
    match cd.checksum_type {
        ChecksumType::None => Ok(()),
        ChecksumType::Crc32c => {
            let expected_windows = window_count(data.len(), cd.bytes_per_checksum);
            if cd.checksums.len() != expected_windows {
                return Err(ChecksumError::CountMismatch {
                    windows: expected_windows,
                    provided: cd.checksums.len(),
                });
            }
            let bpc = cd.bytes_per_checksum.max(1) as usize;
            for (window, bytes) in data.chunks(bpc).enumerate() {
                let got = crc32c::crc32c(bytes).to_be_bytes();
                if cd.checksums[window].as_slice() != got.as_slice() {
                    return Err(ChecksumError::Mismatch { window });
                }
            }
            Ok(())
        }
        other => Err(ChecksumError::Unsupported(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_round_trip_multiple_windows() {
        let data: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
        let cd = compute(&data, 256, ChecksumType::Crc32c).unwrap();
        // 1000 bytes / 256 = 4 windows (3 full + 1 partial of 232).
        assert_eq!(cd.checksums.len(), 4);
        assert_eq!(cd.checksums[0].len(), 4); // 4-byte CRC32C
        verify(&data, &cd).unwrap();
    }

    #[test]
    fn corruption_is_detected_in_the_right_window() {
        let data: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
        let cd = compute(&data, 256, ChecksumType::Crc32c).unwrap();
        let mut corrupt = data.clone();
        corrupt[300] ^= 0xff; // window 1 (bytes 256..512)
        let err = verify(&corrupt, &cd).unwrap_err();
        assert!(matches!(err, ChecksumError::Mismatch { window: 1 }));
    }

    #[test]
    fn count_mismatch_detected() {
        let data = vec![0u8; 512];
        let mut cd = compute(&data, 256, ChecksumType::Crc32c).unwrap();
        cd.checksums.pop(); // now 1 digest for 2 windows
        assert!(matches!(
            verify(&data, &cd),
            Err(ChecksumError::CountMismatch {
                windows: 2,
                provided: 1
            })
        ));
    }

    #[test]
    fn empty_data_has_no_windows() {
        let cd = compute(&[], 256, ChecksumType::Crc32c).unwrap();
        assert!(cd.checksums.is_empty());
        verify(&[], &cd).unwrap();
    }

    #[test]
    fn none_type_is_a_noop() {
        let cd = compute(b"anything", 256, ChecksumType::None).unwrap();
        assert_eq!(cd.checksum_type, ChecksumType::None);
        assert!(cd.checksums.is_empty());
        verify(b"anything", &cd).unwrap();
    }

    #[test]
    fn unsupported_type_errors() {
        assert!(matches!(
            compute(b"x", 256, ChecksumType::Sha256),
            Err(ChecksumError::Unsupported(ChecksumType::Sha256))
        ));
    }
}
