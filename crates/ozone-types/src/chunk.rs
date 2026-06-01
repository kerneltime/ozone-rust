//! Chunk- and block-level aggregate types: the data a datanode stores per block.
//!
//! These mirror the wire messages `ChunkInfo` / `BlockData` / `ChecksumData`
//! but stay free of any protobuf dependency. The datanode persists `BlockData`
//! as block metadata; the chunk bytes themselves live in chunk files keyed by
//! [`ChunkInfo::chunk_name`].

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{BlockId, ChecksumType};

/// Per-chunk checksum bundle: one digest per `bytes_per_checksum`-sized window
/// of the chunk's bytes, in order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChecksumData {
    /// Digest algorithm.
    pub checksum_type: ChecksumType,
    /// Window size each digest covers. Meaningful only when `checksum_type`
    /// is not [`ChecksumType::None`].
    pub bytes_per_checksum: u32,
    /// One digest per window. Digest width is algorithm-dependent: CRC32/CRC32C
    /// = 4 bytes, MD5 = 16, SHA256 = 32.
    pub checksums: Vec<Vec<u8>>,
}

impl ChecksumData {
    /// A bundle declaring "no checksums".
    pub fn none() -> Self {
        Self {
            checksum_type: ChecksumType::None,
            bytes_per_checksum: 0,
            checksums: Vec::new(),
        }
    }
}

/// Per-stripe checksum bundle for EC.
///
/// Stored only on replica-1 (the first data shard) and on every parity replica,
/// matching Ozone's existing convention: those are exactly the replicas a
/// reconstruction coordinator reads to verify a recovered stripe, so duplicating
/// the bundle on each parity replica makes verification possible even when the
/// first data shard is the one being rebuilt.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StripeChecksum {
    /// One checksum per stripe, in stripe order.
    pub per_stripe_checksums: Vec<Vec<u8>>,
}

/// A contiguous run of bytes within a block — the datanode's unit of chunk I/O.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkInfo {
    /// Stable logical name; the datanode derives the on-disk filename from it.
    pub chunk_name: String,
    /// Byte offset of this chunk within its block.
    pub offset: u64,
    /// Length in bytes.
    pub len: u64,
    /// Optional per-window integrity checksums.
    pub checksum_data: Option<ChecksumData>,
    /// Optional EC per-stripe checksum bundle (see [`StripeChecksum`]).
    pub stripe_checksum: Option<StripeChecksum>,
}

/// Metadata key carrying an EC block-group's full logical length.
///
/// Must match Apache Ozone's `OzoneConsts.BLOCK_GROUP_LEN_KEY_IN_PUT_BLOCK`.
/// On EC blocks this records the total user-visible length across all D+P
/// internal blocks, which the reconstruction path needs to size the trailing
/// partial stripe correctly.
pub const BLOCK_GROUP_LEN_KEY: &str = "blockGroupLen";

/// All metadata and chunk layout for a single (internal) block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockData {
    /// Block identity, including the EC replica slot.
    pub block_id: BlockId,
    /// Ordered chunks comprising the block's bytes.
    pub chunks: Vec<ChunkInfo>,
    /// Free-form metadata. For EC blocks, [`BLOCK_GROUP_LEN_KEY`] records the
    /// full block-group length.
    pub metadata: BTreeMap<String, String>,
}

impl BlockData {
    /// An empty block with no chunks or metadata.
    pub fn new(block_id: BlockId) -> Self {
        Self {
            block_id,
            chunks: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }

    /// Sum of all chunk lengths — the block's stored byte count. This is the
    /// *physical* shard length; for the user-visible EC group length use
    /// [`BlockData::block_group_len`].
    pub fn len(&self) -> u64 {
        self.chunks.iter().map(|c| c.len).sum()
    }

    /// True if the block has no chunks.
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Parse the EC block-group length from metadata, if present and valid.
    pub fn block_group_len(&self) -> Option<u64> {
        self.metadata.get(BLOCK_GROUP_LEN_KEY).and_then(|v| v.parse().ok())
    }

    /// Record the EC block-group length in metadata.
    pub fn set_block_group_len(&mut self, len: u64) {
        self.metadata
            .insert(BLOCK_GROUP_LEN_KEY.to_string(), len.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ContainerId, LocalId, ReplicaIndex};

    fn sample_block() -> BlockData {
        let mut b = BlockData::new(BlockId::ec(
            ContainerId(1),
            LocalId(2),
            ReplicaIndex::new(1),
        ));
        b.chunks.push(ChunkInfo {
            chunk_name: "c0".to_string(),
            offset: 0,
            len: 100,
            checksum_data: Some(ChecksumData::none()),
            stripe_checksum: None,
        });
        b.chunks.push(ChunkInfo {
            chunk_name: "c1".to_string(),
            offset: 100,
            len: 50,
            checksum_data: None,
            stripe_checksum: None,
        });
        b
    }

    #[test]
    fn block_len_sums_chunks() {
        let b = sample_block();
        assert_eq!(b.len(), 150);
        assert!(!b.is_empty());
        assert!(BlockData::new(b.block_id).is_empty());
    }

    #[test]
    fn block_group_len_round_trips() {
        let mut b = sample_block();
        assert_eq!(b.block_group_len(), None);
        b.set_block_group_len(1 << 30);
        assert_eq!(b.block_group_len(), Some(1 << 30));
    }

    #[test]
    fn block_data_serde_round_trips() {
        let b = sample_block();
        let json = serde_json::to_string(&b).unwrap();
        let back: BlockData = serde_json::from_str(&json).unwrap();
        assert_eq!(b, back);
    }
}
