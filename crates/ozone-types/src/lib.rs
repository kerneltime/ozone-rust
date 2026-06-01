//! Wire-independent keystone domain types for the Ozone Rust gateway + datanode.
//!
//! These are the clean value types used throughout the codebase in logic and
//! internal APIs. Conversions to/from the prost-generated wire structs live in
//! `ozone-grpc-types` (which depends on this crate), so this crate stays free of
//! any gRPC/protobuf dependency and every other crate can depend on it cheaply.
//!
//! # Invariants enforced here (and therefore NOT re-checked downstream)
//! - [`ReplicaIndex`] is `0` for a non-EC block, or `1..=k+p` for an EC block.
//!   The `1`-indexing matches Apache Ozone's Java convention (slot 1 is the
//!   first data shard), so a slot maps to an ISA-L array position via
//!   [`ReplicaIndex::zero_based`].
//! - [`EcReplicationConfig`] always satisfies `1 <= data <= 32`,
//!   `1 <= parity <= 32`, `ec_chunk_size > 0`. Build one through
//!   [`EcReplicationConfig::new`] (or validate an existing one) at every system
//!   boundary; constructed values are trusted thereafter.
//!
//! # Anti-patterns
//! - Do NOT add a gRPC/prost/tonic dependency to this crate. Conversions belong
//!   in `ozone-grpc-types`.
//! - Do NOT widen [`EcReplicationConfig::data`]/`parity` beyond `u8`; ISA-L caps
//!   `k` and `p` at 32 and the narrow type documents that ceiling.
//!
//! See: notetaker/Projects/Apache Ozone/S3 Gateway Rust/

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

mod chunk;
mod container;

pub use chunk::{
    BlockData, ChecksumData, ChunkInfo, StripeChecksum, BLOCK_GROUP_LEN_KEY,
};
pub use container::{ContainerInfo, ContainerState};

// ============================================================================
// Identifiers
// ============================================================================

/// Identifier of a storage container.
///
/// A container is the unit of replication/EC and the physical grouping of
/// blocks on a datanode. Container ids are allocated by SCM and are globally
/// unique within a cluster.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct ContainerId(pub u64);

impl ContainerId {
    /// The raw numeric id.
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ContainerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for ContainerId {
    #[inline]
    fn from(v: u64) -> Self {
        Self(v)
    }
}

/// Block-local identifier, unique within its [`ContainerId`].
///
/// The `(container_id, local_id)` pair identifies a logical block; an EC block
/// additionally fans out across replica slots sharing the same `local_id`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct LocalId(pub u64);

impl LocalId {
    /// The raw numeric id.
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for LocalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for LocalId {
    #[inline]
    fn from(v: u64) -> Self {
        Self(v)
    }
}

/// EC replica index: a shard's role within its stripe.
///
/// - `0` -> the block is NOT erasure-coded (a single full replica).
/// - `1..=k` -> a *data* shard (1-indexed, per Ozone's Java convention).
/// - `k+1..=k+p` -> a *parity* shard.
///
/// The `k`/`p` boundary is only meaningful relative to a specific
/// [`EcReplicationConfig`]; this type stores just the raw slot. Use
/// [`EcReplicationConfig::is_data`]/[`EcReplicationConfig::is_parity`] to
/// classify, and [`ReplicaIndex::zero_based`] to index an ISA-L shard array.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct ReplicaIndex(u8);

impl ReplicaIndex {
    /// The sentinel for a non-EC (single full replica) block.
    pub const NON_EC: ReplicaIndex = ReplicaIndex(0);

    /// Wrap a raw slot without config validation.
    ///
    /// Prefer [`EcReplicationConfig::replica_index`] when a config is on hand —
    /// it enforces the `1..=k+p` upper bound. This unchecked constructor exists
    /// for decoding wire values whose config is validated separately.
    #[inline]
    pub const fn new(slot: u8) -> Self {
        Self(slot)
    }

    /// The raw slot value (`0` for non-EC).
    #[inline]
    pub const fn get(self) -> u8 {
        self.0
    }

    /// True for the non-EC sentinel (`0`).
    #[inline]
    pub const fn is_non_ec(self) -> bool {
        self.0 == 0
    }

    /// Zero-based shard position (`slot - 1`) for indexing a `k+p` shard array.
    /// `None` for the non-EC sentinel.
    #[inline]
    pub const fn zero_based(self) -> Option<usize> {
        match self.0 {
            0 => None,
            n => Some((n - 1) as usize),
        }
    }
}

impl fmt::Display for ReplicaIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Fully-qualified block identifier: container + local id + replica slot.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct BlockId {
    /// Owning container.
    pub container: ContainerId,
    /// Block id within the container.
    pub local_id: LocalId,
    /// EC replica slot (`0` for non-EC).
    pub replica_index: ReplicaIndex,
}

impl BlockId {
    /// Construct a non-EC block id.
    #[inline]
    pub const fn non_ec(container: ContainerId, local_id: LocalId) -> Self {
        Self {
            container,
            local_id,
            replica_index: ReplicaIndex::NON_EC,
        }
    }

    /// Construct an EC block id at a given replica slot.
    #[inline]
    pub const fn ec(container: ContainerId, local_id: LocalId, replica_index: ReplicaIndex) -> Self {
        Self {
            container,
            local_id,
            replica_index,
        }
    }
}

impl fmt::Display for BlockId {
    /// Renders as `container/local#slot`, e.g. `42/7#3`. Non-EC blocks render
    /// with slot `0`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}#{}",
            self.container, self.local_id, self.replica_index
        )
    }
}

/// A datanode's stable UUID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DatanodeUuid(pub Uuid);

impl DatanodeUuid {
    /// Generate a fresh random (v4) datanode UUID.
    pub fn new_random() -> Self {
        Self(Uuid::new_v4())
    }

    /// The underlying [`Uuid`].
    #[inline]
    pub const fn inner(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for DatanodeUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for DatanodeUuid {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::from_str(s).map(Self)
    }
}

// ============================================================================
// Erasure coding configuration
// ============================================================================

/// Erasure-coding codec family. Only Reed-Solomon is implemented.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum EcCodec {
    /// Reed-Solomon over GF(2^8) with a Cauchy generator matrix.
    #[default]
    Rs,
}

impl fmt::Display for EcCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EcCodec::Rs => f.write_str("RS"),
        }
    }
}

/// Returned when an EC codec string is not recognized.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("unknown EC codec: {0:?} (supported: RS)")]
pub struct UnknownCodec(pub String);

impl FromStr for EcCodec {
    type Err = UnknownCodec;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("rs") {
            Ok(EcCodec::Rs)
        } else {
            Err(UnknownCodec(s.to_owned()))
        }
    }
}

/// Erasure-coding parameters for a key or container.
///
/// Construct via [`EcReplicationConfig::new`] (validates) or the
/// [`EcReplicationConfig::rs`] helper. The standard production profiles are
/// exposed as constants: [`EcReplicationConfig::RS_3_2_1MIB`],
/// [`EcReplicationConfig::RS_6_3_1MIB`], [`EcReplicationConfig::RS_10_4_1MIB`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EcReplicationConfig {
    /// `k` — data shards per stripe. `1..=32`.
    pub data: u8,
    /// `p` — parity shards per stripe. `1..=32`.
    pub parity: u8,
    /// EC cell size in bytes (Ozone default: 1 MiB).
    pub ec_chunk_size: u32,
    /// Codec family.
    pub codec: EcCodec,
}

/// Validation failures for [`EcReplicationConfig`] and replica slots.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EcConfigError {
    /// `data` outside `1..=32`.
    #[error("EC data shards must be in 1..=32, got {0}")]
    Data(u8),
    /// `parity` outside `1..=32`.
    #[error("EC parity shards must be in 1..=32, got {0}")]
    Parity(u8),
    /// `ec_chunk_size` was zero.
    #[error("EC chunk size must be > 0")]
    ChunkSize,
    /// A replica slot fell outside `1..=k+p`.
    #[error("replica slot {slot} out of range 1..={total}")]
    ReplicaSlot {
        /// The offending slot.
        slot: u8,
        /// `k+p`.
        total: u16,
    },
}

impl EcReplicationConfig {
    /// `rs-3-2-1024k` — small clusters.
    pub const RS_3_2_1MIB: Self = Self::rs(3, 2, 1024 * 1024);
    /// `rs-6-3-1024k` — the most common production setting.
    pub const RS_6_3_1MIB: Self = Self::rs(6, 3, 1024 * 1024);
    /// `rs-10-4-1024k` — large clusters.
    pub const RS_10_4_1MIB: Self = Self::rs(10, 4, 1024 * 1024);

    /// Build a Reed-Solomon config WITHOUT validation (const-friendly).
    ///
    /// Used by the profile constants, whose arguments are known-valid. For
    /// runtime/untrusted inputs use [`EcReplicationConfig::new`].
    #[inline]
    pub const fn rs(data: u8, parity: u8, ec_chunk_size: u32) -> Self {
        Self {
            data,
            parity,
            ec_chunk_size,
            codec: EcCodec::Rs,
        }
    }

    /// Construct and validate.
    pub fn new(
        data: u8,
        parity: u8,
        ec_chunk_size: u32,
        codec: EcCodec,
    ) -> Result<Self, EcConfigError> {
        let cfg = Self {
            data,
            parity,
            ec_chunk_size,
            codec,
        };
        cfg.validate()?;
        Ok(cfg)
    }

    /// Check the range invariants. Cheap; call at system boundaries.
    pub fn validate(&self) -> Result<(), EcConfigError> {
        if self.data == 0 || self.data > 32 {
            return Err(EcConfigError::Data(self.data));
        }
        if self.parity == 0 || self.parity > 32 {
            return Err(EcConfigError::Parity(self.parity));
        }
        if self.ec_chunk_size == 0 {
            return Err(EcConfigError::ChunkSize);
        }
        Ok(())
    }

    /// Total shards per stripe (`k + p`). At most `64`, so a `u16` holds it.
    #[inline]
    pub const fn total(&self) -> u16 {
        self.data as u16 + self.parity as u16
    }

    /// Bytes in a full stripe across all data shards (`k * ec_chunk_size`).
    #[inline]
    pub const fn stripe_size(&self) -> u64 {
        self.data as u64 * self.ec_chunk_size as u64
    }

    /// True if `idx` names a data shard slot (`1..=k`).
    #[inline]
    pub fn is_data(&self, idx: ReplicaIndex) -> bool {
        let s = idx.get() as u16;
        s >= 1 && s <= self.data as u16
    }

    /// True if `idx` names a parity shard slot (`k+1..=k+p`).
    #[inline]
    pub fn is_parity(&self, idx: ReplicaIndex) -> bool {
        let s = idx.get() as u16;
        s > self.data as u16 && s <= self.total()
    }

    /// Build a validated replica index in `1..=k+p`.
    pub fn replica_index(&self, slot: u8) -> Result<ReplicaIndex, EcConfigError> {
        let s = slot as u16;
        if s >= 1 && s <= self.total() {
            Ok(ReplicaIndex::new(slot))
        } else {
            Err(EcConfigError::ReplicaSlot {
                slot,
                total: self.total(),
            })
        }
    }
}

// ============================================================================
// Checksums
// ============================================================================

/// Checksum algorithm for chunk-data integrity. Mirrors the wire enum;
/// CRC-32C is Ozone's default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ChecksumType {
    /// No checksum.
    None,
    /// CRC-32 (IEEE 802.3).
    Crc32,
    /// CRC-32C (Castagnoli) — Ozone default.
    #[default]
    Crc32c,
    /// SHA-256.
    Sha256,
    /// MD5.
    Md5,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn container_and_local_id_display_and_from() {
        assert_eq!(ContainerId::from(42).get(), 42);
        assert_eq!(ContainerId(42).to_string(), "42");
        assert_eq!(LocalId::from(7).to_string(), "7");
    }

    #[test]
    fn replica_index_semantics() {
        assert!(ReplicaIndex::NON_EC.is_non_ec());
        assert_eq!(ReplicaIndex::NON_EC.zero_based(), None);
        assert_eq!(ReplicaIndex::new(1).zero_based(), Some(0));
        assert_eq!(ReplicaIndex::new(9).zero_based(), Some(8));
        assert!(!ReplicaIndex::new(1).is_non_ec());
    }

    #[test]
    fn block_id_display() {
        let b = BlockId::ec(ContainerId(42), LocalId(7), ReplicaIndex::new(3));
        assert_eq!(b.to_string(), "42/7#3");
        let n = BlockId::non_ec(ContainerId(1), LocalId(2));
        assert_eq!(n.to_string(), "1/2#0");
    }

    #[test]
    fn ec_config_validation() {
        assert!(EcReplicationConfig::new(6, 3, 1 << 20, EcCodec::Rs).is_ok());
        assert_eq!(
            EcReplicationConfig::new(0, 3, 1 << 20, EcCodec::Rs),
            Err(EcConfigError::Data(0))
        );
        assert_eq!(
            EcReplicationConfig::new(33, 3, 1 << 20, EcCodec::Rs),
            Err(EcConfigError::Data(33))
        );
        assert_eq!(
            EcReplicationConfig::new(6, 0, 1 << 20, EcCodec::Rs),
            Err(EcConfigError::Parity(0))
        );
        assert_eq!(
            EcReplicationConfig::new(6, 3, 0, EcCodec::Rs),
            Err(EcConfigError::ChunkSize)
        );
    }

    #[test]
    fn ec_config_profiles_are_valid_and_sized() {
        for cfg in [
            EcReplicationConfig::RS_3_2_1MIB,
            EcReplicationConfig::RS_6_3_1MIB,
            EcReplicationConfig::RS_10_4_1MIB,
        ] {
            cfg.validate().unwrap();
        }
        assert_eq!(EcReplicationConfig::RS_6_3_1MIB.total(), 9);
        assert_eq!(
            EcReplicationConfig::RS_6_3_1MIB.stripe_size(),
            6 * 1024 * 1024
        );
        assert_eq!(EcReplicationConfig::RS_10_4_1MIB.total(), 14);
    }

    #[test]
    fn ec_config_classifies_replica_slots() {
        let cfg = EcReplicationConfig::RS_6_3_1MIB; // k=6, p=3, total=9
        // Data slots 1..=6.
        for s in 1..=6u8 {
            let idx = cfg.replica_index(s).unwrap();
            assert!(cfg.is_data(idx), "slot {s} should be data");
            assert!(!cfg.is_parity(idx), "slot {s} should not be parity");
        }
        // Parity slots 7..=9.
        for s in 7..=9u8 {
            let idx = cfg.replica_index(s).unwrap();
            assert!(cfg.is_parity(idx), "slot {s} should be parity");
            assert!(!cfg.is_data(idx), "slot {s} should not be data");
        }
        // Out of range.
        assert_eq!(
            cfg.replica_index(0),
            Err(EcConfigError::ReplicaSlot { slot: 0, total: 9 })
        );
        assert_eq!(
            cfg.replica_index(10),
            Err(EcConfigError::ReplicaSlot { slot: 10, total: 9 })
        );
        // The non-EC sentinel is neither data nor parity under any config.
        assert!(!cfg.is_data(ReplicaIndex::NON_EC));
        assert!(!cfg.is_parity(ReplicaIndex::NON_EC));
    }

    #[test]
    fn ec_codec_parse_and_display() {
        assert_eq!("RS".parse::<EcCodec>().unwrap(), EcCodec::Rs);
        assert_eq!("rs".parse::<EcCodec>().unwrap(), EcCodec::Rs);
        assert_eq!(EcCodec::Rs.to_string(), "RS");
        assert!("xor".parse::<EcCodec>().is_err());
    }

    #[test]
    fn datanode_uuid_round_trip() {
        let u = DatanodeUuid::new_random();
        let s = u.to_string();
        let parsed: DatanodeUuid = s.parse().unwrap();
        assert_eq!(u, parsed);
        assert!("not-a-uuid".parse::<DatanodeUuid>().is_err());
    }

    #[test]
    fn serde_newtypes_are_transparent() {
        // Transparent newtypes serialize as their inner scalar.
        let j = serde_json::to_string(&ContainerId(42)).unwrap();
        assert_eq!(j, "42");
        let j = serde_json::to_string(&ReplicaIndex::new(3)).unwrap();
        assert_eq!(j, "3");
        let cfg = EcReplicationConfig::RS_6_3_1MIB;
        let round: EcReplicationConfig =
            serde_json::from_str(&serde_json::to_string(&cfg).unwrap()).unwrap();
        assert_eq!(round, cfg);
    }
}
