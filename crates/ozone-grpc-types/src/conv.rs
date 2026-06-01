//! Conversions between the prost wire types (in [`crate::dn::v1`]) and the
//! `ozone-types` domain types.
//!
//! These live here, not in `ozone-types`, for two reasons: the orphan rule (the
//! wire types are defined in this crate) and dependency direction (`ozone-types`
//! must stay free of any protobuf dependency).
//!
//! Direction conventions:
//! - **domain -> wire** is always infallible ([`From`]). Domain values have
//!   already passed validation, and every domain value maps to a representable
//!   wire value (narrow `u8` widens to `u32`, enums map one-to-one).
//! - **wire -> domain** is fallible ([`TryFrom`], yielding [`ConversionError`]).
//!   Wire values arrive from the network: enum fields may be `UNSPECIFIED`,
//!   required nested messages may be absent, and numeric fields may exceed the
//!   domain type's range.

use std::collections::BTreeMap;

use ozone_types as dom;
use thiserror::Error;

use crate::dn::v1 as wire;

/// Failure decoding a wire message into a domain type.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ConversionError {
    /// A required nested message was `None` on the wire.
    #[error("missing required field: {0}")]
    MissingField(&'static str),

    /// An enum field held a value with no domain meaning (e.g. the proto
    /// `*_UNSPECIFIED` zero value, or an unknown number).
    #[error("invalid enum value {value} for field {field}")]
    BadEnum {
        /// Dotted field path.
        field: &'static str,
        /// The offending wire integer.
        value: i32,
    },

    /// A numeric field exceeded the domain type's range (e.g. a replica index
    /// or EC shard count above `u8::MAX`).
    #[error("value {value} out of range for field {field}")]
    OutOfRange {
        /// Dotted field path.
        field: &'static str,
        /// The offending wire value.
        value: u32,
    },

    /// EC parameters failed domain validation.
    #[error(transparent)]
    EcConfig(#[from] dom::EcConfigError),

    /// The EC codec string was not recognized.
    #[error("unknown EC codec: {0:?}")]
    Codec(String),
}

// ---------------------------------------------------------------------------
// ContainerId  (u64, infallible both ways)
// ---------------------------------------------------------------------------

impl From<dom::ContainerId> for wire::ContainerId {
    fn from(d: dom::ContainerId) -> Self {
        wire::ContainerId { id: d.get() }
    }
}

impl From<wire::ContainerId> for dom::ContainerId {
    fn from(w: wire::ContainerId) -> Self {
        dom::ContainerId(w.id)
    }
}

// ---------------------------------------------------------------------------
// BlockId
// ---------------------------------------------------------------------------

impl From<dom::BlockId> for wire::BlockId {
    fn from(d: dom::BlockId) -> Self {
        wire::BlockId {
            container_id: d.container.get(),
            local_id: d.local_id.get(),
            replica_index: d.replica_index.get() as u32,
        }
    }
}

impl TryFrom<wire::BlockId> for dom::BlockId {
    type Error = ConversionError;
    fn try_from(w: wire::BlockId) -> Result<Self, Self::Error> {
        let replica = u8::try_from(w.replica_index).map_err(|_| ConversionError::OutOfRange {
            field: "BlockId.replica_index",
            value: w.replica_index,
        })?;
        Ok(dom::BlockId {
            container: dom::ContainerId(w.container_id),
            local_id: dom::LocalId(w.local_id),
            replica_index: dom::ReplicaIndex::new(replica),
        })
    }
}

// ---------------------------------------------------------------------------
// EcReplicationConfig
// ---------------------------------------------------------------------------

impl From<dom::EcReplicationConfig> for wire::EcReplicationConfig {
    fn from(d: dom::EcReplicationConfig) -> Self {
        wire::EcReplicationConfig {
            data: d.data as u32,
            parity: d.parity as u32,
            ec_chunk_size: d.ec_chunk_size,
            codec: d.codec.to_string(),
        }
    }
}

impl TryFrom<wire::EcReplicationConfig> for dom::EcReplicationConfig {
    type Error = ConversionError;
    fn try_from(w: wire::EcReplicationConfig) -> Result<Self, Self::Error> {
        let codec: dom::EcCodec = w
            .codec
            .parse()
            .map_err(|_| ConversionError::Codec(w.codec.clone()))?;
        let data = u8::try_from(w.data).map_err(|_| ConversionError::OutOfRange {
            field: "EcReplicationConfig.data",
            value: w.data,
        })?;
        let parity = u8::try_from(w.parity).map_err(|_| ConversionError::OutOfRange {
            field: "EcReplicationConfig.parity",
            value: w.parity,
        })?;
        Ok(dom::EcReplicationConfig::new(
            data,
            parity,
            w.ec_chunk_size,
            codec,
        )?)
    }
}

// ---------------------------------------------------------------------------
// ChecksumType  (domain enum <-> prost enum)
// ---------------------------------------------------------------------------

impl From<dom::ChecksumType> for wire::ChecksumType {
    fn from(d: dom::ChecksumType) -> Self {
        match d {
            dom::ChecksumType::None => wire::ChecksumType::None,
            dom::ChecksumType::Crc32 => wire::ChecksumType::Crc32,
            dom::ChecksumType::Crc32c => wire::ChecksumType::Crc32c,
            dom::ChecksumType::Sha256 => wire::ChecksumType::Sha256,
            dom::ChecksumType::Md5 => wire::ChecksumType::Md5,
        }
    }
}

impl TryFrom<wire::ChecksumType> for dom::ChecksumType {
    type Error = ConversionError;
    fn try_from(w: wire::ChecksumType) -> Result<Self, Self::Error> {
        match w {
            wire::ChecksumType::None => Ok(dom::ChecksumType::None),
            wire::ChecksumType::Crc32 => Ok(dom::ChecksumType::Crc32),
            wire::ChecksumType::Crc32c => Ok(dom::ChecksumType::Crc32c),
            wire::ChecksumType::Sha256 => Ok(dom::ChecksumType::Sha256),
            wire::ChecksumType::Md5 => Ok(dom::ChecksumType::Md5),
            wire::ChecksumType::Unspecified => Err(ConversionError::BadEnum {
                field: "ChecksumData.type",
                value: w as i32,
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// ChecksumData / StripeChecksum
// ---------------------------------------------------------------------------

impl From<dom::ChecksumData> for wire::ChecksumData {
    fn from(d: dom::ChecksumData) -> Self {
        wire::ChecksumData {
            r#type: wire::ChecksumType::from(d.checksum_type) as i32,
            bytes_per_checksum: d.bytes_per_checksum,
            checksums: d.checksums,
        }
    }
}

impl TryFrom<wire::ChecksumData> for dom::ChecksumData {
    type Error = ConversionError;
    fn try_from(w: wire::ChecksumData) -> Result<Self, Self::Error> {
        let wire_ty =
            wire::ChecksumType::try_from(w.r#type).map_err(|_| ConversionError::BadEnum {
                field: "ChecksumData.type",
                value: w.r#type,
            })?;
        Ok(dom::ChecksumData {
            checksum_type: dom::ChecksumType::try_from(wire_ty)?,
            bytes_per_checksum: w.bytes_per_checksum,
            checksums: w.checksums,
        })
    }
}

impl From<dom::StripeChecksum> for wire::StripeChecksum {
    fn from(d: dom::StripeChecksum) -> Self {
        wire::StripeChecksum {
            per_stripe_checksums: d.per_stripe_checksums,
        }
    }
}

impl From<wire::StripeChecksum> for dom::StripeChecksum {
    fn from(w: wire::StripeChecksum) -> Self {
        dom::StripeChecksum {
            per_stripe_checksums: w.per_stripe_checksums,
        }
    }
}

// ---------------------------------------------------------------------------
// ChunkInfo
// ---------------------------------------------------------------------------

impl From<dom::ChunkInfo> for wire::ChunkInfo {
    fn from(d: dom::ChunkInfo) -> Self {
        wire::ChunkInfo {
            chunk_name: d.chunk_name,
            offset: d.offset,
            len: d.len,
            checksum_data: d.checksum_data.map(Into::into),
            stripe_checksum: d.stripe_checksum.map(Into::into),
        }
    }
}

impl TryFrom<wire::ChunkInfo> for dom::ChunkInfo {
    type Error = ConversionError;
    fn try_from(w: wire::ChunkInfo) -> Result<Self, Self::Error> {
        Ok(dom::ChunkInfo {
            chunk_name: w.chunk_name,
            offset: w.offset,
            len: w.len,
            checksum_data: w.checksum_data.map(TryInto::try_into).transpose()?,
            stripe_checksum: w.stripe_checksum.map(Into::into),
        })
    }
}

// ---------------------------------------------------------------------------
// BlockData  (map HashMap <-> BTreeMap; required block_id)
// ---------------------------------------------------------------------------

impl From<dom::BlockData> for wire::BlockData {
    fn from(d: dom::BlockData) -> Self {
        wire::BlockData {
            block_id: Some(d.block_id.into()),
            chunks: d.chunks.into_iter().map(Into::into).collect(),
            metadata: d.metadata.into_iter().collect(),
        }
    }
}

impl TryFrom<wire::BlockData> for dom::BlockData {
    type Error = ConversionError;
    fn try_from(w: wire::BlockData) -> Result<Self, Self::Error> {
        let block_id = w
            .block_id
            .ok_or(ConversionError::MissingField("BlockData.block_id"))?
            .try_into()?;
        let chunks = w
            .chunks
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;
        let metadata: BTreeMap<String, String> = w.metadata.into_iter().collect();
        Ok(dom::BlockData {
            block_id,
            chunks,
            metadata,
        })
    }
}

// ---------------------------------------------------------------------------
// ContainerState  (domain enum <-> prost container_state::State)
// ---------------------------------------------------------------------------

impl From<dom::ContainerState> for wire::container_state::State {
    fn from(d: dom::ContainerState) -> Self {
        use wire::container_state::State;
        match d {
            dom::ContainerState::Open => State::Open,
            dom::ContainerState::Closing => State::Closing,
            dom::ContainerState::QuasiClosed => State::QuasiClosed,
            dom::ContainerState::Closed => State::Closed,
            dom::ContainerState::Unhealthy => State::Unhealthy,
            dom::ContainerState::Deleted => State::Deleted,
        }
    }
}

impl TryFrom<wire::container_state::State> for dom::ContainerState {
    type Error = ConversionError;
    fn try_from(w: wire::container_state::State) -> Result<Self, Self::Error> {
        use wire::container_state::State;
        match w {
            State::Open => Ok(dom::ContainerState::Open),
            State::Closing => Ok(dom::ContainerState::Closing),
            State::QuasiClosed => Ok(dom::ContainerState::QuasiClosed),
            State::Closed => Ok(dom::ContainerState::Closed),
            State::Unhealthy => Ok(dom::ContainerState::Unhealthy),
            State::Deleted => Ok(dom::ContainerState::Deleted),
            State::Unspecified => Err(ConversionError::BadEnum {
                field: "ContainerState.state",
                value: w as i32,
            }),
        }
    }
}

/// Encode a domain [`dom::ContainerState`] as the wire `i32` for a
/// `ContainerState.state` field.
pub fn container_state_to_wire(d: dom::ContainerState) -> i32 {
    wire::container_state::State::from(d) as i32
}

/// Decode a wire `i32` `ContainerState.state` field into a domain state.
pub fn container_state_from_wire(value: i32) -> Result<dom::ContainerState, ConversionError> {
    let state = wire::container_state::State::try_from(value).map_err(|_| {
        ConversionError::BadEnum {
            field: "ContainerState.state",
            value,
        }
    })?;
    dom::ContainerState::try_from(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ozone_types::{
        BlockData, BlockId, ChecksumData, ChecksumType, ChunkInfo, ContainerId, ContainerState,
        EcReplicationConfig, LocalId, ReplicaIndex,
    };

    #[test]
    fn block_id_round_trip() {
        let d = BlockId::ec(ContainerId(42), LocalId(7), ReplicaIndex::new(3));
        let w: wire::BlockId = d.into();
        assert_eq!(w.replica_index, 3);
        let back: BlockId = w.try_into().unwrap();
        assert_eq!(back, d);
    }

    #[test]
    fn ec_config_round_trip_and_codec_string() {
        let d = EcReplicationConfig::RS_6_3_1MIB;
        let w: wire::EcReplicationConfig = d.into();
        assert_eq!(w.codec, "RS");
        assert_eq!(w.data, 6);
        let back: EcReplicationConfig = w.try_into().unwrap();
        assert_eq!(back, d);
    }

    #[test]
    fn ec_config_rejects_unknown_codec() {
        let w = wire::EcReplicationConfig {
            data: 6,
            parity: 3,
            ec_chunk_size: 1 << 20,
            codec: "XOR".to_string(),
        };
        assert!(matches!(
            EcReplicationConfig::try_from(w),
            Err(ConversionError::Codec(_))
        ));
    }

    #[test]
    fn ec_config_rejects_out_of_range_shard_count() {
        let w = wire::EcReplicationConfig {
            data: 6,
            parity: 99, // > 32, caught by domain validation
            ec_chunk_size: 1 << 20,
            codec: "RS".to_string(),
        };
        assert!(matches!(
            EcReplicationConfig::try_from(w),
            Err(ConversionError::EcConfig(_))
        ));
    }

    #[test]
    fn checksum_data_round_trip() {
        let d = ChecksumData {
            checksum_type: ChecksumType::Crc32c,
            bytes_per_checksum: 256,
            checksums: vec![vec![1, 2, 3, 4], vec![5, 6, 7, 8]],
        };
        let w: wire::ChecksumData = d.clone().into();
        assert_eq!(w.r#type, wire::ChecksumType::Crc32c as i32);
        let back: ChecksumData = w.try_into().unwrap();
        assert_eq!(back, d);
    }

    #[test]
    fn checksum_unspecified_is_rejected() {
        let w = wire::ChecksumData {
            r#type: wire::ChecksumType::Unspecified as i32,
            bytes_per_checksum: 0,
            checksums: vec![],
        };
        assert!(matches!(
            ChecksumData::try_from(w),
            Err(ConversionError::BadEnum { .. })
        ));
    }

    #[test]
    fn block_data_round_trip_with_metadata() {
        let mut d = BlockData::new(BlockId::ec(ContainerId(1), LocalId(2), ReplicaIndex::new(1)));
        d.set_block_group_len(1 << 30);
        d.chunks.push(ChunkInfo {
            chunk_name: "c0".into(),
            offset: 0,
            len: 100,
            checksum_data: Some(ChecksumData::none()),
            stripe_checksum: None,
        });
        let w: wire::BlockData = d.clone().into();
        assert!(w.block_id.is_some());
        let back: BlockData = w.try_into().unwrap();
        assert_eq!(back, d);
    }

    #[test]
    fn block_data_missing_block_id_is_error() {
        let w = wire::BlockData {
            block_id: None,
            chunks: vec![],
            metadata: Default::default(),
        };
        assert!(matches!(
            BlockData::try_from(w),
            Err(ConversionError::MissingField("BlockData.block_id"))
        ));
    }

    #[test]
    fn container_state_round_trip_all_variants() {
        for d in [
            ContainerState::Open,
            ContainerState::Closing,
            ContainerState::QuasiClosed,
            ContainerState::Closed,
            ContainerState::Unhealthy,
            ContainerState::Deleted,
        ] {
            let i = container_state_to_wire(d);
            assert_eq!(container_state_from_wire(i).unwrap(), d);
        }
        // The proto zero value has no domain meaning.
        assert!(matches!(
            container_state_from_wire(0),
            Err(ConversionError::BadEnum { .. })
        ));
    }
}
