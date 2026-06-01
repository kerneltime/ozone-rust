//! Container-level aggregate types.

use serde::{Deserialize, Serialize};

use crate::{ContainerId, EcReplicationConfig};

/// Lifecycle state of a container, mirroring Ozone's container state machine.
///
/// The legal transitions the datanode honors:
/// `Open -> Closing -> Closed` (normal close), `Open -> QuasiClosed -> Closed`
/// (close without quorum), and `* -> Unhealthy`/`* -> Deleted` from SCM command.
/// Only [`ContainerState::Open`] accepts writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ContainerState {
    /// Accepting new blocks and chunk writes.
    Open,
    /// Draining; no new blocks, existing writes finishing.
    Closing,
    /// Closed without quorum agreement; read-only, awaiting resolution.
    QuasiClosed,
    /// Immutable and fully closed.
    Closed,
    /// Failed an integrity check; quarantined.
    Unhealthy,
    /// Marked for deletion.
    Deleted,
}

impl ContainerState {
    /// True if the container may accept new blocks/chunks. Only `Open` qualifies.
    pub fn is_writable(self) -> bool {
        matches!(self, ContainerState::Open)
    }

    /// True if the container is permanently immutable (no further writes ever).
    pub fn is_final(self) -> bool {
        matches!(self, ContainerState::Closed | ContainerState::Deleted)
    }
}

/// Summary of a container's identity, state, and usage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerInfo {
    /// Identity.
    pub container_id: ContainerId,
    /// Lifecycle state.
    pub state: ContainerState,
    /// Bytes of chunk data stored across all blocks.
    pub used_bytes: u64,
    /// Number of committed blocks.
    pub block_count: u64,
    /// EC configuration. `None` would indicate a non-EC container; the Rust
    /// datanode only serves EC containers, but the field is optional to mirror
    /// the wire type and stay forward-compatible.
    pub ec_config: Option<EcReplicationConfig>,
}

impl ContainerInfo {
    /// A freshly-created `Open` EC container with zero usage.
    pub fn new_open(container_id: ContainerId, ec_config: EcReplicationConfig) -> Self {
        Self {
            container_id,
            state: ContainerState::Open,
            used_bytes: 0,
            block_count: 0,
            ec_config: Some(ec_config),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_predicates() {
        assert!(ContainerState::Open.is_writable());
        assert!(!ContainerState::Closing.is_writable());
        assert!(!ContainerState::Open.is_final());
        assert!(ContainerState::Closed.is_final());
        assert!(ContainerState::Deleted.is_final());
    }

    #[test]
    fn state_serializes_as_screaming_snake() {
        assert_eq!(
            serde_json::to_string(&ContainerState::QuasiClosed).unwrap(),
            "\"QUASI_CLOSED\""
        );
        assert_eq!(
            serde_json::to_string(&ContainerState::Open).unwrap(),
            "\"OPEN\""
        );
    }

    #[test]
    fn new_open_is_empty_and_writable() {
        let c = ContainerInfo::new_open(ContainerId(7), EcReplicationConfig::RS_6_3_1MIB);
        assert_eq!(c.state, ContainerState::Open);
        assert!(c.state.is_writable());
        assert_eq!(c.used_bytes, 0);
        assert_eq!(c.block_count, 0);
        assert_eq!(c.ec_config, Some(EcReplicationConfig::RS_6_3_1MIB));
    }
}
