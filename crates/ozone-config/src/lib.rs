//! Typed configuration for the two Ozone binaries: the datanode and the S3
//! gateway. Both are loaded from a single TOML file via
//! [`DatanodeConfig::from_toml_path`] / [`GatewayConfig::from_toml_path`].
//!
//! # Design
//!
//! Fields are typed at the strongest level the wire format allows, so that
//! parsing *is* most of the validation. In particular bind addresses are
//! [`SocketAddr`], not strings: a malformed `host:port` is rejected by serde
//! during the TOML parse, before [`DatanodeConfig::validate`] ever runs. The
//! only checks `validate` adds on top are the ones serde cannot express on a
//! single field — non-emptiness of collections and of the remote-endpoint
//! strings (those stay `String` because they are dialed lazily by a gRPC
//! client, may be DNS names, and must tolerate being unresolved at load time).
//!
//! # Boundary, not core
//!
//! Validation lives exclusively in `from_toml_path` (the trust boundary). Code
//! that already holds a `DatanodeConfig`/`GatewayConfig` may assume it is
//! valid; these types are plain data and intentionally carry no further
//! invariants beyond "constructed through the loader or `Default`". `Default`
//! is itself a valid configuration except where a path must name real
//! on-disk state the operator has to supply (UUID file, data dirs, metadata
//! dir, DB/cert paths) — those default to empty/placeholder and are expected
//! to be overridden.
//!
//! # Durations
//!
//! TOML has no duration scalar, so [`Duration`] fields are encoded as integer
//! seconds through the [`duration_secs`] module and the `#[serde(with = ...)]`
//! attribute. Sub-second precision is deliberately unrepresentable: every
//! duration here is an operational interval where seconds are the natural and
//! sufficient granularity.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ozone_types::EcReplicationConfig;
use serde::{Deserialize, Serialize};

/// Errors raised while loading or validating a configuration file.
///
/// `Io` and `Toml` are surfaced verbatim from the loader so the operator sees
/// the exact filesystem or parse failure (line/column for TOML). `Invalid`
/// carries the semantic checks from [`DatanodeConfig::validate`] /
/// [`GatewayConfig::validate`] that cannot be expressed in the type system.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The config file could not be read (missing, permissions, etc.).
    #[error("failed to read config file: {0}")]
    Io(#[from] std::io::Error),
    /// The file was read but is not valid TOML, or a field has the wrong
    /// shape/type (e.g. a malformed `host:port` bind address).
    #[error("failed to parse config TOML: {0}")]
    Toml(#[from] toml::de::Error),
    /// The TOML parsed into the right shape but violates a semantic rule
    /// (e.g. empty `data_dirs`, blank remote endpoint).
    #[error("invalid config: {0}")]
    Invalid(String),
}

/// Paths backing mutual-TLS (mTLS) for a gRPC/HTTP endpoint.
///
/// Presence of this block (`Some`) is what turns TLS on for the owning config;
/// all three paths are then required. The files are not opened or parsed here —
/// loading and validating the PEM material is the transport layer's job, so a
/// `TlsConfig` whose paths do not yet exist still round-trips through TOML.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// CA bundle used to verify the peer certificate chain.
    pub ca_cert: PathBuf,
    /// This endpoint's own certificate (leaf, PEM).
    pub cert: PathBuf,
    /// Private key matching `cert` (PEM).
    pub key: PathBuf,
}

/// Configuration for the datanode binary.
///
/// The datanode serves chunk I/O over the `DatanodeGatewayService` gRPC API,
/// persists blocks under `data_dirs`, keeps its metadata in a fjall database at
/// `metadata_dir`, and registers itself with the SCM at `scm_address`. Its
/// stable identity is a UUID persisted at `uuid_file` across restarts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatanodeConfig {
    /// File where the datanode persists its [`ozone_types::DatanodeUuid`] so
    /// the same identity survives restarts. Generated on first boot if absent.
    pub uuid_file: PathBuf,
    /// Chunk-storage roots. Must be non-empty; the datanode spreads blocks
    /// across these volumes. Order is significant only to the storage layer.
    pub data_dirs: Vec<PathBuf>,
    /// Directory holding the fjall metadata database (block/container indexes).
    pub metadata_dir: PathBuf,
    /// Bind address for the `DatanodeGatewayService` gRPC server.
    /// Defaults to `0.0.0.0:19864` (Ozone's standard DN client-RPC port).
    pub listen_addr: SocketAddr,
    /// SCM endpoint (`host:port`) this datanode registers and heartbeats with.
    /// Kept as a string: it may be a DNS name and need not resolve at load.
    pub scm_address: String,
    /// Interval between heartbeats to the SCM. Encoded in TOML as integer
    /// seconds. Defaults to 30s.
    #[serde(with = "duration_secs")]
    pub heartbeat_interval: Duration,
    /// Optional mTLS material for the gRPC server. `None` disables TLS.
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

impl Default for DatanodeConfig {
    fn default() -> Self {
        Self {
            uuid_file: PathBuf::new(),
            data_dirs: Vec::new(),
            metadata_dir: PathBuf::new(),
            // 0.0.0.0:19864 — Ozone's conventional datanode client-RPC port.
            listen_addr: SocketAddr::from(([0, 0, 0, 0], 19864)),
            scm_address: String::new(),
            heartbeat_interval: Duration::from_secs(30),
            tls: None,
        }
    }
}

impl DatanodeConfig {
    /// Read `path`, parse it as TOML, then [`validate`](Self::validate).
    ///
    /// The only entry point that should be trusted to produce a sound config;
    /// every error case is a [`ConfigError`] variant.
    pub fn from_toml_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path)?;
        Self::from_toml_str(&raw)
    }

    /// Parse a TOML string and validate it. Exposed primarily so tests (and
    /// callers with config already in memory) avoid a temp file; `from_toml_path`
    /// is the thin file-reading wrapper over this.
    pub fn from_toml_str(toml_str: &str) -> Result<Self, ConfigError> {
        let cfg: Self = toml::from_str(toml_str)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Enforce the rules serde cannot express per-field.
    ///
    /// Bind addresses are already validated by their [`SocketAddr`] type during
    /// parsing, so this only checks: at least one `data_dirs` entry, and a
    /// non-blank `scm_address`.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.data_dirs.is_empty() {
            return Err(ConfigError::Invalid(
                "data_dirs must contain at least one path".into(),
            ));
        }
        if self.scm_address.trim().is_empty() {
            return Err(ConfigError::Invalid("scm_address must not be empty".into()));
        }
        Ok(())
    }
}

/// Configuration for the S3 gateway binary.
///
/// The gateway terminates the S3 HTTP API on `listen_addr` and translates each
/// request into Ozone Manager (OM) gRPC calls against `om_address`. EC layout
/// for newly created keys and the S3 `ListObjects` page size are policy knobs
/// that have working defaults but are commonly tuned per deployment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    /// Bind address for the S3 HTTP server. Defaults to `0.0.0.0:9878`
    /// (Ozone's standard S3 gateway port).
    pub listen_addr: SocketAddr,
    /// Ozone Manager gRPC endpoint (`host:port`). Kept as a string: may be a
    /// DNS name and need not resolve at load time.
    pub om_address: String,
    /// S3 region advertised to clients and used in SigV4. Defaults to
    /// `"us-east-1"`.
    #[serde(default = "default_region")]
    pub region: String,
    /// EC profile applied to keys created without an explicit replication
    /// config. Defaults to [`EcReplicationConfig::RS_6_3_1MIB`]. In TOML this
    /// is a sub-table; its `codec` serializes as `"RS"`.
    #[serde(default = "default_ec")]
    pub default_ec: EcReplicationConfig,
    /// Default `max-keys` for `ListObjects(V2)` when the client omits it.
    /// Defaults to 1000 (the AWS S3 default). An explicit client value still
    /// overrides this at request time.
    #[serde(default = "default_max_keys")]
    pub max_keys_default: u32,
    /// Optional mTLS material for the HTTP server. `None` serves plain HTTP.
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            // 0.0.0.0:9878 — Ozone's conventional S3 gateway HTTP port.
            listen_addr: SocketAddr::from(([0, 0, 0, 0], 9878)),
            om_address: String::new(),
            region: default_region(),
            default_ec: default_ec(),
            max_keys_default: default_max_keys(),
            tls: None,
        }
    }
}

impl GatewayConfig {
    /// Read `path`, parse it as TOML, then [`validate`](Self::validate).
    pub fn from_toml_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path)?;
        Self::from_toml_str(&raw)
    }

    /// Parse a TOML string and validate it. See
    /// [`DatanodeConfig::from_toml_str`] for the rationale.
    pub fn from_toml_str(toml_str: &str) -> Result<Self, ConfigError> {
        let cfg: Self = toml::from_str(toml_str)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Enforce the rules serde cannot express per-field.
    ///
    /// `listen_addr` is validated by its type during parsing; this only checks
    /// that `om_address` is non-blank.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.om_address.trim().is_empty() {
            return Err(ConfigError::Invalid("om_address must not be empty".into()));
        }
        Ok(())
    }
}

/// S3 region default. AWS treats `us-east-1` as the global/legacy default, so
/// it is the least-surprising value for clients that send no region.
fn default_region() -> String {
    "us-east-1".to_string()
}

/// Default EC profile for new keys: RS(6,3) with a 1 MiB cell.
fn default_ec() -> EcReplicationConfig {
    EcReplicationConfig::RS_6_3_1MIB
}

/// Default `ListObjects` page size, matching AWS S3's documented default.
fn default_max_keys() -> u32 {
    1000
}

/// (De)serialize a [`std::time::Duration`] as a whole number of seconds.
///
/// TOML has no duration type, so duration fields are written as plain integers
/// and interpreted as seconds. Use via `#[serde(with = "duration_secs")]`.
///
/// Round-trip note: serialization truncates toward zero
/// ([`Duration::as_secs`]), so any sub-second remainder is dropped. This is
/// acceptable because every duration in this crate is a coarse operational
/// interval (e.g. heartbeat period) where seconds are the intended precision;
/// do not reuse this module for durations that need millisecond fidelity.
pub mod duration_secs {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    /// Serialize as `u64` seconds (sub-second part truncated).
    pub fn serialize<S>(d: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(d.as_secs())
    }

    /// Deserialize a `u64` second count into a [`Duration`].
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(secs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datanode_round_trips_through_toml() {
        let toml = r#"
            uuid_file = "/var/lib/ozone/dn.uuid"
            data_dirs = ["/data/1", "/data/2"]
            metadata_dir = "/var/lib/ozone/dn-meta"
            listen_addr = "0.0.0.0:19864"
            scm_address = "scm.internal:9861"
            heartbeat_interval = 45

            [tls]
            ca_cert = "/etc/ozone/ca.pem"
            cert = "/etc/ozone/dn.pem"
            key = "/etc/ozone/dn.key"
        "#;

        let cfg = DatanodeConfig::from_toml_str(toml).expect("valid datanode config");
        assert_eq!(cfg.data_dirs.len(), 2);
        assert_eq!(cfg.scm_address, "scm.internal:9861");
        assert_eq!(cfg.heartbeat_interval, Duration::from_secs(45));
        assert!(cfg.tls.is_some());

        // Re-serialize and re-parse: the typed values must survive a full cycle.
        let serialized = toml::to_string(&cfg).expect("serialize datanode config");
        let reparsed = DatanodeConfig::from_toml_str(&serialized).expect("reparse");
        assert_eq!(reparsed.listen_addr, cfg.listen_addr);
        assert_eq!(reparsed.heartbeat_interval, cfg.heartbeat_interval);
    }

    #[test]
    fn empty_data_dirs_fails_validation() {
        let toml = r#"
            uuid_file = "/var/lib/ozone/dn.uuid"
            data_dirs = []
            metadata_dir = "/var/lib/ozone/dn-meta"
            listen_addr = "0.0.0.0:19864"
            scm_address = "scm.internal:9861"
            heartbeat_interval = 30
        "#;

        let err = DatanodeConfig::from_toml_str(toml).expect_err("empty data_dirs must fail");
        assert!(matches!(err, ConfigError::Invalid(_)));
    }

    #[test]
    fn blank_om_address_fails_validation() {
        let toml = r#"
            listen_addr = "0.0.0.0:9878"
            om_address = "   "
        "#;

        let err = GatewayConfig::from_toml_str(toml).expect_err("blank om_address must fail");
        assert!(matches!(err, ConfigError::Invalid(_)));
    }

    #[test]
    fn duration_round_trips_as_seconds() {
        // The on-wire form is a bare integer count of seconds, not a table.
        let cfg = DatanodeConfig {
            data_dirs: vec![PathBuf::from("/data/1")],
            scm_address: "scm:9861".into(),
            heartbeat_interval: Duration::from_secs(17),
            ..Default::default()
        };
        let serialized = toml::to_string(&cfg).expect("serialize");
        assert!(
            serialized.contains("heartbeat_interval = 17"),
            "expected integer-seconds encoding, got:\n{serialized}"
        );
        let reparsed = DatanodeConfig::from_toml_str(&serialized).expect("reparse");
        assert_eq!(reparsed.heartbeat_interval, Duration::from_secs(17));
    }

    #[test]
    fn gateway_defaults_apply_when_omitted() {
        // Only the two required-ish fields are present; everything else must
        // fall back to its documented default.
        let toml = r#"
            listen_addr = "0.0.0.0:9878"
            om_address = "om.internal:9862"
        "#;

        let cfg = GatewayConfig::from_toml_str(toml).expect("valid gateway config");
        assert_eq!(cfg.region, "us-east-1");
        assert_eq!(cfg.max_keys_default, 1000);
        assert_eq!(cfg.default_ec, EcReplicationConfig::RS_6_3_1MIB);
        assert!(cfg.tls.is_none());
    }

    #[test]
    fn default_ec_serializes_codec_as_rs() {
        let cfg = GatewayConfig {
            om_address: "om:9862".into(),
            ..Default::default()
        };
        let serialized = toml::to_string(&cfg).expect("serialize gateway config");
        // ozone-types encodes EcCodec with rename_all = "UPPERCASE".
        assert!(
            serialized.contains("codec = \"RS\""),
            "expected codec rendered as RS, got:\n{serialized}"
        );

        let reparsed = GatewayConfig::from_toml_str(&serialized).expect("reparse");
        assert_eq!(reparsed.default_ec, EcReplicationConfig::RS_6_3_1MIB);
    }
}
