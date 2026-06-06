//! `ozone-dn` datanode binary.
//!
//! Serves the `DatanodeGatewayService` over a fjall metadata store and a
//! filesystem chunk store, and runs the SCM registration + heartbeat loop in the
//! background.
//!
//! Configuration comes from a TOML file (`ozone-dn <config.toml>`) or, with no
//! argument, from environment variables:
//! - `OZONE_DN_DATA_DIR`  (default `/tmp/ozone-dn/data`)
//! - `OZONE_DN_META_DIR`  (default `/tmp/ozone-dn/meta`)
//! - `OZONE_DN_UUID_FILE` (default `/tmp/ozone-dn/uuid`)
//! - `OZONE_DN_LISTEN`    (default `0.0.0.0:19864`)
//! - `OZONE_DN_SCM`       (default `127.0.0.1:19863`)
//! - `RUST_LOG`           tracing filter

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use ozone_config::DatanodeConfig;
use ozone_dn_server::{DatanodeService, ScmRegistration};
use ozone_fjall_store::FjallMetaStore;
use ozone_grpc_types::scm::dn::v1 as scm;
use ozone_observability::{init_tracing, TracingOptions};
use ozone_storage::FileChunkStore;
use ozone_types::DatanodeUuid;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing(&TracingOptions::default())?;

    let cfg = load_config()?;
    cfg.validate()?;

    let uuid = load_or_create_uuid(&cfg.uuid_file)?;
    let meta = Arc::new(FjallMetaStore::open(&cfg.metadata_dir)?);
    let data_root = cfg
        .data_dirs
        .first()
        .expect("validated: data_dirs is non-empty");
    let chunks = Arc::new(FileChunkStore::new(data_root));
    let service = DatanodeService::new(meta.clone(), chunks.clone());

    // SCM registration + heartbeat loop in the background; if SCM is down the
    // loop logs and exits, but the datanode keeps serving the gateway.
    let reg = ScmRegistration {
        datanode_id: scm::DatanodeId {
            uuid: uuid.to_string(),
            ip_address: cfg.listen_addr.ip().to_string(),
            host_name: hostname(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            setup_time_ms: 0,
        },
        meta: meta.clone(),
        chunks: chunks.clone(),
        heartbeat_interval: cfg.heartbeat_interval,
    };
    let scm_endpoint = format!("http://{}", cfg.scm_address);
    tokio::spawn(async move {
        if let Err(e) = reg.run(scm_endpoint).await {
            tracing::error!("SCM registration loop ended: {e}");
        }
    });

    tracing::info!(listen = %cfg.listen_addr, uuid = %uuid, "ozone-dn datanode serving");
    Server::builder()
        .add_service(service.into_server())
        .serve(cfg.listen_addr)
        .await?;
    Ok(())
}

/// Load config from a TOML file path (`argv[1]`), or from the environment.
fn load_config() -> Result<DatanodeConfig, Box<dyn std::error::Error>> {
    if let Some(path) = std::env::args().nth(1) {
        return Ok(DatanodeConfig::from_toml_path(path)?);
    }
    let var = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    Ok(DatanodeConfig {
        uuid_file: var("OZONE_DN_UUID_FILE", "/tmp/ozone-dn/uuid").into(),
        data_dirs: vec![var("OZONE_DN_DATA_DIR", "/tmp/ozone-dn/data").into()],
        metadata_dir: var("OZONE_DN_META_DIR", "/tmp/ozone-dn/meta").into(),
        listen_addr: var("OZONE_DN_LISTEN", "0.0.0.0:19864").parse()?,
        scm_address: var("OZONE_DN_SCM", "127.0.0.1:19863"),
        heartbeat_interval: Duration::from_secs(30),
        tls: None,
    })
}

/// Read the datanode UUID from `path`, or generate and persist a fresh one.
fn load_or_create_uuid(path: &Path) -> Result<DatanodeUuid, Box<dyn std::error::Error>> {
    if let Ok(s) = std::fs::read_to_string(path) {
        if let Ok(u) = s.trim().parse::<DatanodeUuid>() {
            return Ok(u);
        }
    }
    let u = DatanodeUuid::new_random();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, u.to_string())?;
    Ok(u)
}

/// Best-effort hostname for the node report.
fn hostname() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| "datanode".to_string())
}
