//! Datanode SCM control plane: register, then run a heartbeat loop and execute
//! the commands SCM sends back.
//!
//! On [`ScmRegistration::run`] the datanode performs the version handshake,
//! registers (sending its `DatanodeID` + a node report), then opens a
//! bidirectional heartbeat stream: a ticker task pushes periodic heartbeats
//! while the main task consumes responses and dispatches their [`pb::ScmCommand`]s.
//! Container close/delete are applied to the metadata store; other commands are
//! acknowledged-by-ignoring for now (reconstruction is driven separately).

use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio_stream::wrappers::ReceiverStream;

use ozone_grpc_types::scm::dn::v1 as pb;
use ozone_scm_client::{ScmClient, ScmClientError};
use ozone_storage::MetaStore;
use ozone_types::{ContainerId, ContainerState};

/// Failure of the registration/heartbeat loop.
#[derive(Debug, Error)]
pub enum ScmLoopError {
    /// SCM connect or unary RPC failed.
    #[error(transparent)]
    Client(#[from] ScmClientError),
    /// The heartbeat response stream errored.
    #[error(transparent)]
    Stream(#[from] tonic::Status),
}

/// What the datanode registers to SCM with, plus the store commands act on.
pub struct ScmRegistration {
    /// This datanode's identity (uuid, addresses, version).
    pub datanode_id: pb::DatanodeId,
    /// Metadata store that container lifecycle commands are applied to.
    pub meta: Arc<dyn MetaStore>,
    /// Fallback heartbeat interval if SCM does not dictate one.
    pub heartbeat_interval: Duration,
}

impl ScmRegistration {
    /// Connect to SCM at `scm_endpoint`, register, and run the heartbeat loop
    /// until the stream closes or errors.
    pub async fn run(self, scm_endpoint: String) -> Result<(), ScmLoopError> {
        let mut client = ScmClient::connect(scm_endpoint).await?;

        let _version = client
            .get_version(pb::VersionRequest {
                client_major: 1,
                client_minor: 0,
            })
            .await?;

        let reg = client
            .register(pb::RegisterRequest {
                datanode_id: Some(self.datanode_id.clone()),
                node_report: Some(node_report()),
            })
            .await?;
        let uuid = if reg.assigned_uuid.is_empty() {
            self.datanode_id.uuid.clone()
        } else {
            reg.assigned_uuid.clone()
        };
        let interval = if reg.heartbeat_interval_sec > 0 {
            Duration::from_secs(reg.heartbeat_interval_sec as u64)
        } else {
            self.heartbeat_interval
        };
        tracing::info!(uuid = %uuid, "registered with SCM");

        // Bidirectional heartbeat: a ticker task emits heartbeats; this task
        // consumes responses and dispatches their commands.
        let (tx, rx) = tokio::sync::mpsc::channel::<pb::HeartbeatRequest>(8);
        let mut inbound = client.heartbeat(ReceiverStream::new(rx)).await?;

        let ticker_uuid = uuid.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let hb = pb::HeartbeatRequest {
                    datanode_uuid: ticker_uuid.clone(),
                    node_report: Some(node_report()),
                    command_status: Vec::new(),
                };
                if tx.send(hb).await.is_err() {
                    break; // response side closed
                }
            }
        });

        while let Some(resp) = inbound.message().await? {
            for cmd in resp.commands {
                self.handle_command(cmd).await;
            }
        }
        Ok(())
    }

    async fn handle_command(&self, cmd: pb::ScmCommand) {
        use pb::scm_command::Payload;
        match cmd.payload {
            Some(Payload::CloseContainer(c)) => {
                if let Some(cid) = c.container_id {
                    let id = ContainerId(cid.id);
                    match self.meta.set_container_state(id, ContainerState::Closed).await {
                        Ok(()) => tracing::info!(%id, "closed container per SCM command"),
                        Err(e) => tracing::warn!(%id, "close-container command failed: {e}"),
                    }
                }
            }
            Some(Payload::DeleteContainer(c)) => {
                if let Some(cid) = c.container_id {
                    let id = ContainerId(cid.id);
                    if let Err(e) = self.meta.delete_container(id).await {
                        tracing::warn!(%id, "delete-container command failed: {e}");
                    }
                }
            }
            _ => tracing::debug!(cmd_id = cmd.cmd_id, "ignoring unhandled SCM command"),
        }
    }
}

/// A minimal node report. A production datanode fills volume capacity from the
/// real filesystem; this reports a single nominal `IN_SERVICE` volume.
fn node_report() -> pb::NodeReport {
    pb::NodeReport {
        volumes: vec![pb::VolumeReport {
            volume_name: "data".to_string(),
            capacity_bytes: 0,
            used_bytes: 0,
            reserved_bytes: 0,
            storage_type: "DISK".to_string(),
        }],
        layout_version: 1,
        operational_state: "IN_SERVICE".to_string(),
    }
}
