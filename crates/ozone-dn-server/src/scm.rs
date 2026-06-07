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

use ozone_grpc_types::dn::v1 as dn;
use ozone_grpc_types::scm::dn::v1 as pb;
use ozone_scm_client::{ScmClient, ScmClientError};
use ozone_storage::{ChunkStore, MetaStore, StorageError};
use ozone_types::{ContainerId, ContainerState, EcReplicationConfig, LocalId};

use crate::repair;

/// Failure of the registration/heartbeat loop.
#[derive(Debug, Error)]
pub enum ScmLoopError {
    /// SCM connect or unary RPC failed.
    #[error(transparent)]
    Client(#[from] ScmClientError),
    /// The heartbeat response stream errored.
    #[error(transparent)]
    Stream(#[from] tonic::Status),
    /// A local store read failed while building a report.
    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// What the datanode registers to SCM with, plus the stores commands act on.
pub struct ScmRegistration {
    /// This datanode's identity (uuid, addresses, version).
    pub datanode_id: pb::DatanodeId,
    /// Metadata store that container lifecycle commands + reports read/write.
    pub meta: Arc<dyn MetaStore>,
    /// Chunk store, so a DeleteContainer command also reclaims chunk bytes.
    pub chunks: Arc<dyn ChunkStore>,
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

        // Tell SCM which containers this datanode holds (a FULL report). Without
        // it a real SCM never learns this DN's replicas. Best-effort: a report
        // failure must not abort the heartbeat loop.
        if let Err(e) = self.send_full_container_report(&mut client, &uuid).await {
            tracing::warn!("initial container report failed: {e}");
        }

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
                self.handle_command(cmd, &uuid).await;
            }
        }
        Ok(())
    }

    async fn handle_command(&self, cmd: pb::ScmCommand, self_uuid: &str) {
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
                    // Metadata first, then chunk bytes (same order as the
                    // gateway-facing DeleteContainer): a crash between the two
                    // leaves reclaimable orphan files, not dangling metadata.
                    // Deleting only metadata here would leak the chunk bytes.
                    if let Err(e) = self.meta.delete_container(id).await {
                        tracing::warn!(%id, "delete-container (metadata) failed: {e}");
                    }
                    if let Err(e) = self.chunks.delete_container(id).await {
                        tracing::warn!(%id, "delete-container (chunks) failed: {e}");
                    }
                }
            }
            Some(Payload::ReconstructEcContainers(c)) => {
                self.handle_reconstruct(c, self_uuid).await
            }
            _ => tracing::debug!(cmd_id = cmd.cmd_id, "ignoring unhandled SCM command"),
        }
    }

    /// Execute an EC reconstruction command: for each named block group, rebuild
    /// the missing shard(s) from the surviving peers and persist them locally.
    /// Acts only if this datanode is one of the command's targets.
    async fn handle_reconstruct(&self, cmd: pb::ReconstructEcContainersCommand, self_uuid: &str) {
        if !cmd.targets.iter().any(|t| t.uuid == self_uuid) {
            return; // not addressed to this datanode
        }
        let Some(cid) = cmd.container_id.as_ref() else {
            return;
        };
        let container = ContainerId(cid.id);
        let ec: EcReplicationConfig = match cmd.ec_config.and_then(|c| c.try_into().ok()) {
            Some(ec) => ec,
            None => {
                tracing::warn!(%container, "reconstruct command has no/invalid ec_config");
                return;
            }
        };
        let missing_slots: Vec<u8> = cmd.missing_indexes.iter().map(|i| *i as u8).collect();
        // Join each source peer (uuid -> ip+port) with the slot it holds, dropping
        // any peer whose slot we are rebuilding.
        let sources: Vec<(u8, String)> = cmd
            .sources
            .iter()
            .filter_map(|s| {
                let slot = *cmd.source_replica_indexes.get(&s.uuid)? as u8;
                if missing_slots.contains(&slot) {
                    return None;
                }
                Some((slot, format!("http://{}:{}", s.ip_address, s.gateway_port)))
            })
            .collect();

        for block in &cmd.blocks {
            let input = repair::RepairInput {
                container,
                local: LocalId(block.local_id),
                ec,
                block_group_len: block.block_group_len,
                missing_slots: missing_slots.clone(),
                sources: sources.clone(),
            };
            match repair::reconstruct_and_persist(&self.meta, &self.chunks, input).await {
                Ok(slots) => {
                    tracing::info!(%container, local = block.local_id, ?slots, "repaired EC shards")
                }
                Err(e) => {
                    tracing::warn!(%container, local = block.local_id, "EC repair failed: {e}")
                }
            }
        }
    }

    /// Send a single FULL [`pb::ContainerReportRequest`] listing every container
    /// this datanode holds, and drain SCM's acks. `bcsi_id`/`replica_index` are
    /// reported as 0 (this slice does not track per-container BCSI or a single
    /// container-level replica index).
    async fn send_full_container_report(
        &self,
        client: &mut ScmClient,
        uuid: &str,
    ) -> Result<(), ScmLoopError> {
        let reports: Vec<pb::ContainerReport> = self
            .meta
            .list_containers()
            .await?
            .into_iter()
            .map(|c| pb::ContainerReport {
                container_id: Some(dn::ContainerId { id: c.container_id.0 }),
                state: dn::container_state::State::from(c.state) as i32,
                used_bytes: c.used_bytes,
                block_count: c.block_count,
                bcsi_id: 0,
                replica_index: 0,
            })
            .collect();
        let req = pb::ContainerReportRequest {
            datanode_uuid: uuid.to_string(),
            kind: pb::container_report_request::Kind::Full as i32,
            reports,
        };
        let mut acks = client.container_report(tokio_stream::once(req)).await?;
        while acks.message().await?.is_some() {}
        Ok(())
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
