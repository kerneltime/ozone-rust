//! Datanode SCM control plane, COMPLIANT with the real Apache Ozone
//! `StorageContainerDatanodeProtocol`.
//!
//! The datanode polls SCM via the unary `submitRequest` (no streaming): version
//! handshake -> register (carrying the four reports inline + a `REPLICATION` named
//! port + real volume capacity, keeping its OWN uuid) -> a heartbeat loop that
//! drains the scrubber's UNHEALTHY findings into the heartbeat's incremental
//! container report and dispatches the commands SCM returns. SCM's command queue
//! is drained remove-on-read, so every command in a response is executed before
//! the next heartbeat (a panic in a handler is caught so it cannot stall the loop).
//!
//! This coexists with the bespoke [`crate::scm`] loop during the Track-2 migration;
//! the bespoke one is retired in B7. For B2 the reconstruction handler parses the
//! REAL command shape but reuses the existing in-place repair primitive; the
//! survivor-enumeration + `min(blockGroupLen)` rewrite is B4.

use std::collections::HashSet;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use parking_lot::Mutex;
use thiserror::Error;

use ozone_grpc_types::hadoop::hdds as oz;
use ozone_scm_client::compliant::{OzoneScmClient, ScmError};
use ozone_storage::{ChunkStore, MetaStore, StorageError};
use ozone_types::{ContainerId, ContainerState, EcCodec, EcReplicationConfig};

use crate::repair;
use crate::scrub::RepairRequest;

/// The named port under which the datanode advertises its data-plane endpoint, so
/// a reconstructing peer (or the gateway) can dial it. Real Ozone uses this name
/// for the container-replication endpoint.
const REPLICATION_PORT_NAME: &str = "REPLICATION";

/// A nominal reported volume capacity. A production datanode would `statvfs` its
/// data volume; SCM only needs a non-zero capacity/remaining to place
/// reconstruction targets (a zero capacity makes it skip this node). [I] refine to
/// real filesystem stats.
const NOMINAL_CAPACITY: u64 = 1 << 40; // 1 TiB

/// Failure of the compliant registration/heartbeat loop.
#[derive(Debug, Error)]
pub enum CompliantScmError {
    /// SCM transport / RPC error.
    #[error(transparent)]
    Client(#[from] ScmError),
    /// A local store read failed while building a report.
    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// What the datanode registers with, plus the stores commands act on. Speaks the
/// real protocol; the data-plane port is advertised as a `REPLICATION` named port.
pub struct CompliantScmRegistration {
    /// This datanode's own uuid (kept across registration — SCM only echoes it).
    pub uuid: String,
    /// This datanode's IP, advertised in `DatanodeDetailsProto`.
    pub ip_address: String,
    /// This datanode's hostname.
    pub host_name: String,
    /// The data-plane gRPC/HTTP port, advertised as the `REPLICATION` port.
    pub data_port: u32,
    /// Metadata store container lifecycle commands + reports read/write.
    pub meta: Arc<dyn MetaStore>,
    /// Chunk store, so a DeleteContainer command also reclaims chunk bytes.
    pub chunks: Arc<dyn ChunkStore>,
    /// Heartbeat interval.
    pub heartbeat_interval: Duration,
    /// Bit-rot findings from the local scrubber; reported UNHEALTHY (incrementally,
    /// on the heartbeat) so SCM mints a ReconstructEC. `None` = no scrubber wired.
    pub repairs: Option<tokio::sync::mpsc::Receiver<RepairRequest>>,
}

impl CompliantScmRegistration {
    /// Connect, register, and run the heartbeat poll loop forever (until aborted).
    pub async fn run(mut self, scm_endpoint: String) -> Result<(), CompliantScmError> {
        let mut client = OzoneScmClient::connect(scm_endpoint).await?;
        let _version = client.get_version().await?;

        let reg = client.register(self.build_register().await?).await?;
        // Keep our OWN uuid; the response merely echoes it. A real SCM does NOT
        // rename the datanode.
        tracing::info!(uuid = %self.uuid, cluster = %reg.cluster_id, "registered with real SCM");

        // Scrubber findings become pending INCREMENTAL container replicas, folded
        // into the next heartbeat. A rising-edge latch reports each (container,slot)
        // once until it heals, so re-emitted findings don't storm SCM.
        let pending: Arc<Mutex<Vec<oz::ContainerReplicaProto>>> = Arc::new(Mutex::new(Vec::new()));
        if let Some(mut repairs) = self.repairs.take() {
            let pending = pending.clone();
            let uuid = self.uuid.clone();
            tokio::spawn(async move {
                let mut reported: HashSet<(ContainerId, u8)> = HashSet::new();
                while let Some(req) = repairs.recv().await {
                    if reported.insert((req.container, req.slot)) {
                        pending.lock().push(unhealthy_replica(&uuid, req.container, req.slot));
                    }
                }
            });
        }

        let mut ticker = tokio::time::interval(self.heartbeat_interval);
        loop {
            ticker.tick().await;
            let incremental: Vec<oz::ContainerReplicaProto> = std::mem::take(&mut pending.lock());
            let hb = oz::ScmHeartbeatRequestProto {
                datanode_details: self.datanode_details(),
                node_report: Some(self.node_report()),
                incremental_container_report: if incremental.is_empty() {
                    Vec::new()
                } else {
                    vec![oz::IncrementalContainerReportProto { report: incremental }]
                },
                ..Default::default()
            };
            let resp = match client.send_heartbeat(hb).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("heartbeat failed (retry next tick): {e}");
                    continue;
                }
            };
            // Remove-on-read: dispatch every command before the next heartbeat. A
            // handler panic is caught so it cannot kill the loop (control-plane DoS).
            for cmd in resp.commands {
                let guarded = AssertUnwindSafe(self.handle_command(cmd, &pending));
                if guarded.catch_unwind().await.is_err() {
                    tracing::error!("SCM command handler panicked; heartbeat loop continues");
                }
            }
        }
    }

    fn datanode_details(&self) -> oz::DatanodeDetailsProto {
        oz::DatanodeDetailsProto {
            uuid: Some(self.uuid.clone()),
            ip_address: self.ip_address.clone(),
            host_name: self.host_name.clone(),
            ports: vec![oz::Port {
                name: REPLICATION_PORT_NAME.to_string(),
                value: self.data_port,
            }],
            ..Default::default()
        }
    }

    fn node_report(&self) -> oz::NodeReportProto {
        oz::NodeReportProto {
            storage_report: vec![oz::StorageReportProto {
                storage_uuid: format!("{}-vol0", self.uuid),
                storage_location: "/data".to_string(),
                capacity: Some(NOMINAL_CAPACITY),
                scm_used: Some(0),
                remaining: Some(NOMINAL_CAPACITY),
                storage_type: Some(oz::StorageTypeProto::Disk as i32),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    async fn build_register(&self) -> Result<oz::ScmRegisterRequestProto, CompliantScmError> {
        Ok(oz::ScmRegisterRequestProto {
            extended_datanode_details: oz::ExtendedDatanodeDetailsProto {
                datanode_details: self.datanode_details(),
                version: Some(env!("CARGO_PKG_VERSION").to_string()),
                setup_time: Some(0),
                ..Default::default()
            },
            node_report: self.node_report(),
            container_report: oz::ContainerReportsProto {
                reports: self.container_replicas().await?,
            },
            // A data-only EC datanode hosts no Ratis pipelines; the wrapping message
            // is required but its list may be empty.
            pipeline_reports: oz::PipelineReportsProto::default(),
            data_node_layout_version: None,
        })
    }

    /// The container replicas this DN holds, with each replica's EC slot. The slot
    /// is derived from the blocks (every block of a container on this DN carries
    /// this DN's replica index). Empty EC containers (no blocks, hence no known
    /// slot) are skipped — [I] a real DN persists the slot in container metadata.
    async fn container_replicas(&self) -> Result<Vec<oz::ContainerReplicaProto>, CompliantScmError> {
        let mut out = Vec::new();
        for c in self.meta.list_containers().await? {
            let page = self.meta.list_blocks(c.container_id, 0, 1).await?;
            let Some(first) = page.blocks.first() else {
                continue;
            };
            let slot = first.block_id.replica_index.get();
            out.push(oz::ContainerReplicaProto {
                container_id: c.container_id.0 as i64,
                state: container_state_to_real(c.state) as i32,
                replica_index: Some(slot as i32),
                origin_node_id: Some(self.uuid.clone()),
                key_count: Some(c.block_count as i64),
                used: Some(c.used_bytes as i64),
                ..Default::default()
            });
        }
        Ok(out)
    }

    async fn handle_command(
        &self,
        cmd: oz::ScmCommandProto,
        pending: &Mutex<Vec<oz::ContainerReplicaProto>>,
    ) {
        use oz::scm_command_proto::Type;
        match Type::try_from(cmd.command_type) {
            Ok(Type::CloseContainerCommand) => {
                if let Some(c) = cmd.close_container_command_proto {
                    let id = ContainerId(c.container_id as u64);
                    match self.meta.set_container_state(id, ContainerState::Closed).await {
                        Ok(()) => {
                            tracing::info!(%id, "closed container per SCM command");
                            // Announce the new CLOSED state so SCM converges its replica
                            // view (real Ozone's sendICR-on-close).
                            self.report_state(pending, id).await;
                        }
                        Err(e) => tracing::warn!(%id, "close-container failed: {e}"),
                    }
                }
            }
            Ok(Type::DeleteContainerCommand) => {
                if let Some(c) = cmd.delete_container_command_proto {
                    let id = ContainerId(c.container_id as u64);
                    // This DN holds exactly one EC replica (slot) of a container, so
                    // deleting the local container IS deleting the replica the
                    // command's `replicaIndex` names. Metadata first, then bytes.
                    if let Err(e) = self.meta.delete_container(id).await {
                        tracing::warn!(%id, "delete-container (metadata) failed: {e}");
                    }
                    if let Err(e) = self.chunks.delete_container(id).await {
                        tracing::warn!(%id, "delete-container (chunks) failed: {e}");
                    }
                }
            }
            Ok(Type::ReconstructEcContainersCommand) => {
                if let Some(c) = cmd.reconstruct_ec_containers_command_proto {
                    self.handle_reconstruct(c, pending).await;
                }
            }
            _ => tracing::debug!(cmd = cmd.command_type, "ignoring unhandled SCM command"),
        }
    }

    /// Execute a real-shape ReconstructEC command for the slots assigned to THIS
    /// datanode. Compliant algorithm: enumerate block groups from the SURVIVORS and
    /// derive each length as `min(blockGroupLen)` (see
    /// [`repair::reconstruct_from_survivors`]). `targets[i]` is paired with
    /// `missingContainerIndexes[i]`; `sources` carry their slot inline + a
    /// REPLICATION port.
    async fn handle_reconstruct(
        &self,
        cmd: oz::ReconstructEcContainersCommandProto,
        pending: &Mutex<Vec<oz::ContainerReplicaProto>>,
    ) {
        let my_slots: Vec<u8> = cmd
            .targets
            .iter()
            .zip(cmd.missing_container_indexes.iter())
            .filter(|(t, _)| t.uuid.as_deref() == Some(self.uuid.as_str()))
            .map(|(_, &idx)| idx)
            .collect();
        if my_slots.is_empty() {
            return; // not a target
        }
        let container = ContainerId(cmd.container_id as u64);
        let ec = match to_domain_ec(&cmd.ec_replication_config) {
            Ok(ec) => ec,
            Err(()) => {
                tracing::warn!(%container, "reconstruct: bad ec_replication_config");
                return;
            }
        };
        let sources: Vec<(u8, String)> = cmd
            .sources
            .iter()
            .filter_map(|s| {
                let slot = s.replica_index as u8;
                if my_slots.contains(&slot) {
                    return None;
                }
                let dd = &s.datanode_details;
                let port = dd
                    .ports
                    .iter()
                    .find(|p| p.name == REPLICATION_PORT_NAME)
                    .map(|p| p.value)?;
                Some((slot, format!("http://{}:{}", dd.ip_address, port)))
            })
            .collect();

        let input = repair::ReconstructInput {
            container,
            ec,
            missing_slots: my_slots,
            sources,
        };
        match repair::reconstruct_from_survivors(&self.meta, &self.chunks, input).await {
            Ok(locals) => {
                tracing::info!(%container, groups = locals.len(), "reconstructed EC block groups from survivors");
                // Announce the rebuilt replica's new state (CLOSED for a fresh whole
                // replica) so SCM's view converges and any prior UNHEALTHY entry is
                // overwritten. Only when something was actually rebuilt -- a no-op or a
                // rolled-back unrecoverable rebuild reports nothing.
                if !locals.is_empty() {
                    self.report_state(pending, container).await;
                }
            }
            Err(e) => tracing::warn!(%container, "EC reconstruction failed: {e}"),
        }
    }

    /// Push an INCREMENTAL container replica announcing `container`'s CURRENT state to
    /// SCM (the convergence signal after a local state change). SCM keys replicas by
    /// (containerID, datanodeID) ignoring state, so this OVERWRITES any prior entry --
    /// e.g. reporting a rebuilt replica CLOSED clears the UNHEALTHY one SCM held. The
    /// EC slot is the replica index of the container's first block (a DN holds one EC
    /// slot per container); a container with no blocks (unknown slot) is skipped.
    async fn report_state(
        &self,
        pending: &Mutex<Vec<oz::ContainerReplicaProto>>,
        container: ContainerId,
    ) {
        let Ok(Some(ci)) = self.meta.get_container(container).await else {
            return;
        };
        let Ok(page) = self.meta.list_blocks(container, 0, 1).await else {
            return;
        };
        let Some(first) = page.blocks.first() else {
            return;
        };
        let slot = first.block_id.replica_index.get();
        pending.lock().push(oz::ContainerReplicaProto {
            container_id: container.0 as i64,
            state: container_state_to_real(ci.state) as i32,
            replica_index: Some(slot as i32),
            origin_node_id: Some(self.uuid.clone()),
            key_count: Some(ci.block_count as i64),
            used: Some(ci.used_bytes as i64),
            ..Default::default()
        });
    }
}

/// Build an INCREMENTAL UNHEALTHY container replica for the self-heal signal. SCM
/// turns this into a ReconstructEC; `replicaIndex = slot` tells it which EC slot.
fn unhealthy_replica(uuid: &str, container: ContainerId, slot: u8) -> oz::ContainerReplicaProto {
    oz::ContainerReplicaProto {
        container_id: container.0 as i64,
        state: oz::container_replica_proto::State::Unhealthy as i32,
        replica_index: Some(slot as i32),
        origin_node_id: Some(uuid.to_string()),
        ..Default::default()
    }
}

/// Map the domain container state to the real `ContainerReplicaProto.State`.
fn container_state_to_real(state: ContainerState) -> oz::container_replica_proto::State {
    use oz::container_replica_proto::State as R;
    match state {
        ContainerState::Open => R::Open,
        ContainerState::Closing => R::Closing,
        ContainerState::QuasiClosed => R::QuasiClosed,
        ContainerState::Closed => R::Closed,
        ContainerState::Unhealthy => R::Unhealthy,
        ContainerState::Deleted => R::Deleted,
    }
}

/// Convert the real `ECReplicationConfig` to the domain config.
fn to_domain_ec(w: &oz::EcReplicationConfig) -> Result<EcReplicationConfig, ()> {
    let codec: EcCodec = w.codec.parse().map_err(|_| ())?;
    EcReplicationConfig::new(w.data as u8, w.parity as u8, w.ec_chunk_size as u32, codec)
        .map_err(|_| ())
}
