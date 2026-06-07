//! In-memory fake Storage Container Manager (SCM) for testing the datanode's
//! registration + heartbeat loop without a real SCM.
//!
//! Implements [`ScmRustDatanodeService`]: a version handshake, registration, a
//! bidirectional heartbeat stream, and a bidirectional container-report stream.
//! The heartbeat handler counts inbound heartbeats (so a test can assert the
//! datanode is alive) and can deliver a one-shot batch of [`pb::ScmCommand`]s on
//! the first heartbeat response (so a test can drive command handling, e.g. a
//! `CloseContainer`).

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

use ozone_grpc_types::dn::v1 as dn;
use ozone_grpc_types::scm::dn::v1 as pb;
use ozone_grpc_types::scm::dn::v1::scm_rust_datanode_service_server::{
    ScmRustDatanodeService, ScmRustDatanodeServiceServer,
};

/// Monotonic command ids for synthesized commands (unique is all that matters).
static NEXT_CMD_ID: AtomicU64 = AtomicU64::new(1);

/// Whether `cmd` should be delivered to the datanode heartbeating as `uuid`. A
/// real SCM routes each command to its target DN's heartbeat; a `ReconstructEC`
/// names explicit `targets`, so it goes ONLY to a matching DN (otherwise a peer's
/// heartbeat would consume — and then ignore — another DN's command, losing it).
/// Commands without datanode targets deliver to any heartbeat.
fn command_targets(cmd: &pb::ScmCommand, uuid: &str) -> bool {
    match &cmd.payload {
        Some(pb::scm_command::Payload::ReconstructEcContainers(c)) => {
            c.targets.iter().any(|t| t.uuid == uuid)
        }
        _ => true,
    }
}

/// The survivor pipeline + EC config a ReconstructEC needs — what only the
/// cluster (OM/SCM) knows. A test configures this once via [`FakeScm::with_pipeline`]
/// and the fake uses it to turn an inbound UNHEALTHY report into a real command.
#[derive(Clone)]
pub struct PipelineFixture {
    /// Survivor datanodes holding shards (each carries uuid + ip + gateway_port).
    pub sources: Vec<pb::DatanodeId>,
    /// uuid -> EC slot (1..=k+p) it holds.
    pub source_replica_indexes: HashMap<String, u32>,
    /// EC config of the container (dn-package wire form; from `ec().into()`).
    pub ec_config: dn::EcReplicationConfig,
}

/// A fake SCM. Clone-free; transfer ownership to the server via
/// [`FakeScm::into_server`].
pub struct FakeScm {
    cluster_id: String,
    heartbeat_interval_sec: u32,
    heartbeats: Arc<AtomicU64>,
    /// Commands delivered once, on the first heartbeat response of each stream.
    commands: Vec<pb::ScmCommand>,
    /// Every container-report request this SCM has received (for assertions).
    reports: Arc<Mutex<Vec<pb::ContainerReportRequest>>>,
    /// If set, an inbound UNHEALTHY container report triggers a synthesized
    /// ReconstructEC built from this pipeline, queued for the next heartbeat.
    pipeline: Option<PipelineFixture>,
    /// Commands synthesized from inbound reports, drained onto heartbeat responses.
    pending: Arc<Mutex<VecDeque<pb::ScmCommand>>>,
    /// `(container_id, replica_index)` already turned into a command, so a
    /// duplicate report does not issue a second reconstruct.
    issued: Arc<Mutex<std::collections::HashSet<(u64, u32)>>>,
}

impl Default for FakeScm {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeScm {
    /// A fake SCM that issues no commands.
    pub fn new() -> Self {
        Self {
            cluster_id: "test-cluster".to_string(),
            heartbeat_interval_sec: 1,
            heartbeats: Arc::new(AtomicU64::new(0)),
            commands: Vec::new(),
            reports: Arc::new(Mutex::new(Vec::new())),
            pipeline: None,
            pending: Arc::new(Mutex::new(VecDeque::new())),
            issued: Arc::new(Mutex::new(std::collections::HashSet::new())),
        }
    }

    /// A fake SCM that delivers `commands` on the first heartbeat response.
    pub fn with_commands(commands: Vec<pb::ScmCommand>) -> Self {
        Self {
            commands,
            ..Self::new()
        }
    }

    /// A fake SCM that reacts to an UNHEALTHY container report by issuing a
    /// ReconstructEC for that container+slot (built from `pipeline`), delivered on
    /// the next heartbeat — the SCM side of the self-heal loop.
    pub fn with_pipeline(pipeline: PipelineFixture) -> Self {
        Self {
            pipeline: Some(pipeline),
            ..Self::new()
        }
    }

    /// A shared counter of heartbeats this SCM has received. Clone it BEFORE
    /// [`FakeScm::into_server`] consumes `self`, then poll it from the test.
    pub fn heartbeat_counter(&self) -> Arc<AtomicU64> {
        self.heartbeats.clone()
    }

    /// A shared handle to the container-report requests this SCM has received.
    /// Clone it BEFORE [`FakeScm::into_server`] consumes `self`.
    pub fn received_reports(&self) -> Arc<Mutex<Vec<pb::ContainerReportRequest>>> {
        self.reports.clone()
    }

    /// Wrap in the tonic server for `Server::add_service`.
    pub fn into_server(self) -> ScmRustDatanodeServiceServer<Self> {
        ScmRustDatanodeServiceServer::new(self)
    }
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl ScmRustDatanodeService for FakeScm {
    async fn get_version(
        &self,
        _req: Request<pb::VersionRequest>,
    ) -> Result<Response<pb::VersionResponse>, Status> {
        Ok(Response::new(pb::VersionResponse {
            server_major: 1,
            server_minor: 0,
            cluster_id: self.cluster_id.clone(),
        }))
    }

    async fn register(
        &self,
        req: Request<pb::RegisterRequest>,
    ) -> Result<Response<pb::RegisterResponse>, Status> {
        // Echo the datanode's own UUID as the assigned UUID (a real SCM may
        // rewrite it on first registration; the fake accepts it as-is).
        let assigned_uuid = req
            .into_inner()
            .datanode_id
            .map(|d| d.uuid)
            .unwrap_or_default();
        Ok(Response::new(pb::RegisterResponse {
            assigned_uuid,
            heartbeat_interval_sec: self.heartbeat_interval_sec,
        }))
    }

    type HeartbeatStream = BoxStream<pb::HeartbeatResponse>;

    async fn heartbeat(
        &self,
        req: Request<Streaming<pb::HeartbeatRequest>>,
    ) -> Result<Response<Self::HeartbeatStream>, Status> {
        let mut inbound = req.into_inner();
        let counter = self.heartbeats.clone();
        let commands = self.commands.clone();
        let pending = self.pending.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            let mut first = true;
            // Ends on Ok(None) (clean close) or Err (transport drop).
            while let Ok(Some(hb)) = inbound.message().await {
                counter.fetch_add(1, Ordering::Relaxed);
                let mut cmds = if first {
                    first = false;
                    commands.clone()
                } else {
                    Vec::new()
                };
                // Deliver synthesized commands (self-heal) only to their target DN,
                // leaving others queued for the DN they name — so two datanodes
                // heartbeating one SCM don't consume each other's commands.
                {
                    let mut q = pending.lock();
                    let mut keep = VecDeque::with_capacity(q.len());
                    while let Some(c) = q.pop_front() {
                        if command_targets(&c, &hb.datanode_uuid) {
                            cmds.push(c);
                        } else {
                            keep.push_back(c);
                        }
                    }
                    *q = keep;
                }
                if tx
                    .send(Ok(pb::HeartbeatResponse { commands: cmds }))
                    .await
                    .is_err()
                {
                    break; // datanode closed the stream
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    type ContainerReportStream = BoxStream<pb::ContainerReportAck>;

    async fn container_report(
        &self,
        req: Request<Streaming<pb::ContainerReportRequest>>,
    ) -> Result<Response<Self::ContainerReportStream>, Status> {
        let mut inbound = req.into_inner();
        let reports = self.reports.clone();
        let pipeline = self.pipeline.clone();
        let pending = self.pending.clone();
        let issued = self.issued.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            let mut seq = 0u64;
            while let Ok(Some(report)) = inbound.message().await {
                // Self-heal: an UNHEALTHY report becomes a ReconstructEC built from
                // the configured pipeline, queued for the next heartbeat. Deduped
                // by (container, slot) so a duplicate report issues no second command.
                if let Some(pipe) = &pipeline {
                    for cr in &report.reports {
                        if cr.state != dn::container_state::State::Unhealthy as i32 {
                            continue;
                        }
                        let Some(cid) = cr.container_id else {
                            continue;
                        };
                        if !issued.lock().insert((cid.id, cr.replica_index)) {
                            continue;
                        }
                        pending.lock().push_back(pb::ScmCommand {
                            cmd_id: NEXT_CMD_ID.fetch_add(1, Ordering::Relaxed),
                            term: 0,
                            encoded_token: Vec::new(),
                            deadline_ms: 0,
                            payload: Some(pb::scm_command::Payload::ReconstructEcContainers(
                                pb::ReconstructEcContainersCommand {
                                    container_id: Some(cid),
                                    sources: pipe.sources.clone(),
                                    targets: vec![pb::DatanodeId {
                                        uuid: report.datanode_uuid.clone(),
                                        ..Default::default()
                                    }],
                                    missing_indexes: vec![cr.replica_index],
                                    ec_config: Some(pipe.ec_config.clone()),
                                    source_replica_indexes: pipe.source_replica_indexes.clone(),
                                    blocks: Vec::new(),
                                },
                            )),
                        });
                    }
                }
                reports.lock().push(report);
                seq += 1;
                if tx
                    .send(Ok(pb::ContainerReportAck { acked_seq: seq }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}
