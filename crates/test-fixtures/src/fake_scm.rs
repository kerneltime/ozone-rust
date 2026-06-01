//! In-memory fake Storage Container Manager (SCM) for testing the datanode's
//! registration + heartbeat loop without a real SCM.
//!
//! Implements [`ScmRustDatanodeService`]: a version handshake, registration, a
//! bidirectional heartbeat stream, and a bidirectional container-report stream.
//! The heartbeat handler counts inbound heartbeats (so a test can assert the
//! datanode is alive) and can deliver a one-shot batch of [`pb::ScmCommand`]s on
//! the first heartbeat response (so a test can drive command handling, e.g. a
//! `CloseContainer`).

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

use ozone_grpc_types::scm::dn::v1 as pb;
use ozone_grpc_types::scm::dn::v1::scm_rust_datanode_service_server::{
    ScmRustDatanodeService, ScmRustDatanodeServiceServer,
};

/// A fake SCM. Clone-free; transfer ownership to the server via
/// [`FakeScm::into_server`].
pub struct FakeScm {
    cluster_id: String,
    heartbeat_interval_sec: u32,
    heartbeats: Arc<AtomicU64>,
    /// Commands delivered once, on the first heartbeat response of each stream.
    commands: Vec<pb::ScmCommand>,
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
        }
    }

    /// A fake SCM that delivers `commands` on the first heartbeat response.
    pub fn with_commands(commands: Vec<pb::ScmCommand>) -> Self {
        Self {
            commands,
            ..Self::new()
        }
    }

    /// A shared counter of heartbeats this SCM has received. Clone it BEFORE
    /// [`FakeScm::into_server`] consumes `self`, then poll it from the test.
    pub fn heartbeat_counter(&self) -> Arc<AtomicU64> {
        self.heartbeats.clone()
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
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            let mut first = true;
            // Ends on Ok(None) (clean close) or Err (transport drop).
            while let Ok(Some(_hb)) = inbound.message().await {
                counter.fetch_add(1, Ordering::Relaxed);
                let cmds = if first {
                    first = false;
                    commands.clone()
                } else {
                    Vec::new()
                };
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
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            let mut seq = 0u64;
            while let Ok(Some(_report)) = inbound.message().await {
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
