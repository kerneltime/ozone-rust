//! A fake SCM speaking the REAL Apache Ozone `StorageContainerDatanodeProtocol`
//! (vendored `ozone_grpc_types::hadoop::hdds`), used as the COMPLIANCE harness for
//! the Rust datanode.
//!
//! Unlike [`crate::fake_scm`] (the bespoke Rust-native protocol), this validates
//! the datanode against the ACTUAL Ozone wire contract:
//! - ONE unary `submitRequest`, multiplexed by a `Type` discriminator (no streams);
//! - `Register` carries all four reports inline; the response echoes the DN's own
//!   uuid + the cluster id (no rename);
//! - commands ride the heartbeat RESPONSE and are drained remove-on-read, exactly
//!   once, mirroring SCM's per-DN `CommandQueue`.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tonic::{Request, Response, Status};

use ozone_grpc_types::hadoop::hdds as oz;
use oz::storage_container_datanode_protocol_service_server::{
    StorageContainerDatanodeProtocolService, StorageContainerDatanodeProtocolServiceServer,
};

/// Monotonic command ids for synthesized commands (uniqueness is all that matters).
static NEXT_CMD_ID: AtomicU64 = AtomicU64::new(1);

/// The survivor pipeline + EC config a ReconstructEC needs — what only SCM knows.
/// A test configures it once via [`CompliantScm::with_pipeline`]; the fake turns an
/// inbound UNHEALTHY incremental container report into a real ReconstructEC targeting
/// the reporting DN — the SCM side of the compliant self-heal loop.
#[derive(Clone)]
pub struct CompliantPipelineFixture {
    /// Survivor datanodes, each carrying its EC slot inline (`replica_index`) and a
    /// REPLICATION port (so the reconstructing target can dial it).
    pub sources: Vec<oz::DatanodeDetailsAndReplicaIndexProto>,
    /// EC config of the container (real wire form).
    pub ec_config: oz::EcReplicationConfig,
}

/// Synthesize a ReconstructEC for `(container, slot)` targeting `target_uuid`, built
/// from the pipeline. `missing_container_indexes` is the byte-per-slot wire form;
/// `targets[0]` pairs positionally with it. The target carries only its uuid (the
/// handler matches on uuid and writes locally; it never dials the target).
fn synth_reconstruct(
    container_id: i64,
    slot: i32,
    target_uuid: &str,
    pipe: &CompliantPipelineFixture,
) -> oz::ScmCommandProto {
    oz::ScmCommandProto {
        command_type: oz::scm_command_proto::Type::ReconstructEcContainersCommand as i32,
        reconstruct_ec_containers_command_proto: Some(oz::ReconstructEcContainersCommandProto {
            container_id,
            sources: pipe.sources.clone(),
            targets: vec![oz::DatanodeDetailsProto {
                uuid: Some(target_uuid.to_string()),
                ..Default::default()
            }],
            missing_container_indexes: vec![slot as u8],
            ec_replication_config: pipe.ec_config.clone(),
            cmd_id: NEXT_CMD_ID.fetch_add(1, Ordering::Relaxed) as i64,
        }),
        ..Default::default()
    }
}

/// Whether `cmd` should be delivered to the DN heartbeating as `uuid`. A ReconstructEC
/// names explicit targets, so it goes ONLY to a matching DN (otherwise a peer's
/// heartbeat would consume — and drop — another DN's command). Other commands deliver
/// to any heartbeat.
fn command_targets(cmd: &oz::ScmCommandProto, uuid: &str) -> bool {
    if cmd.command_type == oz::scm_command_proto::Type::ReconstructEcContainersCommand as i32 {
        if let Some(c) = &cmd.reconstruct_ec_containers_command_proto {
            return c.targets.iter().any(|t| t.uuid.as_deref() == Some(uuid));
        }
    }
    true
}

/// Inspectable record of exactly what the datanode sent on the wire.
#[derive(Default)]
pub struct ScmRecord {
    /// Every register request received (decoded real proto).
    pub registers: Vec<oz::ScmRegisterRequestProto>,
    /// Every heartbeat request received (decoded real proto).
    pub heartbeats: Vec<oz::ScmHeartbeatRequestProto>,
}

/// A compliant fake SCM. Transfer ownership to a tonic server via
/// [`CompliantScm::into_server`]; clone [`CompliantScm::record`] /
/// [`CompliantScm::pending`] BEFORE that to retain inspection/command handles.
pub struct CompliantScm {
    cluster_id: String,
    record: Arc<Mutex<ScmRecord>>,
    /// Commands queued for delivery; drained remove-on-read on each heartbeat,
    /// exactly like the real per-DN command queue.
    pending: Arc<Mutex<VecDeque<oz::ScmCommandProto>>>,
    /// If set, an inbound UNHEALTHY incremental report synthesizes a ReconstructEC
    /// (built from this pipeline) targeting the reporting DN, queued for delivery.
    pipeline: Option<CompliantPipelineFixture>,
    /// `(container_id, replica_index)` already turned into a command, so a duplicate
    /// UNHEALTHY report does not issue a second reconstruct.
    issued: Arc<Mutex<HashSet<(i64, i32)>>>,
}

impl Default for CompliantScm {
    fn default() -> Self {
        Self::new()
    }
}

impl CompliantScm {
    /// A compliant SCM that issues no commands.
    pub fn new() -> Self {
        Self {
            cluster_id: "test-cluster".to_string(),
            record: Arc::new(Mutex::new(ScmRecord::default())),
            pending: Arc::new(Mutex::new(VecDeque::new())),
            pipeline: None,
            issued: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// A compliant SCM pre-loaded with `commands`, delivered (drained) on the
    /// datanode's heartbeats — all currently-pending on each heartbeat, once.
    pub fn with_commands(commands: Vec<oz::ScmCommandProto>) -> Self {
        let s = Self::new();
        *s.pending.lock() = commands.into();
        s
    }

    /// A compliant SCM that reacts to an UNHEALTHY incremental report by issuing a
    /// ReconstructEC for that container+slot (built from `pipeline`), delivered on a
    /// later heartbeat — the SCM side of the compliant self-heal loop.
    pub fn with_pipeline(pipeline: CompliantPipelineFixture) -> Self {
        Self {
            pipeline: Some(pipeline),
            ..Self::new()
        }
    }

    /// Handle to inspect what the datanode sent. Clone BEFORE [`Self::into_server`].
    pub fn record(&self) -> Arc<Mutex<ScmRecord>> {
        self.record.clone()
    }

    /// Handle to enqueue commands at runtime. Clone BEFORE [`Self::into_server`].
    pub fn pending(&self) -> Arc<Mutex<VecDeque<oz::ScmCommandProto>>> {
        self.pending.clone()
    }

    /// Consume into a tonic server.
    pub fn into_server(self) -> StorageContainerDatanodeProtocolServiceServer<Self> {
        StorageContainerDatanodeProtocolServiceServer::new(self)
    }

    fn base_response(cmd_type: i32) -> oz::ScmDatanodeResponse {
        oz::ScmDatanodeResponse {
            cmd_type,
            trace_id: None,
            success: Some(true),
            message: None,
            status: oz::Status::Ok as i32,
            get_version_response: None,
            register_response: None,
            send_heartbeat_response: None,
        }
    }
}

#[tonic::async_trait]
impl StorageContainerDatanodeProtocolService for CompliantScm {
    async fn submit_request(
        &self,
        req: Request<oz::ScmDatanodeRequest>,
    ) -> Result<Response<oz::ScmDatanodeResponse>, Status> {
        let req = req.into_inner();
        let mut resp = Self::base_response(req.cmd_type);
        match oz::Type::try_from(req.cmd_type) {
            Ok(oz::Type::GetVersion) => {
                resp.get_version_response = Some(oz::ScmVersionResponseProto {
                    software_version: 1,
                    keys: Vec::new(),
                });
            }
            Ok(oz::Type::Register) => {
                let reg = req
                    .register_request
                    .ok_or_else(|| Status::invalid_argument("missing registerRequest"))?;
                // Echo the DN's OWN uuid (no rename), like the real SCM.
                let uuid = reg
                    .extended_datanode_details
                    .datanode_details
                    .uuid
                    .clone()
                    .unwrap_or_default();
                self.record.lock().registers.push(reg);
                resp.register_response = Some(oz::ScmRegisteredResponseProto {
                    error_code: oz::scm_registered_response_proto::ErrorCode::Success as i32,
                    datanode_uuid: uuid,
                    cluster_id: self.cluster_id.clone(),
                    address_list: None,
                    hostname: None,
                    ip_address: None,
                    network_name: None,
                    network_location: None,
                });
            }
            Ok(oz::Type::SendHeartbeat) => {
                let hb = req
                    .send_heartbeat_request
                    .ok_or_else(|| Status::invalid_argument("missing sendHeartbeatRequest"))?;
                let uuid = hb.datanode_details.uuid.clone().unwrap_or_default();
                // Pipeline self-heal: an UNHEALTHY incremental report becomes a
                // ReconstructEC targeting the reporting DN, deduped by (container, slot).
                if let Some(pipe) = &self.pipeline {
                    for icr in &hb.incremental_container_report {
                        for cr in &icr.report {
                            if cr.state != oz::container_replica_proto::State::Unhealthy as i32 {
                                continue;
                            }
                            let slot = cr.replica_index.unwrap_or(0);
                            if !self.issued.lock().insert((cr.container_id, slot)) {
                                continue;
                            }
                            self.pending
                                .lock()
                                .push_back(synth_reconstruct(cr.container_id, slot, &uuid, pipe));
                        }
                    }
                }
                self.record.lock().heartbeats.push(hb);
                // Remove-on-read: deliver every currently-pending command that TARGETS
                // this DN, exactly once (a ReconstructEC for another DN stays queued).
                let commands: Vec<_> = {
                    let mut q = self.pending.lock();
                    let mut keep = VecDeque::with_capacity(q.len());
                    let mut out = Vec::new();
                    while let Some(c) = q.pop_front() {
                        if command_targets(&c, &uuid) {
                            out.push(c);
                        } else {
                            keep.push_back(c);
                        }
                    }
                    *q = keep;
                    out
                };
                resp.send_heartbeat_response = Some(oz::ScmHeartbeatResponseProto {
                    datanode_uuid: uuid,
                    commands,
                    term: None,
                });
            }
            Err(_) => return Err(Status::invalid_argument("unknown cmdType")),
        }
        Ok(Response::new(resp))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oz::storage_container_datanode_protocol_service_client::StorageContainerDatanodeProtocolServiceClient as Client;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    fn dn_details(uuid: &str) -> oz::DatanodeDetailsProto {
        oz::DatanodeDetailsProto {
            uuid: Some(uuid.to_string()),
            ip_address: "127.0.0.1".to_string(),
            host_name: "host".to_string(),
            ports: vec![oz::Port {
                name: "REPLICATION".to_string(),
                value: 19864,
            }],
            ..Default::default()
        }
    }

    fn minimal_register(uuid: &str) -> oz::ScmRegisterRequestProto {
        oz::ScmRegisterRequestProto {
            extended_datanode_details: oz::ExtendedDatanodeDetailsProto {
                datanode_details: dn_details(uuid),
                ..Default::default()
            },
            node_report: oz::NodeReportProto::default(),
            container_report: oz::ContainerReportsProto::default(),
            pipeline_reports: oz::PipelineReportsProto::default(),
            data_node_layout_version: None,
        }
    }

    fn heartbeat_req(uuid: &str) -> oz::ScmDatanodeRequest {
        oz::ScmDatanodeRequest {
            cmd_type: oz::Type::SendHeartbeat as i32,
            trace_id: None,
            get_version_request: None,
            register_request: None,
            send_heartbeat_request: Some(oz::ScmHeartbeatRequestProto {
                datanode_details: dn_details(uuid),
                ..Default::default()
            }),
        }
    }

    async fn serve(scm: CompliantScm) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            Server::builder()
                .add_service(scm.into_server())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .ok();
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn datanode_handshake_roundtrips_over_real_protocol() {
        let scm = CompliantScm::new();
        let record = scm.record();
        let mut client = Client::connect(serve(scm).await).await.unwrap();

        let v = client
            .submit_request(oz::ScmDatanodeRequest {
                cmd_type: oz::Type::GetVersion as i32,
                trace_id: None,
                get_version_request: Some(oz::ScmVersionRequestProto {}),
                register_request: None,
                send_heartbeat_request: None,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(v.status, oz::Status::Ok as i32);
        assert_eq!(v.get_version_response.unwrap().software_version, 1);

        let r = client
            .submit_request(oz::ScmDatanodeRequest {
                cmd_type: oz::Type::Register as i32,
                trace_id: None,
                get_version_request: None,
                register_request: Some(minimal_register("dn-1")),
                send_heartbeat_request: None,
            })
            .await
            .unwrap()
            .into_inner();
        let reg = r.register_response.unwrap();
        assert_eq!(reg.datanode_uuid, "dn-1", "real SCM echoes the DN's own uuid (no rename)");
        assert_eq!(reg.cluster_id, "test-cluster");

        let h = client.submit_request(heartbeat_req("dn-1")).await.unwrap().into_inner();
        let hb = h.send_heartbeat_response.unwrap();
        assert_eq!(hb.datanode_uuid, "dn-1");
        assert!(hb.commands.is_empty());

        let rec = record.lock();
        assert_eq!(rec.registers.len(), 1);
        assert_eq!(rec.heartbeats.len(), 1);
        assert!(
            rec.registers[0]
                .extended_datanode_details
                .datanode_details
                .ports
                .iter()
                .any(|p| p.name == "REPLICATION"),
            "register must carry the REPLICATION port (decoded from the real proto)"
        );
    }

    #[tokio::test]
    async fn commands_drain_remove_on_read() {
        let cmd = oz::ScmCommandProto {
            command_type: oz::scm_command_proto::Type::CloseContainerCommand as i32,
            ..Default::default()
        };
        let mut client = Client::connect(serve(CompliantScm::with_commands(vec![cmd])).await)
            .await
            .unwrap();

        let first = client
            .submit_request(heartbeat_req("dn-1"))
            .await
            .unwrap()
            .into_inner()
            .send_heartbeat_response
            .unwrap();
        assert_eq!(first.commands.len(), 1, "command delivered on the first heartbeat");

        let second = client
            .submit_request(heartbeat_req("dn-1"))
            .await
            .unwrap()
            .into_inner()
            .send_heartbeat_response
            .unwrap();
        assert!(second.commands.is_empty(), "remove-on-read: not redelivered");
    }
}
