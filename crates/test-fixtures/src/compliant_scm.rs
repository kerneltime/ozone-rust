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

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;
use tonic::{Request, Response, Status};

use ozone_grpc_types::hadoop::hdds as oz;
use oz::storage_container_datanode_protocol_service_server::{
    StorageContainerDatanodeProtocolService, StorageContainerDatanodeProtocolServiceServer,
};

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
        }
    }

    /// A compliant SCM pre-loaded with `commands`, delivered (drained) on the
    /// datanode's heartbeats — all currently-pending on each heartbeat, once.
    pub fn with_commands(commands: Vec<oz::ScmCommandProto>) -> Self {
        let s = Self::new();
        *s.pending.lock() = commands.into();
        s
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
                self.record.lock().heartbeats.push(hb);
                // Remove-on-read: deliver every currently-pending command once.
                let commands: Vec<_> = self.pending.lock().drain(..).collect();
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
