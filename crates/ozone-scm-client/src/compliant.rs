//! Compliant transport client for the REAL Apache Ozone
//! `StorageContainerDatanodeProtocol` (vendored `ozone_grpc_types::hadoop::hdds`).
//!
//! The datanode is the client and SCM never pushes: the DN POLLS via the single
//! unary `submitRequest`, multiplexed by a `Type` discriminator
//! (`GetVersion`/`Register`/`SendHeartbeat`). Each wrapper builds the
//! `SCMDatanodeRequest` envelope, checks the response `status`, and returns the
//! typed sub-response. This crate is pure transport — the register/heartbeat
//! state machine (cadence, retry, report building) lives in the datanode.

use ozone_grpc_types::hadoop::hdds as oz;
use oz::storage_container_datanode_protocol_service_client::StorageContainerDatanodeProtocolServiceClient;
use tonic::transport::{Channel, Endpoint};

/// Errors raised by [`OzoneScmClient`].
#[derive(Debug, thiserror::Error)]
pub enum ScmError {
    /// Failed to build the endpoint or establish the transport channel.
    #[error("scm transport connect failed: {0}")]
    Connect(#[from] tonic::transport::Error),
    /// The unary RPC returned a non-OK gRPC status.
    #[error("scm rpc failed: {0}")]
    Rpc(#[from] tonic::Status),
    /// The RPC succeeded at the transport layer but SCM set `status = ERROR`.
    #[error("scm returned status ERROR: {0}")]
    Scm(String),
    /// The response did not carry the sub-message its `cmdType` requires.
    #[error("scm response missing the {0} sub-message")]
    MissingResponse(&'static str),
}

/// Compliant async client for the real SCM datanode protocol.
#[derive(Debug, Clone)]
pub struct OzoneScmClient {
    inner: StorageContainerDatanodeProtocolServiceClient<Channel>,
}

impl OzoneScmClient {
    /// Connect to an SCM endpoint (single eager attempt; caller owns retry).
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self, ScmError> {
        let channel = Endpoint::from_shared(endpoint.into())?.connect().await?;
        Ok(Self::from_channel(channel))
    }

    /// Wrap an already-built channel (e.g. a lazily-connected one) without I/O.
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            inner: StorageContainerDatanodeProtocolServiceClient::new(channel),
        }
    }

    async fn submit(
        &mut self,
        req: oz::ScmDatanodeRequest,
    ) -> Result<oz::ScmDatanodeResponse, ScmError> {
        let resp = self.inner.submit_request(req).await?.into_inner();
        if resp.status == oz::Status::Error as i32 {
            return Err(ScmError::Scm(resp.message.unwrap_or_default()));
        }
        Ok(resp)
    }

    /// `GetVersion` — the version/cluster handshake before registering.
    pub async fn get_version(&mut self) -> Result<oz::ScmVersionResponseProto, ScmError> {
        self.submit(oz::ScmDatanodeRequest {
            cmd_type: oz::Type::GetVersion as i32,
            trace_id: None,
            get_version_request: Some(oz::ScmVersionRequestProto {}),
            register_request: None,
            send_heartbeat_request: None,
        })
        .await?
        .get_version_response
        .ok_or(ScmError::MissingResponse("getVersionResponse"))
    }

    /// `Register` — enroll with SCM (carries the four inline reports). The
    /// response echoes the DN's OWN uuid + the cluster id; the DN keeps its uuid.
    pub async fn register(
        &mut self,
        reg: oz::ScmRegisterRequestProto,
    ) -> Result<oz::ScmRegisteredResponseProto, ScmError> {
        self.submit(oz::ScmDatanodeRequest {
            cmd_type: oz::Type::Register as i32,
            trace_id: None,
            get_version_request: None,
            register_request: Some(reg),
            send_heartbeat_request: None,
        })
        .await?
        .register_response
        .ok_or(ScmError::MissingResponse("registerResponse"))
    }

    /// `SendHeartbeat` — one poll. The response's `commands` are SCM's per-DN
    /// queue drained remove-on-read; the caller MUST dispatch them all before the
    /// next heartbeat (a dropped command is not redelivered).
    pub async fn send_heartbeat(
        &mut self,
        hb: oz::ScmHeartbeatRequestProto,
    ) -> Result<oz::ScmHeartbeatResponseProto, ScmError> {
        self.submit(oz::ScmDatanodeRequest {
            cmd_type: oz::Type::SendHeartbeat as i32,
            trace_id: None,
            get_version_request: None,
            register_request: None,
            send_heartbeat_request: Some(hb),
        })
        .await?
        .send_heartbeat_response
        .ok_or(ScmError::MissingResponse("sendHeartbeatResponse"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_fixtures::compliant_scm::CompliantScm;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    fn dn(uuid: &str) -> oz::DatanodeDetailsProto {
        oz::DatanodeDetailsProto {
            uuid: Some(uuid.to_string()),
            ip_address: "127.0.0.1".to_string(),
            host_name: "h".to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn handshakes_and_polls_against_compliant_scm() {
        let scm = CompliantScm::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            Server::builder()
                .add_service(scm.into_server())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .ok();
        });
        let mut client = OzoneScmClient::connect(format!("http://{addr}")).await.unwrap();

        assert_eq!(client.get_version().await.unwrap().software_version, 1);

        let reg = oz::ScmRegisterRequestProto {
            extended_datanode_details: oz::ExtendedDatanodeDetailsProto {
                datanode_details: dn("dn-1"),
                ..Default::default()
            },
            node_report: oz::NodeReportProto::default(),
            container_report: oz::ContainerReportsProto::default(),
            pipeline_reports: oz::PipelineReportsProto::default(),
            data_node_layout_version: None,
        };
        assert_eq!(client.register(reg).await.unwrap().datanode_uuid, "dn-1");

        let hb = oz::ScmHeartbeatRequestProto {
            datanode_details: dn("dn-1"),
            ..Default::default()
        };
        let resp = client.send_heartbeat(hb).await.unwrap();
        assert_eq!(resp.datanode_uuid, "dn-1");
        assert!(resp.commands.is_empty());
    }
}
