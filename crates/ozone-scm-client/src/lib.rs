//! Thin async gRPC transport client for `ScmRustDatanodeService`.
//!
//! This crate is **pure transport**: it wraps the tonic-generated
//! [`ScmRustDatanodeServiceClient`] and exposes the four service RPCs with the
//! tonic plumbing (`Response`/`IntoRequest`/`IntoStreamingRequest`) hidden.
//! Callers work directly in the prost wire types from
//! [`ozone_grpc_types::scm::dn::v1`]; we deliberately do **not** introduce
//! SCM-specific domain types here. Higher layers (the datanode's registration
//! and heartbeat state machines) own retry, backoff, scheduling, and any
//! mapping to domain models.
//!
//! ## Connection model
//! [`ScmClient::connect`] performs a **single** eager connect attempt and
//! returns an error if the SCM is unreachable. There is no built-in retry or
//! reconnect: a caller that needs resilience should loop around `connect`
//! itself, or build its own [`Channel`] (e.g. `connect_lazy`) and hand it to
//! [`ScmClient::from_channel`].
//!
//! ## Streaming RPCs
//! `Heartbeat` and `ContainerReport` are **bidirectional** streams. For each,
//! the caller supplies an outbound [`futures::Stream`] of request messages and
//! receives the inbound [`tonic::Streaming`] of responses. The two halves run
//! independently: the caller drives the outbound side (deciding cadence and
//! when to stop) and concurrently consumes the inbound side. Dropping the
//! returned response stream tears the RPC down. The heartbeat stream in
//! particular is **long-lived** — it stays open for the lifetime of the
//! datanode's registration and carries periodic reports out and SCM commands
//! back in.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod compliant;

use ozone_grpc_types::scm::dn::v1::scm_rust_datanode_service_client::ScmRustDatanodeServiceClient;
use ozone_grpc_types::scm::dn::v1::{
    ContainerReportAck, ContainerReportRequest, HeartbeatRequest, HeartbeatResponse,
    RegisterRequest, RegisterResponse, VersionRequest, VersionResponse,
};
use tonic::transport::{Channel, Endpoint};

/// Errors raised by [`ScmClient`].
///
/// Connection-establishment failures and per-RPC failures are kept as distinct
/// variants so callers can react differently (e.g. a [`Self::Connect`] error
/// during startup is fatal, whereas an [`Self::Rpc`] error mid-stream usually
/// triggers a reconnect in the caller's loop).
#[derive(Debug, thiserror::Error)]
pub enum ScmClientError {
    /// Failed to build the [`Endpoint`] or establish the transport channel.
    ///
    /// Wraps [`tonic::transport::Error`], which covers both an invalid endpoint
    /// URI and a refused/timed-out TCP/TLS connect.
    #[error("scm transport connect failed: {0}")]
    Connect(#[from] tonic::transport::Error),

    /// The RPC reached the server but returned a non-OK gRPC status, or the
    /// stream failed in transit.
    ///
    /// Wraps the [`tonic::Status`] verbatim so the gRPC code and message
    /// survive for the caller to inspect.
    #[error("scm rpc failed: {0}")]
    Rpc(#[from] tonic::Status),
}

/// Async client for `ScmRustDatanodeService`.
///
/// Cheap to clone-by-channel: the underlying tonic [`Channel`] multiplexes
/// concurrent RPCs over one HTTP/2 connection, so sharing one channel via
/// [`Self::from_channel`] is the idiomatic way to fan out calls. The RPC
/// methods take `&mut self` because the generated tonic client requires unique
/// access while a call is in flight.
#[derive(Debug, Clone)]
pub struct ScmClient {
    inner: ScmRustDatanodeServiceClient<Channel>,
}

impl ScmClient {
    /// Connect to an SCM endpoint and return a ready client.
    ///
    /// `endpoint` is a URI string such as `"http://127.0.0.1:9863"`. This does
    /// a **single** eager connect attempt: a DNS, TCP, or TLS failure surfaces
    /// immediately as [`ScmClientError::Connect`]. No retry is performed here —
    /// that is the caller's responsibility. To defer the connection or to share
    /// a pre-built channel, use [`Self::from_channel`] instead.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self, ScmClientError> {
        let channel = Endpoint::from_shared(endpoint.into())?.connect().await?;
        Ok(Self::from_channel(channel))
    }

    /// Wrap an already-built [`Channel`] without performing any I/O.
    ///
    /// Use this to share one channel across multiple service clients, or to
    /// inject a lazily-connected channel (`Channel::connect_lazy`) — including
    /// in tests, where it lets the client be constructed without a live server.
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            inner: ScmRustDatanodeServiceClient::new(channel),
        }
    }

    /// `GetVersion` — unary handshake that returns the SCM's protocol version
    /// and cluster id. Typically the first call after connecting, used to gate
    /// compatibility before registering.
    pub async fn get_version(
        &mut self,
        req: VersionRequest,
    ) -> Result<VersionResponse, ScmClientError> {
        Ok(self.inner.get_version(req).await?.into_inner())
    }

    /// `Register` — unary call that enrolls this datanode with the SCM.
    ///
    /// The response carries the SCM-assigned UUID (which may differ from the
    /// one proposed in the request on first registration) and the heartbeat
    /// interval the datanode must honor.
    pub async fn register(
        &mut self,
        req: RegisterRequest,
    ) -> Result<RegisterResponse, ScmClientError> {
        Ok(self.inner.register(req).await?.into_inner())
    }

    /// `Heartbeat` — open the long-lived bidirectional heartbeat stream.
    ///
    /// `outbound` is the caller-driven stream of [`HeartbeatRequest`]s (node
    /// reports plus command-status updates); the returned [`tonic::Streaming`]
    /// yields [`HeartbeatResponse`]s carrying SCM commands. The caller owns the
    /// cadence of `outbound` and must concurrently poll the returned stream to
    /// receive commands. Dropping the returned stream closes the RPC.
    pub async fn heartbeat<S>(
        &mut self,
        outbound: S,
    ) -> Result<tonic::Streaming<HeartbeatResponse>, ScmClientError>
    where
        S: futures::Stream<Item = HeartbeatRequest> + Send + 'static,
    {
        Ok(self.inner.heartbeat(outbound).await?.into_inner())
    }

    /// `ContainerReport` — open the bidirectional container-report stream.
    ///
    /// `outbound` is the caller-driven stream of [`ContainerReportRequest`]s
    /// (full or incremental container reports); the returned
    /// [`tonic::Streaming`] yields [`ContainerReportAck`]s acknowledging
    /// processed sequence numbers. As with [`Self::heartbeat`], the two halves
    /// are independent and dropping the returned stream closes the RPC.
    pub async fn container_report<S>(
        &mut self,
        outbound: S,
    ) -> Result<tonic::Streaming<ContainerReportAck>, ScmClientError>
    where
        S: futures::Stream<Item = ContainerReportRequest> + Send + 'static,
    {
        Ok(self.inner.container_report(outbound).await?.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `connect` does a single eager attempt; pointing it at a port nothing is
    /// bound to must fail with a transport (connect) error, not panic or hang.
    #[tokio::test]
    async fn connect_refused_is_connect_error() {
        let err = ScmClient::connect("http://127.0.0.1:1")
            .await
            .expect_err("connecting to an unbound port must fail");
        assert!(
            matches!(err, ScmClientError::Connect(_)),
            "expected Connect error, got {err:?}"
        );
    }

    /// `from_channel` over a lazily-connected channel must construct the client
    /// without contacting a server. `connect_lazy` defers the TCP connect but
    /// still spawns its connection manager onto the current runtime, so this
    /// runs under `#[tokio::test]` to provide a reactor; no I/O to a live
    /// server happens.
    #[tokio::test]
    async fn from_channel_builds_over_lazy_channel() {
        let channel = Channel::from_static("http://127.0.0.1:50051").connect_lazy();
        let _client = ScmClient::from_channel(channel);
    }

    /// A malformed endpoint URI must surface as a [`ScmClientError::Connect`]
    /// (the `Endpoint::from_shared` parse failure), exercising the `?`-from
    /// conversion on the transport-error path without any network I/O.
    #[tokio::test]
    async fn connect_invalid_uri_is_connect_error() {
        let err = ScmClient::connect("not a valid uri")
            .await
            .expect_err("a malformed URI must fail");
        assert!(
            matches!(err, ScmClientError::Connect(_)),
            "expected Connect error, got {err:?}"
        );
    }
}
