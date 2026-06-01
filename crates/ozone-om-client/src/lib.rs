//! Async gRPC transport client for the `OmRustGatewayService`.
//!
//! This crate is a thin, stateless wrapper over the `tonic`-generated client in
//! [`ozone_grpc_types::om::gw::v1`]. It exposes one async method per RPC and
//! does nothing else: requests and responses are the prost **wire** types
//! exactly as defined in `proto/om_rust_gateway_v1.proto`. No domain types are
//! introduced here — the S3 gateway owns the wire <-> domain conversion at its
//! own boundary, so duplicating it here would create two sources of truth.
//!
//! # What this wrapper does and does not do
//!
//! - It holds a single `tonic` [`Channel`] and forwards calls to it. The
//!   underlying `Channel` is itself cheaply cloneable and internally pooled by
//!   `tonic`/`hyper`; this type adds no pooling, load balancing, or sharing
//!   logic of its own.
//! - It performs **no** retries, backoff, deadlines, hedging, or circuit
//!   breaking. Every method is a single RPC attempt. Callers that need
//!   at-least-once semantics or retry-on-`UNAVAILABLE` must implement that
//!   policy themselves, around these methods.
//! - It is **stateless** beyond the channel handle: there is no per-call
//!   mutable bookkeeping. `&mut self` on the RPC methods is required only
//!   because the generated `tonic` client takes `&mut self` (it drives the
//!   inner service to readiness); it does not imply the wrapper is a
//!   single-use or stateful object. Clone the [`Channel`] and build multiple
//!   [`OmClient`]s if you want concurrent in-flight calls without `&mut`
//!   contention.
//!
//! # Errors
//!
//! All fallible operations return [`OmClientError`], which distinguishes
//! transport/connection setup failures ([`OmClientError::Connect`]) from
//! per-RPC application/transport status failures ([`OmClientError::Rpc`]).

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use ozone_grpc_types::om::gw::v1::om_rust_gateway_service_client::OmRustGatewayServiceClient;
use ozone_grpc_types::om::gw::v1::{
    AbortMultipartUploadRequest, AbortMultipartUploadResponse, AllocateBlockRequest,
    AllocateBlockResponse, CommitKeyRequest, CommitKeyResponse, CompleteMultipartUploadRequest,
    CompleteMultipartUploadResponse, CopyKeyRequest, CopyKeyResponse, CreateKeyRequest,
    CreateKeyResponse, DeleteKeyRequest, DeleteKeyResponse, HeadBucketRequest, HeadBucketResponse,
    HeadKeyRequest, HeadKeyResponse, InitiateMultipartUploadRequest,
    InitiateMultipartUploadResponse, ListKeysRequest, ListKeysResponse,
    ListMultipartUploadsRequest, ListMultipartUploadsResponse, ListPartsRequest, ListPartsResponse,
    LookupKeyRequest, LookupKeyResponse,
};
use tonic::transport::{Channel, Endpoint};

/// Errors returned by [`OmClient`].
///
/// The two variants intentionally separate the two failure regimes a caller
/// must reason about differently:
///
/// - [`OmClientError::Connect`] is raised only by [`OmClient::connect`] and
///   means the channel could not be established (bad URI, DNS failure, refused
///   connection, TLS handshake failure). It never originates from an RPC call.
/// - [`OmClientError::Rpc`] wraps a [`tonic::Status`] returned by an individual
///   RPC. This covers both application-level errors surfaced by the server
///   (e.g. `NOT_FOUND`, `PERMISSION_DENIED`) and transport-level errors that
///   manifest mid-call (e.g. `UNAVAILABLE`, `DEADLINE_EXCEEDED`). Inspect
///   [`tonic::Status::code`] to decide whether a retry is appropriate.
#[derive(Debug, thiserror::Error)]
pub enum OmClientError {
    /// Failed to establish the transport channel during [`OmClient::connect`].
    ///
    /// Carries the underlying `tonic` transport error verbatim.
    #[error("failed to connect to OM gateway: {0}")]
    Connect(#[from] tonic::transport::Error),

    /// An RPC returned a non-OK gRPC [`tonic::Status`].
    ///
    /// This is the normal way the server signals both expected outcomes
    /// (missing key, denied access) and transient transport problems. Match on
    /// the status code to classify it.
    #[error("OM gateway RPC failed: {0}")]
    Rpc(#[from] tonic::Status),
}

/// Async client for the `OmRustGatewayService`.
///
/// A `OmClient` is a thin handle around a single `tonic` [`Channel`]. It is the
/// transport seam between the Rust S3 gateway and the Ozone Manager: every
/// method maps one-to-one onto an RPC and exchanges prost wire types directly.
///
/// Construct one of two ways:
///
/// - [`OmClient::connect`] — eagerly dial an endpoint (one attempt, no retry).
/// - [`OmClient::from_channel`] — adopt a `Channel` you already built, e.g. a
///   lazily-connected or shared/cloned channel configured elsewhere. This is
///   the preferred path when the gateway owns channel construction (timeouts,
///   TLS, connection reuse) centrally.
///
/// See the crate-level docs for the explicit non-goals (no retry, no pooling,
/// stateless).
#[derive(Debug, Clone)]
pub struct OmClient {
    inner: OmRustGatewayServiceClient<Channel>,
}

impl OmClient {
    /// Dial `endpoint` and return a ready client.
    ///
    /// This makes **exactly one** connection attempt via
    /// `Endpoint::from_shared(..)?.connect().await?`; on any failure it returns
    /// [`OmClientError::Connect`] and does not retry. Retry/backoff policy is
    /// the caller's responsibility — wrap this call, or prefer
    /// [`OmClient::from_channel`] with a channel configured for lazy connect so
    /// transient startup races resolve on first use.
    ///
    /// `endpoint` is any `http`/`https` authority string accepted by
    /// [`Endpoint::from_shared`], for example `"http://127.0.0.1:50051"`.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self, OmClientError> {
        let channel = Endpoint::from_shared(endpoint.into())?.connect().await?;
        Ok(Self {
            inner: OmRustGatewayServiceClient::new(channel),
        })
    }

    /// Wrap an already-constructed [`Channel`].
    ///
    /// Use this to share or reuse a channel built elsewhere — for instance a
    /// `Channel::connect_lazy()` handle, a channel with custom timeouts/TLS, or
    /// a clone of a channel already in use by another client. No network I/O
    /// happens here; this is infallible and synchronous.
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            inner: OmRustGatewayServiceClient::new(channel),
        }
    }

    /// `HeadBucket` — check bucket existence and fetch its default config.
    pub async fn head_bucket(
        &mut self,
        req: HeadBucketRequest,
    ) -> Result<HeadBucketResponse, OmClientError> {
        Ok(self.inner.head_bucket(req).await?.into_inner())
    }

    /// `LookupKey` — resolve a key to its block locations for reads.
    pub async fn lookup_key(
        &mut self,
        req: LookupKeyRequest,
    ) -> Result<LookupKeyResponse, OmClientError> {
        Ok(self.inner.lookup_key(req).await?.into_inner())
    }

    /// `HeadKey` — fetch key metadata without its block locations payload.
    pub async fn head_key(
        &mut self,
        req: HeadKeyRequest,
    ) -> Result<HeadKeyResponse, OmClientError> {
        Ok(self.inner.head_key(req).await?.into_inner())
    }

    /// `CreateKey` — open a key for writing; returns client/open ids and any
    /// pre-allocated blocks.
    pub async fn create_key(
        &mut self,
        req: CreateKeyRequest,
    ) -> Result<CreateKeyResponse, OmClientError> {
        Ok(self.inner.create_key(req).await?.into_inner())
    }

    /// `AllocateBlock` — request an additional block for an open key.
    pub async fn allocate_block(
        &mut self,
        req: AllocateBlockRequest,
    ) -> Result<AllocateBlockResponse, OmClientError> {
        Ok(self.inner.allocate_block(req).await?.into_inner())
    }

    /// `CommitKey` — finalize an open key with its size and block locations.
    pub async fn commit_key(
        &mut self,
        req: CommitKeyRequest,
    ) -> Result<CommitKeyResponse, OmClientError> {
        Ok(self.inner.commit_key(req).await?.into_inner())
    }

    /// `DeleteKey` — delete a committed key.
    pub async fn delete_key(
        &mut self,
        req: DeleteKeyRequest,
    ) -> Result<DeleteKeyResponse, OmClientError> {
        Ok(self.inner.delete_key(req).await?.into_inner())
    }

    /// `CopyKey` — server-side copy from a source key to a destination key.
    pub async fn copy_key(
        &mut self,
        req: CopyKeyRequest,
    ) -> Result<CopyKeyResponse, OmClientError> {
        Ok(self.inner.copy_key(req).await?.into_inner())
    }

    /// `InitiateMultipartUpload` — begin a multipart upload; returns the upload id.
    pub async fn initiate_multipart_upload(
        &mut self,
        req: InitiateMultipartUploadRequest,
    ) -> Result<InitiateMultipartUploadResponse, OmClientError> {
        Ok(self
            .inner
            .initiate_multipart_upload(req)
            .await?
            .into_inner())
    }

    /// `AbortMultipartUpload` — discard an in-progress multipart upload.
    pub async fn abort_multipart_upload(
        &mut self,
        req: AbortMultipartUploadRequest,
    ) -> Result<AbortMultipartUploadResponse, OmClientError> {
        Ok(self.inner.abort_multipart_upload(req).await?.into_inner())
    }

    /// `CompleteMultipartUpload` — assemble uploaded parts into the final key.
    pub async fn complete_multipart_upload(
        &mut self,
        req: CompleteMultipartUploadRequest,
    ) -> Result<CompleteMultipartUploadResponse, OmClientError> {
        Ok(self
            .inner
            .complete_multipart_upload(req)
            .await?
            .into_inner())
    }

    /// `ListParts` — page through the parts already uploaded for an upload id.
    pub async fn list_parts(
        &mut self,
        req: ListPartsRequest,
    ) -> Result<ListPartsResponse, OmClientError> {
        Ok(self.inner.list_parts(req).await?.into_inner())
    }

    /// `ListMultipartUploads` — page through in-progress multipart uploads.
    pub async fn list_multipart_uploads(
        &mut self,
        req: ListMultipartUploadsRequest,
    ) -> Result<ListMultipartUploadsResponse, OmClientError> {
        Ok(self.inner.list_multipart_uploads(req).await?.into_inner())
    }

    /// `ListKeys` — server-streaming key listing.
    ///
    /// Returns the inbound [`tonic::Streaming`] handle directly; the caller
    /// drives it (e.g. `while let Some(item) = stream.message().await?`). Each
    /// streamed item is itself fallible: errors surface as [`tonic::Status`]
    /// while polling the stream, not from this call. This method only fails if
    /// the stream could not be opened.
    pub async fn list_keys(
        &mut self,
        req: ListKeysRequest,
    ) -> Result<tonic::Streaming<ListKeysResponse>, OmClientError> {
        Ok(self.inner.list_keys(req).await?.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // `connect` makes a single real dial attempt. Port 1 is the well-known
    // "this will refuse" port; the attempt must fail (no server, no retry) and
    // surface as `OmClientError::Connect`.
    #[tokio::test]
    async fn connect_to_dead_endpoint_errors() {
        let result = OmClient::connect("http://127.0.0.1:1").await;
        assert!(
            matches!(result, Err(OmClientError::Connect(_))),
            "expected Connect error dialing a dead endpoint, got: {result:?}"
        );
    }

    // A syntactically invalid endpoint fails at `Endpoint::from_shared`, which
    // is also a transport error and maps to `OmClientError::Connect` via
    // `#[from]`. Guards the `?` on `from_shared`.
    #[tokio::test]
    async fn connect_to_invalid_endpoint_errors() {
        let result = OmClient::connect("not a valid uri").await;
        assert!(
            matches!(result, Err(OmClientError::Connect(_))),
            "expected Connect error for a malformed endpoint, got: {result:?}"
        );
    }

    // `from_channel` is pure construction with no network I/O. A
    // lazily-connected channel must build an `OmClient` without contacting any
    // server. `connect_lazy` defers the actual dial but registers a connection
    // task with the runtime at construction time, so this needs a reactor
    // present (hence `#[tokio::test]`); it still never reaches the server. This
    // is the "real but server-free" smoke test for the adopt-a-channel path.
    #[tokio::test]
    async fn from_channel_builds_client() {
        let channel = Channel::from_static("http://127.0.0.1:50051").connect_lazy();
        let _client = OmClient::from_channel(channel);
    }
}
