//! In-memory fake Ozone Manager implementing [`OmRustGatewayService`].
//!
//! This fixture lets S3-gateway integration tests exercise the OM control
//! plane (create/allocate/commit/lookup/list/copy/delete) plus multipart
//! upload (initiate/complete/abort/list-parts/list-uploads) without a real Java
//! OM. See the per-RPC docs below for the exact contract.
//!
//! # Multipart model: the gateway tracks parts, the OM only finalizes
//! This fake intentionally does NOT persist per-part state. In the real system
//! the gateway is the authority for in-flight multipart uploads: it allocates
//! blocks per part, writes part data to datanodes, and remembers each part's
//! `(part_number, etag, size, locations)`. The OM's role is narrow:
//! - `initiate` mints a unique `upload_id` (and nothing else; the request's
//!   `vbk` is not persisted at initiate time).
//! - `complete` receives the full, authoritative part list from the gateway and
//!   stitches it into one committed key (concatenated locations, summed size,
//!   AWS-style multipart ETag). This is the only multipart RPC that mutates
//!   `keys`.
//! - `abort` is a no-op: the gateway (not the OM) owns part cleanup on the
//!   datanodes, so there is nothing for the OM to undo here.
//! - `list_parts` / `list_multipart_uploads` return empty: the gateway serves
//!   these from its own in-flight registry; this OM fixture has no part state to
//!   enumerate. They exist only so the trait is fully implemented.
//!
//! Because of this split there is no `uploads`/`parts` map in [`State`]; the
//! only multipart state is the `next_upload_id` counter.
//!
//! # Topology
//! A `FakeOm` is parameterized by exactly ONE static EC pipeline
//! ([`PipelineConfig`]). Every block this fake allocates lands in container `1`
//! on that one pipeline. The pipeline's datanodes are fixed for the life of the
//! fixture; there is no placement, no exclusion-list handling, and no failover.
//!
//! # Invariants
//! - `datanodes.len() == ec.data + ec.parity` (k + p). Enforced in
//!   [`PipelineConfig::new`]; constructing a `FakeOm` with a mismatched config
//!   panics, because a test that misconfigures the pipeline is a test bug.
//! - Replica-slot mapping is positional and 1-indexed: `datanodes[i]` holds EC
//!   replica slot `i + 1`. Slot 1..=k are data shards, slot k+1..=k+p are parity
//!   shards (Ozone's convention; see `BlockId.replica_index` in the dn proto).
//!   This mapping is published verbatim in `Pipeline.member_replica_indexes`
//!   (keyed by datanode UUID).
//! - The block-GROUP id (`BlockId.replica_index == 0`) is what the OM hands to
//!   the gateway. Per-shard replica indexes are NOT encoded in the allocated
//!   `BlockId`; they live only in the pipeline's `member_replica_indexes`. The
//!   gateway is expected to derive per-shard `BlockId`s by cloning the group id
//!   and setting `replica_index` from the pipeline. We mirror real OM behavior
//!   here so the gateway code path is identical.
//! - `local_id` is monotonically increasing across the whole fixture (shared
//!   counter), starting at 1 for the first allocation. `container_id` is always
//!   `1`. `client_id` (returned by `create_key`) is a separate monotonic
//!   counter starting at 1.
//! - All timestamps (`creation_time_ms`, `modification_time_ms`,
//!   `commit_time_ms`) are `0`. No wall clock is wired into this fixture on
//!   purpose: tests that assert on timestamps would be non-deterministic, and
//!   the gateway does not depend on OM-supplied timestamps for correctness.
//!
//! # ETag handling
//! `commit_key` receives the gateway-computed ETag as raw bytes. We store it in
//! the key's `metadata` under the literal key `"ETAG"` as a UTF-8 *lossy*
//! string (matching how the real OM surfaces it via `OmKeyInfoLite.metadata`),
//! and we also merge any request-supplied metadata. `copy_key` reads the ETag
//! back out of that metadata entry and returns it as bytes.

use std::collections::HashMap;
use std::pin::Pin;

use md5::{Digest, Md5};
use parking_lot::Mutex;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use ozone_grpc_types::dn::v1 as dn;
use ozone_grpc_types::om::gw::v1 as pb;
use ozone_grpc_types::om::gw::v1::om_rust_gateway_service_server::{
    OmRustGatewayService, OmRustGatewayServiceServer,
};

/// Metadata key under which the committed object's ETag is stored, mirroring
/// the real OM's `OmKeyInfoLite.metadata["ETAG"]` surface.
const ETAG_METADATA_KEY: &str = "ETAG";

/// Fixed container id for every block this fake allocates.
const CONTAINER_ID: u64 = 1;

/// Fixed pipeline id reported in every allocated [`pb::Pipeline`].
const PIPELINE_ID: &str = "pipeline-1";

/// The static EC pipeline a [`FakeOm`] allocates from.
///
/// Holds the ordered datanode set plus its EC config. The ordering is
/// load-bearing: `datanodes[i]` is the holder of replica slot `i + 1` (see the
/// module-level invariants). Build one with [`PipelineConfig::new`], which
/// enforces `datanodes.len() == data + parity`.
#[derive(Clone, Debug)]
pub struct PipelineConfig {
    /// Ordered datanodes. Position `i` (0-based) owns EC replica slot `i + 1`.
    pub datanodes: Vec<pb::DatanodeDetails>,
    /// Erasure-coding parameters; `data + parity` must equal `datanodes.len()`.
    pub ec: dn::EcReplicationConfig,
}

impl PipelineConfig {
    /// Build a pipeline config, asserting the datanode count matches k + p.
    ///
    /// # Panics
    /// Panics if `datanodes.len() != ec.data + ec.parity`. A mismatch is a test
    /// configuration bug, so failing loudly at construction is intended.
    pub fn new(datanodes: Vec<pb::DatanodeDetails>, ec: dn::EcReplicationConfig) -> Self {
        let expected = (ec.data + ec.parity) as usize;
        assert_eq!(
            datanodes.len(),
            expected,
            "pipeline datanode count {} must equal data+parity {} (k={}, p={})",
            datanodes.len(),
            expected,
            ec.data,
            ec.parity,
        );
        Self { datanodes, ec }
    }
}

/// Mutable in-memory state, guarded by a single `parking_lot::Mutex`.
///
/// A single coarse lock over the whole map is intentional: this is a test
/// fixture, contention is irrelevant, and one lock makes the RPC handlers
/// trivially correct under concurrent gateway calls.
struct State {
    /// Committed keys, addressed by `(volume, bucket, key)`.
    keys: HashMap<(String, String, String), pb::OmKeyInfoLite>,
    /// Next block `local_id` to hand out; first allocation returns 1.
    next_local_id: u64,
    /// Next `client_id` to hand out from `create_key`; first call returns 1.
    next_client_id: u64,
    /// Next multipart `upload_id` ordinal to hand out from
    /// `initiate_multipart_upload`; first call returns `upload-1`. There is no
    /// per-part state to track here on purpose (see module docs: the gateway is
    /// the authority for in-flight parts; the OM only finalizes on `complete`).
    next_upload_id: u64,
    /// Buckets that exist, keyed by `(volume, bucket)`, value = creation time ms
    /// (always 0; no clock). Backs CreateBucket/DeleteBucket/ListBuckets.
    /// HeadBucket stays permissive (always reports exists) so callers that
    /// assume pre-provisioned OBS buckets keep working.
    buckets: HashMap<(String, String), u64>,
}

/// In-memory fake Ozone Manager.
///
/// Cheap-ish to construct; cloning is not provided because the server takes
/// ownership via [`FakeOm::into_server`]. All RPC handlers borrow `&self` and
/// lock the internal state as needed.
pub struct FakeOm {
    state: Mutex<State>,
    pipeline: PipelineConfig,
}

impl FakeOm {
    /// Create a fake OM that allocates every block from `pipeline`.
    pub fn new(pipeline: PipelineConfig) -> Self {
        Self {
            state: Mutex::new(State {
                keys: HashMap::new(),
                next_local_id: 1,
                next_client_id: 1,
                next_upload_id: 1,
                buckets: HashMap::new(),
            }),
            pipeline,
        }
    }

    /// Wrap this fake in the tonic server for `Server::add_service`.
    pub fn into_server(self) -> OmRustGatewayServiceServer<Self> {
        OmRustGatewayServiceServer::new(self)
    }

    /// The EC config of the configured pipeline.
    fn ec(&self) -> dn::EcReplicationConfig {
        self.pipeline.ec.clone()
    }

    /// Build the static pipeline message: configured datanodes plus the
    /// positional `uuid -> (i + 1)` replica-slot map.
    fn pipeline_message(&self) -> pb::Pipeline {
        let member_replica_indexes = self
            .pipeline
            .datanodes
            .iter()
            .enumerate()
            .map(|(i, dn)| (dn.uuid.clone(), i as u32 + 1))
            .collect();
        pb::Pipeline {
            id: PIPELINE_ID.to_string(),
            members: self.pipeline.datanodes.clone(),
            member_replica_indexes,
        }
    }

    /// Allocate one block on the fixed pipeline and bump `next_local_id`.
    ///
    /// The returned `BlockId` carries `replica_index == 0` (the block-GROUP id);
    /// per-shard slots are conveyed only through the pipeline's replica-index
    /// map. `offset`/`length` are 0 and `block_token` is empty: this fake issues
    /// no SCM tokens, and the DN fixture accepts an empty token.
    ///
    /// Named distinctly from the trait's `allocate_block` RPC so that inherent
    /// method resolution never shadows the trait method on `FakeOm`.
    fn alloc_block(&self) -> pb::KeyLocation {
        let local_id = {
            let mut st = self.state.lock();
            let id = st.next_local_id;
            st.next_local_id += 1;
            id
        };
        pb::KeyLocation {
            block_id: Some(dn::BlockId {
                container_id: CONTAINER_ID,
                local_id,
                replica_index: 0,
            }),
            offset: 0,
            length: 0,
            pipeline: Some(self.pipeline_message()),
            block_token: Vec::new(),
        }
    }
}

/// Pull the `(volume, bucket, key)` tuple out of a request's `vbk`, or map a
/// missing `vbk` to `InvalidArgument`.
///
/// `tonic::Status` is a large error type dictated by tonic; the trait methods
/// that consume this helper return `Result<_, Status>` anyway, so boxing here
/// would just add an unbox at every call site. Allow the large-Err lint.
#[allow(clippy::result_large_err)]
fn key_tuple(vbk: &Option<pb::VolumeBucketKey>) -> Result<(String, String, String), Status> {
    let vbk = vbk
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing vbk"))?;
    Ok((
        vbk.volume_name.clone(),
        vbk.bucket_name.clone(),
        vbk.key_name.clone(),
    ))
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl OmRustGatewayService for FakeOm {
    async fn head_bucket(
        &self,
        _req: Request<pb::HeadBucketRequest>,
    ) -> Result<Response<pb::HeadBucketResponse>, Status> {
        Ok(Response::new(pb::HeadBucketResponse {
            exists: true,
            default_ec_config: Some(self.ec()),
            bucket_layout: "OBJECT_STORE".to_string(),
        }))
    }

    /// Create a bucket. Idempotent: `created` is false if it already existed.
    async fn create_bucket(
        &self,
        req: Request<pb::CreateBucketRequest>,
    ) -> Result<Response<pb::CreateBucketResponse>, Status> {
        let req = req.into_inner();
        let created = self
            .state
            .lock()
            .buckets
            .insert((req.volume_name, req.bucket_name), 0)
            .is_none();
        Ok(Response::new(pb::CreateBucketResponse { created }))
    }

    /// Delete a bucket. Lenient: succeeds whether or not it existed.
    async fn delete_bucket(
        &self,
        req: Request<pb::DeleteBucketRequest>,
    ) -> Result<Response<pb::DeleteBucketResponse>, Status> {
        let req = req.into_inner();
        self.state
            .lock()
            .buckets
            .remove(&(req.volume_name, req.bucket_name));
        Ok(Response::new(pb::DeleteBucketResponse {}))
    }

    /// List buckets in a volume, sorted by name.
    async fn list_buckets(
        &self,
        req: Request<pb::ListBucketsRequest>,
    ) -> Result<Response<pb::ListBucketsResponse>, Status> {
        let vol = req.into_inner().volume_name;
        let st = self.state.lock();
        let mut buckets: Vec<pb::list_buckets_response::BucketInfo> = st
            .buckets
            .iter()
            .filter(|((v, _), _)| *v == vol)
            .map(|((_, b), t)| pb::list_buckets_response::BucketInfo {
                bucket_name: b.clone(),
                creation_time_ms: *t,
            })
            .collect();
        buckets.sort_by(|a, b| a.bucket_name.cmp(&b.bucket_name));
        Ok(Response::new(pb::ListBucketsResponse { buckets }))
    }

    async fn lookup_key(
        &self,
        req: Request<pb::LookupKeyRequest>,
    ) -> Result<Response<pb::LookupKeyResponse>, Status> {
        let tuple = key_tuple(&req.into_inner().vbk)?;
        let key_info = self
            .state
            .lock()
            .keys
            .get(&tuple)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("key not found: {}", tuple.2)))?;
        Ok(Response::new(pb::LookupKeyResponse {
            key_info: Some(key_info),
        }))
    }

    async fn head_key(
        &self,
        req: Request<pb::HeadKeyRequest>,
    ) -> Result<Response<pb::HeadKeyResponse>, Status> {
        let tuple = key_tuple(&req.into_inner().vbk)?;
        let key_info = self
            .state
            .lock()
            .keys
            .get(&tuple)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("key not found: {}", tuple.2)))?;
        Ok(Response::new(pb::HeadKeyResponse {
            key_info: Some(key_info),
        }))
    }

    type ListKeysStream = BoxStream<pb::ListKeysResponse>;

    /// List keys under `prefix`, optionally folding `delimiter`.
    ///
    /// Pagination is a no-op: this always emits exactly one response with
    /// `is_truncated = false` and an empty continuation token, regardless of
    /// `max_keys` or `continuation_token`. When `delimiter` is non-empty, the
    /// substring of `key_name` after `prefix` up to (and including) the first
    /// delimiter is folded into `common_prefixes` (deduplicated, insertion
    /// order preserved) and that key is omitted from `keys`; keys with no
    /// delimiter in their remainder are returned in `keys` as usual.
    async fn list_keys(
        &self,
        req: Request<pb::ListKeysRequest>,
    ) -> Result<Response<Self::ListKeysStream>, Status> {
        let req = req.into_inner();
        let mut keys: Vec<pb::OmKeyInfoLite> = Vec::new();
        let mut common_prefixes: Vec<String> = Vec::new();

        let state = self.state.lock();
        let mut matching: Vec<&pb::OmKeyInfoLite> = state
            .keys
            .iter()
            .filter(|((vol, bucket, key), _)| {
                *vol == req.volume_name && *bucket == req.bucket_name && key.starts_with(&req.prefix)
            })
            .map(|(_, info)| info)
            .collect();
        // Deterministic output ordering by key name (HashMap iteration is not).
        matching.sort_by(|a, b| {
            let ak = a.vbk.as_ref().map(|v| v.key_name.as_str()).unwrap_or("");
            let bk = b.vbk.as_ref().map(|v| v.key_name.as_str()).unwrap_or("");
            ak.cmp(bk)
        });

        for info in matching {
            let key_name = info
                .vbk
                .as_ref()
                .map(|v| v.key_name.as_str())
                .unwrap_or("");
            if req.delimiter.is_empty() {
                keys.push(info.clone());
                continue;
            }
            let remainder = &key_name[req.prefix.len()..];
            match remainder.find(&req.delimiter) {
                Some(idx) => {
                    let end = idx + req.delimiter.len();
                    let common = format!("{}{}", req.prefix, &remainder[..end]);
                    if !common_prefixes.contains(&common) {
                        common_prefixes.push(common);
                    }
                }
                None => keys.push(info.clone()),
            }
        }
        drop(state);

        let resp = pb::ListKeysResponse {
            keys,
            common_prefixes,
            next_continuation_token: String::new(),
            is_truncated: false,
        };
        let stream = tokio_stream::once(Ok(resp));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn create_key(
        &self,
        _req: Request<pb::CreateKeyRequest>,
    ) -> Result<Response<pb::CreateKeyResponse>, Status> {
        let client_id = {
            let mut st = self.state.lock();
            let id = st.next_client_id;
            st.next_client_id += 1;
            id
        };
        Ok(Response::new(pb::CreateKeyResponse {
            client_id,
            open_version: 1,
            pre_allocated_blocks: vec![self.alloc_block()],
        }))
    }

    async fn allocate_block(
        &self,
        _req: Request<pb::AllocateBlockRequest>,
    ) -> Result<Response<pb::AllocateBlockResponse>, Status> {
        Ok(Response::new(pb::AllocateBlockResponse {
            new_block: Some(self.alloc_block()),
        }))
    }

    /// Commit a key: persist an [`pb::OmKeyInfoLite`] built from the request.
    ///
    /// `data_size` comes from `final_size`, `locations` from `final_locations`,
    /// `ec_config` from the configured pipeline. Request metadata is copied in
    /// and the ETag bytes are stored under `metadata["ETAG"]` as a UTF-8 lossy
    /// string. All timestamps are 0 (no clock; see module docs).
    async fn commit_key(
        &self,
        req: Request<pb::CommitKeyRequest>,
    ) -> Result<Response<pb::CommitKeyResponse>, Status> {
        let req = req.into_inner();
        let tuple = key_tuple(&req.vbk)?;

        // Persist user metadata (Content-Type, x-amz-meta-*), then record the ETag.
        let mut metadata: HashMap<String, String> = req.metadata;
        metadata.insert(
            ETAG_METADATA_KEY.to_string(),
            String::from_utf8_lossy(&req.etag).into_owned(),
        );

        let info = pb::OmKeyInfoLite {
            vbk: req.vbk,
            data_size: req.final_size,
            creation_time_ms: 0,
            modification_time_ms: 0,
            locations: req.final_locations,
            metadata,
            ec_config: Some(self.ec()),
        };
        self.state.lock().keys.insert(tuple, info);
        Ok(Response::new(pb::CommitKeyResponse { commit_time_ms: 0 }))
    }

    /// Delete a key. Idempotent: succeeds whether or not the key existed.
    async fn delete_key(
        &self,
        req: Request<pb::DeleteKeyRequest>,
    ) -> Result<Response<pb::DeleteKeyResponse>, Status> {
        let tuple = key_tuple(&req.into_inner().vbk)?;
        self.state.lock().keys.remove(&tuple);
        Ok(Response::new(pb::DeleteKeyResponse {}))
    }

    /// Server-side copy: clone the source key's metadata under `dest`.
    ///
    /// Returns `NotFound` if the source is absent. The destination inherits the
    /// source's size, locations, ec_config, and metadata (ETag included); only
    /// its `vbk` is rewritten to `dest`. The response carries the copied size
    /// and the source ETag bytes (decoded back from `metadata["ETAG"]`).
    async fn copy_key(
        &self,
        req: Request<pb::CopyKeyRequest>,
    ) -> Result<Response<pb::CopyKeyResponse>, Status> {
        let req = req.into_inner();
        let src_tuple = key_tuple(&req.source)?;
        let dest = req
            .dest
            .ok_or_else(|| Status::invalid_argument("missing dest"))?;
        let dest_tuple = (
            dest.volume_name.clone(),
            dest.bucket_name.clone(),
            dest.key_name.clone(),
        );

        let mut st = self.state.lock();
        let mut info = st
            .keys
            .get(&src_tuple)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("source key not found: {}", src_tuple.2)))?;
        let size = info.data_size;
        let etag = info
            .metadata
            .get(ETAG_METADATA_KEY)
            .map(|s| s.as_bytes().to_vec())
            .unwrap_or_default();
        info.vbk = Some(dest);
        st.keys.insert(dest_tuple, info);

        Ok(Response::new(pb::CopyKeyResponse { size, etag }))
    }

    /// Initiate a multipart upload: mint a unique `upload_id` and return it.
    ///
    /// The id is `upload-{n}` where `n` is a monotonic counter (first call
    /// yields `upload-1`). Nothing else is persisted: the request's `vbk` is not
    /// recorded here, and no part state is created. The gateway is the authority
    /// for the in-flight upload from this point until `complete`/`abort` (see
    /// module docs).
    async fn initiate_multipart_upload(
        &self,
        _req: Request<pb::InitiateMultipartUploadRequest>,
    ) -> Result<Response<pb::InitiateMultipartUploadResponse>, Status> {
        let n = {
            let mut st = self.state.lock();
            let n = st.next_upload_id;
            st.next_upload_id += 1;
            n
        };
        Ok(Response::new(pb::InitiateMultipartUploadResponse {
            upload_id: format!("upload-{n}"),
        }))
    }

    /// Abort a multipart upload: no-op success.
    ///
    /// Part data lives on the datanodes and the part registry lives in the
    /// gateway; the OM holds no per-part state to discard. Cleaning up the
    /// orphaned part blocks is the gateway's responsibility, so the OM simply
    /// acknowledges the abort. Always succeeds, even for an unknown `upload_id`.
    async fn abort_multipart_upload(
        &self,
        _req: Request<pb::AbortMultipartUploadRequest>,
    ) -> Result<Response<pb::AbortMultipartUploadResponse>, Status> {
        Ok(Response::new(pb::AbortMultipartUploadResponse {}))
    }

    /// Complete a multipart upload: stitch the gateway-supplied parts into one
    /// committed key.
    ///
    /// The gateway sends the authoritative part list, each [`pb::Part`] carrying
    /// its `part_number`, the 16-byte BINARY MD5 of that part's bytes in `etag`
    /// (NOT hex), the part's byte `size`, and the part's block-group
    /// `locations`. From these we build the final key:
    /// - Parts are sorted by `part_number` ascending (defensive; the gateway may
    ///   already sort).
    /// - `final_locations` = every part's `locations` concatenated in sorted
    ///   order.
    /// - `final_size` = the sum of the part `size`s.
    /// - The object ETag is the AWS-compliant multipart form:
    ///   `hex(md5(concat(part.etag for each sorted part))) + "-" + N`, where the
    ///   concatenation is over the raw 16-byte binary part digests, the outer
    ///   MD5 is lowercase-hex-encoded, and `N` is the part count. This string is
    ///   stored under `metadata["ETAG"]` (mirroring `commit_key`) and returned
    ///   as bytes.
    ///
    /// Returns `InvalidArgument("no parts")` if `parts` is empty (a complete
    /// with zero parts is an S3-level client error). All timestamps are 0.
    async fn complete_multipart_upload(
        &self,
        req: Request<pb::CompleteMultipartUploadRequest>,
    ) -> Result<Response<pb::CompleteMultipartUploadResponse>, Status> {
        let req = req.into_inner();
        let tuple = key_tuple(&req.vbk)?;

        let mut parts = req.parts;
        if parts.is_empty() {
            return Err(Status::invalid_argument("no parts"));
        }
        parts.sort_by_key(|p| p.part_number);

        let final_size: u64 = parts.iter().map(|p| p.size).sum();
        let final_locations: Vec<pb::KeyLocation> =
            parts.iter().flat_map(|p| p.locations.clone()).collect();

        // AWS-compliant multipart ETag: MD5 over the concatenated raw 16-byte
        // per-part binary digests, hex-encoded lowercase, suffixed with "-N".
        let mut hasher = Md5::new();
        for p in &parts {
            hasher.update(&p.etag);
        }
        let digest = hasher.finalize();
        let etag = format!("{:x}-{}", digest, parts.len());

        let mut metadata: HashMap<String, String> = HashMap::new();
        metadata.insert(ETAG_METADATA_KEY.to_string(), etag.clone());

        let info = pb::OmKeyInfoLite {
            vbk: req.vbk,
            data_size: final_size,
            creation_time_ms: 0,
            modification_time_ms: 0,
            locations: final_locations,
            metadata,
            ec_config: Some(self.ec()),
        };
        self.state.lock().keys.insert(tuple, info);

        Ok(Response::new(pb::CompleteMultipartUploadResponse {
            etag: etag.into_bytes(),
            final_size,
        }))
    }

    /// List parts of an in-flight upload: always empty.
    ///
    /// The gateway serves ListParts from its own in-flight registry; this OM
    /// fixture keeps no per-part state (see module docs), so there is nothing to
    /// enumerate. Returns an empty, untruncated page.
    async fn list_parts(
        &self,
        _req: Request<pb::ListPartsRequest>,
    ) -> Result<Response<pb::ListPartsResponse>, Status> {
        Ok(Response::new(pb::ListPartsResponse {
            parts: vec![],
            next_part_number_marker: 0,
            is_truncated: false,
        }))
    }

    /// List in-flight multipart uploads: always empty.
    ///
    /// The OM does not register initiated uploads in this fixture (initiate only
    /// mints an id), so there is nothing to list. Returns an empty, untruncated
    /// page.
    async fn list_multipart_uploads(
        &self,
        _req: Request<pb::ListMultipartUploadsRequest>,
    ) -> Result<Response<pb::ListMultipartUploadsResponse>, Status> {
        Ok(Response::new(pb::ListMultipartUploadsResponse {
            uploads: vec![],
            next_key_marker: String::new(),
            next_upload_id_marker: String::new(),
            is_truncated: false,
        }))
    }
}

/// Build a [`pb::DatanodeDetails`] for tests. `ip_address` is set to `host`,
/// and `cert_serial` is left empty (this fake issues no certs).
pub fn datanode_details(uuid: &str, host: &str, gateway_port: u32) -> pb::DatanodeDetails {
    pb::DatanodeDetails {
        uuid: uuid.to_string(),
        ip_address: host.to_string(),
        host_name: host.to_string(),
        gateway_port,
        cert_serial: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 3+2 RS pipeline with five distinct datanodes (slots 1..=5).
    fn test_pipeline() -> PipelineConfig {
        let ec = dn::EcReplicationConfig {
            data: 3,
            parity: 2,
            ec_chunk_size: 1024 * 1024,
            codec: "RS".to_string(),
        };
        let datanodes = (0..5)
            .map(|i| datanode_details(&format!("dn-{i}"), &format!("host-{i}"), 9000 + i as u32))
            .collect();
        PipelineConfig::new(datanodes, ec)
    }

    fn vbk(key: &str) -> pb::VolumeBucketKey {
        pb::VolumeBucketKey {
            volume_name: "vol".to_string(),
            bucket_name: "bucket".to_string(),
            key_name: key.to_string(),
        }
    }

    #[tokio::test]
    async fn head_bucket_reports_object_store() {
        let om = FakeOm::new(test_pipeline());
        let resp = om
            .head_bucket(Request::new(pb::HeadBucketRequest {
                volume_name: "vol".to_string(),
                bucket_name: "bucket".to_string(),
                auth: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.exists);
        assert_eq!(resp.bucket_layout, "OBJECT_STORE");
        let ec = resp.default_ec_config.unwrap();
        assert_eq!((ec.data, ec.parity), (3, 2));
    }

    #[tokio::test]
    async fn create_key_returns_client_id_and_pipeline() {
        let om = FakeOm::new(test_pipeline());
        let resp = om
            .create_key(Request::new(pb::CreateKeyRequest {
                vbk: Some(vbk("obj")),
                expected_size: 0,
                ec_config: None,
                metadata: HashMap::new(),
                auth: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.client_id, 1);
        assert_eq!(resp.open_version, 1);
        assert_eq!(resp.pre_allocated_blocks.len(), 1);

        let loc = &resp.pre_allocated_blocks[0];
        let bid = loc.block_id.as_ref().unwrap();
        assert_eq!(bid.container_id, 1);
        assert_eq!(bid.replica_index, 0, "allocated id is the block-group id");

        let pipe = loc.pipeline.as_ref().unwrap();
        assert_eq!(pipe.members.len(), 5, "k+p members");
        // Positional mapping: dn-i holds slot i+1.
        for i in 0..5u32 {
            let uuid = format!("dn-{i}");
            assert_eq!(pipe.member_replica_indexes.get(&uuid), Some(&(i + 1)));
        }
    }

    #[tokio::test]
    async fn allocate_block_local_ids_are_monotonic() {
        let om = FakeOm::new(test_pipeline());
        let mut last = 0u64;
        for _ in 0..4 {
            let resp = om
                .allocate_block(Request::new(pb::AllocateBlockRequest {
                    client_id: 1,
                    open_version: 1,
                    vbk: Some(vbk("obj")),
                    exclude_dn_uuids: Vec::new(),
                    auth: None,
                }))
                .await
                .unwrap()
                .into_inner();
            let id = resp.new_block.unwrap().block_id.unwrap().local_id;
            assert!(id > last, "local_id {id} must exceed previous {last}");
            last = id;
        }
    }

    #[tokio::test]
    async fn commit_then_lookup_round_trips() {
        let om = FakeOm::new(test_pipeline());
        let loc = om.alloc_block();
        om.commit_key(Request::new(pb::CommitKeyRequest {
            client_id: 1,
            open_version: 1,
            vbk: Some(vbk("obj")),
            final_size: 4096,
            final_locations: vec![loc],
            etag: b"deadbeef".to_vec(),
            metadata: HashMap::new(),
            auth: None,
        }))
        .await
        .unwrap();

        let resp = om
            .lookup_key(Request::new(pb::LookupKeyRequest {
                vbk: Some(vbk("obj")),
                auth: None,
            }))
            .await
            .unwrap()
            .into_inner();
        let info = resp.key_info.unwrap();
        assert_eq!(info.data_size, 4096);
        assert_eq!(info.locations.len(), 1);
        assert_eq!(
            info.metadata.get("ETAG").map(String::as_str),
            Some("deadbeef")
        );
    }

    #[tokio::test]
    async fn lookup_missing_key_is_not_found() {
        let om = FakeOm::new(test_pipeline());
        let err = om
            .lookup_key(Request::new(pb::LookupKeyRequest {
                vbk: Some(vbk("nope")),
                auth: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    /// Minimal `KeyLocation` for the OM-side multipart tests. The OM only stores
    /// locations verbatim (it does not inspect them), so a bare block-group id
    /// with no pipeline/token is enough. `local_id` distinguishes locations so
    /// concatenation can be observed.
    fn part_location(local_id: u64) -> pb::KeyLocation {
        pb::KeyLocation {
            block_id: Some(dn::BlockId {
                container_id: 1,
                local_id,
                replica_index: 0,
            }),
            offset: 0,
            length: 0,
            pipeline: None,
            block_token: Vec::new(),
        }
    }

    #[tokio::test]
    async fn initiate_returns_unique_upload_ids() {
        let om = FakeOm::new(test_pipeline());
        let req = || {
            Request::new(pb::InitiateMultipartUploadRequest {
                vbk: Some(vbk("obj")),
                ec_config: None,
                metadata: HashMap::new(),
                auth: None,
            })
        };
        let first = om
            .initiate_multipart_upload(req())
            .await
            .unwrap()
            .into_inner()
            .upload_id;
        let second = om
            .initiate_multipart_upload(req())
            .await
            .unwrap()
            .into_inner()
            .upload_id;
        assert_ne!(first, second, "each initiate must mint a fresh upload id");
        assert_eq!(first, "upload-1");
        assert_eq!(second, "upload-2");
    }

    #[tokio::test]
    async fn complete_multipart_builds_key_with_dash_suffix_etag() {
        let om = FakeOm::new(test_pipeline());
        let upload_id = om
            .initiate_multipart_upload(Request::new(pb::InitiateMultipartUploadRequest {
                vbk: Some(vbk("obj")),
                ec_config: None,
                metadata: HashMap::new(),
                auth: None,
            }))
            .await
            .unwrap()
            .into_inner()
            .upload_id;

        // Two fabricated parts. Binary 16-byte etags (chosen, not real MD5s),
        // distinct sizes, and one location each so concatenation is observable.
        let parts = vec![
            pb::Part {
                part_number: 1,
                etag: vec![0xAA; 16],
                size: 5_000,
                locations: vec![part_location(10)],
            },
            pb::Part {
                part_number: 2,
                etag: vec![0xBB; 16],
                size: 1_234,
                locations: vec![part_location(20)],
            },
        ];
        let total_size: u64 = parts.iter().map(|p| p.size).sum();
        let total_locations: usize = parts.iter().map(|p| p.locations.len()).sum();

        // Independently recompute the AWS multipart ETag the impl should return.
        let mut hasher = Md5::new();
        hasher.update([0xAA; 16]);
        hasher.update([0xBB; 16]);
        let expected_etag = format!("{:x}-2", hasher.finalize());

        let resp = om
            .complete_multipart_upload(Request::new(pb::CompleteMultipartUploadRequest {
                vbk: Some(vbk("obj")),
                upload_id,
                parts,
                auth: None,
            }))
            .await
            .unwrap()
            .into_inner();

        let etag_str = String::from_utf8(resp.etag).unwrap();
        assert!(
            etag_str.ends_with("-2"),
            "multipart etag must carry the part-count suffix: {etag_str}"
        );
        assert_eq!(etag_str, expected_etag);
        assert_eq!(resp.final_size, total_size);

        // The completed object must be looked-up-able with stitched locations
        // and the multipart etag in metadata.
        let info = om
            .lookup_key(Request::new(pb::LookupKeyRequest {
                vbk: Some(vbk("obj")),
                auth: None,
            }))
            .await
            .unwrap()
            .into_inner()
            .key_info
            .unwrap();
        assert_eq!(info.data_size, total_size);
        assert_eq!(info.locations.len(), total_locations);
        assert_eq!(info.metadata.get("ETAG"), Some(&etag_str));
    }

    #[tokio::test]
    async fn complete_with_no_parts_is_invalid() {
        let om = FakeOm::new(test_pipeline());
        let err = om
            .complete_multipart_upload(Request::new(pb::CompleteMultipartUploadRequest {
                vbk: Some(vbk("obj")),
                upload_id: "upload-1".to_string(),
                parts: vec![],
                auth: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }
}
