//! In-memory fake Ozone Manager implementing [`OmRustGatewayService`].
//!
//! This fixture lets S3-gateway integration tests exercise the OM control
//! plane (create/allocate/commit/lookup/list/copy/delete) plus multipart
//! upload (initiate/complete/abort/list-parts/list-uploads) without a real Java
//! OM. See the per-RPC docs below for the exact contract.
//!
//! # Multipart model: the OM is the authority for in-flight uploads
//! The OM persists part records (in [`State::uploads`]) so the gateway holds NO
//! in-flight multipart state and any gateway replica can drive the upload:
//! - `initiate` mints a unique `upload_id` and persists a [`StoredUpload`]
//!   (capturing the `vbk` + an initiated timestamp).
//! - `commit_multipart_part` upserts one part's `(etag, size, locations)` under
//!   its part number (last writer wins on a re-upload). The gateway calls this
//!   after it has EC-written the part's data to the datanodes.
//! - `complete` looks up the upload's stored parts in the client's order,
//!   validates them (ascending/no-dup, all present, every non-last part
//!   `>= 5 MiB`), stitches one committed key (concatenated locations, summed
//!   size, AWS-style multipart ETag), and removes the upload. This is the only
//!   multipart RPC that mutates `keys`.
//! - `abort` removes the upload (idempotent; unknown ids succeed). The
//!   already-written part blocks are reclaimed by container GC, out of scope.
//! - `list_parts` / `list_multipart_uploads` read the stored state.
//!
//! The single coarse `State` mutex serializes every part upsert/complete/abort,
//! which is stronger than required: a real OM needs only per-`upload_id`
//! linearizability (a single-writer Raft group), not a global lock, so neither
//! the proto nor the tests bake in a global-lock assumption.
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
//! - No wall clock is wired into this fixture: `commit_time_ms` and bucket
//!   creation times are `0`, and committed object keys use a FIXED
//!   `creation_time_ms`/`modification_time_ms` ([`FAKE_OBJECT_TIME_MS`]). The
//!   fixed object time is deterministic (so timestamp assertions are stable) yet
//!   non-zero, so the gateway's Last-Modified header and date-conditional request
//!   handling (If-Modified-Since / If-Unmodified-Since) are actually exercisable.
//!
//! # ETag handling
//! `commit_key` receives the gateway-computed ETag as raw bytes. We store it in
//! the key's `metadata` under the literal key `"ETAG"` as a UTF-8 *lossy*
//! string (matching how the real OM surfaces it via `OmKeyInfoLite.metadata`),
//! and we also merge any request-supplied metadata.

use std::collections::{BTreeMap, HashMap};
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

/// Fixed object creation/modification time (2021-01-01T00:00:00Z in ms). A real
/// OM stamps wall-clock time; the fake uses a constant so Last-Modified and the
/// date-conditional request paths (If-Modified-Since / If-Unmodified-Since) are
/// deterministic and actually exercisable (a zero time would make every object
/// look epoch-old).
const FAKE_OBJECT_TIME_MS: u64 = 1_609_459_200_000;

/// Minimum size of every multipart part except the last (S3 `EntityTooSmall`).
/// The OM is the authority for this check now that it holds the part sizes.
const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;
/// Inclusive upper bound on multipart part numbers (S3 allows 1..=10000).
const MAX_PART_NUMBER: u32 = 10_000;

/// One uploaded part's record, persisted in the OM (see [`StoredUpload`]).
struct StoredPart {
    etag_binary: Vec<u8>,
    etag_hex: String,
    size: u64,
    locations: Vec<pb::KeyLocation>,
}

/// An in-flight multipart upload, persisted in the OM so any gateway replica can
/// commit parts, complete, abort, or list. Parts are keyed by part number in a
/// `BTreeMap`, so `list_parts`/`complete` see them in ascending order for free.
struct StoredUpload {
    vbk: pb::VolumeBucketKey,
    initiated_ms: u64,
    parts: BTreeMap<u32, StoredPart>,
}

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
    /// `initiate_multipart_upload`; first call returns `upload-1`.
    next_upload_id: u64,
    /// In-flight multipart uploads, keyed by `upload_id`. The OM is the authority
    /// for part records (see module docs): `initiate` inserts, `commit_multipart_part`
    /// upserts a part, `complete`/`abort` remove the entry.
    uploads: HashMap<String, StoredUpload>,
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
                uploads: HashMap::new(),
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

    /// List keys under `prefix`, optionally folding `delimiter`, with real
    /// pagination over `max_keys` / `continuation_token`.
    ///
    /// Builds the delimiter-folded sequence of listing entries — each entry is
    /// either a key or a folded `common_prefix` (the substring of `key_name`
    /// after `prefix` up to and including the first delimiter) — sorted by listing
    /// name with prefixes deduplicated. Pagination is over this MERGED sequence so
    /// the continuation token is stable across keys and prefixes, and both keys
    /// and common prefixes count toward `max_keys` (matching S3 `KeyCount`). The
    /// continuation token is the listing name of the last entry returned on the
    /// previous page; the next page resumes strictly after it. Emitting real
    /// `is_truncated` / `next_continuation_token` lets the gateway's pagination be
    /// exercised — re-faking it (one untruncated page) would mask that path.
    async fn list_keys(
        &self,
        req: Request<pb::ListKeysRequest>,
    ) -> Result<Response<Self::ListKeysStream>, Status> {
        let req = req.into_inner();
        let max_keys = req.max_keys as usize;

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

        // Merged entry sequence: (listing_name, Some(key) | None=common-prefix).
        let mut entries: Vec<(String, Option<pb::OmKeyInfoLite>)> = Vec::new();
        let mut seen_prefix: std::collections::HashSet<String> = std::collections::HashSet::new();
        for info in matching {
            let key_name = info.vbk.as_ref().map(|v| v.key_name.as_str()).unwrap_or("");
            if req.delimiter.is_empty() {
                entries.push((key_name.to_string(), Some(info.clone())));
                continue;
            }
            let remainder = &key_name[req.prefix.len()..];
            match remainder.find(&req.delimiter) {
                Some(idx) => {
                    let end = idx + req.delimiter.len();
                    let common = format!("{}{}", req.prefix, &remainder[..end]);
                    if seen_prefix.insert(common.clone()) {
                        entries.push((common, None));
                    }
                }
                None => entries.push((key_name.to_string(), Some(info.clone()))),
            }
        }
        drop(state);
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        // Resume strictly after the continuation token.
        if !req.continuation_token.is_empty() {
            let token = req.continuation_token.clone();
            entries.retain(|(name, _)| name.as_str() > token.as_str());
        }

        // Truncate to max_keys; the token is the last returned entry's name. A
        // max_keys of 0 is NOT "truncated": there is no last entry to use as a
        // cursor, so reporting truncated-with-no-token would livelock a paginating
        // client (AWS returns KeyCount=0, IsTruncated=false here).
        let (is_truncated, next_continuation_token) = if max_keys == 0 {
            entries.clear();
            (false, String::new())
        } else if entries.len() > max_keys {
            let token = entries[max_keys - 1].0.clone();
            entries.truncate(max_keys);
            (true, token)
        } else {
            (false, String::new())
        };

        let mut keys = Vec::new();
        let mut common_prefixes = Vec::new();
        for (name, info) in entries {
            match info {
                Some(k) => keys.push(k),
                None => common_prefixes.push(name),
            }
        }

        let resp = pb::ListKeysResponse {
            keys,
            common_prefixes,
            next_continuation_token,
            is_truncated,
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
            creation_time_ms: FAKE_OBJECT_TIME_MS,
            modification_time_ms: FAKE_OBJECT_TIME_MS,
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

    /// Replace the object's full tag set. Tags are stored in the key metadata
    /// under an `x-amz-tag-<key>` prefix (so they never collide with user
    /// `x-amz-meta-*` metadata). NotFound if the key is absent.
    async fn put_object_tagging(
        &self,
        req: Request<pb::PutObjectTaggingRequest>,
    ) -> Result<Response<pb::PutObjectTaggingResponse>, Status> {
        let req = req.into_inner();
        let tuple = key_tuple(&req.vbk)?;
        let mut st = self.state.lock();
        let info = st
            .keys
            .get_mut(&tuple)
            .ok_or_else(|| Status::not_found(format!("key not found: {}", tuple.2)))?;
        info.metadata.retain(|k, _| !k.starts_with("x-amz-tag-"));
        for tag in req.tags {
            info.metadata.insert(format!("x-amz-tag-{}", tag.key), tag.value);
        }
        Ok(Response::new(pb::PutObjectTaggingResponse {}))
    }

    /// Initiate a multipart upload: mint a unique `upload_id` and persist the
    /// upload so later parts/complete/list can find it.
    ///
    /// The id is `upload-{n}` where `n` is a monotonic counter (first call yields
    /// `upload-1`). A [`StoredUpload`] is recorded under that id, capturing the
    /// request's `vbk` and an initiated timestamp, with an empty part map. A
    /// missing `vbk` is `InvalidArgument`. The OM is the authority for the
    /// in-flight upload until `complete`/`abort` (see module docs).
    async fn initiate_multipart_upload(
        &self,
        req: Request<pb::InitiateMultipartUploadRequest>,
    ) -> Result<Response<pb::InitiateMultipartUploadResponse>, Status> {
        let req = req.into_inner();
        let vbk = req
            .vbk
            .ok_or_else(|| Status::invalid_argument("missing vbk"))?;
        let mut st = self.state.lock();
        let n = st.next_upload_id;
        st.next_upload_id += 1;
        let upload_id = format!("upload-{n}");
        st.uploads.insert(
            upload_id.clone(),
            StoredUpload {
                vbk,
                initiated_ms: FAKE_OBJECT_TIME_MS,
                parts: BTreeMap::new(),
            },
        );
        Ok(Response::new(pb::InitiateMultipartUploadResponse {
            upload_id,
            initiated_ms: FAKE_OBJECT_TIME_MS,
        }))
    }

    /// Persist one uploaded part under its upload. Upsert: a re-uploaded part
    /// number overwrites the prior record (last writer wins). `NotFound` if the
    /// upload is unknown; `InvalidArgument` if the part number is out of range
    /// (the OM is the writer of record, so it enforces the bound authoritatively).
    async fn commit_multipart_part(
        &self,
        req: Request<pb::CommitMultipartPartRequest>,
    ) -> Result<Response<pb::CommitMultipartPartResponse>, Status> {
        let req = req.into_inner();
        if !(1..=MAX_PART_NUMBER).contains(&req.part_number) {
            return Err(Status::invalid_argument(format!(
                "part number {} out of range (1..={MAX_PART_NUMBER})",
                req.part_number
            )));
        }
        let mut st = self.state.lock();
        let upload = st
            .uploads
            .get_mut(&req.upload_id)
            .ok_or_else(|| Status::not_found(format!("no such upload: {}", req.upload_id)))?;
        upload.parts.insert(
            req.part_number,
            StoredPart {
                etag_binary: req.etag_binary,
                etag_hex: req.etag_hex,
                size: req.size,
                locations: req.locations,
            },
        );
        Ok(Response::new(pb::CommitMultipartPartResponse {}))
    }

    /// Abort a multipart upload: remove its stored record. Idempotent — aborting
    /// an unknown `upload_id` is a no-op success (S3 abort is lenient). The
    /// already-written part blocks are reclaimed by container GC (out of scope).
    async fn abort_multipart_upload(
        &self,
        req: Request<pb::AbortMultipartUploadRequest>,
    ) -> Result<Response<pb::AbortMultipartUploadResponse>, Status> {
        self.state.lock().uploads.remove(&req.into_inner().upload_id);
        Ok(Response::new(pb::AbortMultipartUploadResponse {}))
    }

    /// Complete a multipart upload from the OM's own stored parts, in the
    /// client-supplied order, then remove the upload.
    ///
    /// Validation (all `InvalidArgument` except an unknown upload, which is
    /// `NotFound`): non-empty; strictly ascending with no duplicates (defense in
    /// depth — the gateway also checks the inbound XML order); every named part
    /// must have been uploaded; every non-last part must be `>= 5 MiB`
    /// (`EntityTooSmall`, authoritative here since the OM owns the sizes). The
    /// final key concatenates each part's `locations` in order, sums the sizes,
    /// and its ETag is the AWS multipart form `hex(md5(concat of the raw 16-byte
    /// per-part digests)) + "-N"`, stored under `metadata["ETAG"]`.
    async fn complete_multipart_upload(
        &self,
        req: Request<pb::CompleteMultipartUploadRequest>,
    ) -> Result<Response<pb::CompleteMultipartUploadResponse>, Status> {
        let req = req.into_inner();
        let tuple = key_tuple(&req.vbk)?;
        let order = req.ordered_part_numbers;
        if order.is_empty() {
            return Err(Status::invalid_argument("no parts"));
        }
        if order.windows(2).any(|w| w[0] >= w[1]) {
            return Err(Status::invalid_argument(
                "parts must be ascending with no duplicates",
            ));
        }

        let mut st = self.state.lock();
        let upload = st
            .uploads
            .get(&req.upload_id)
            .ok_or_else(|| Status::not_found(format!("no such upload: {}", req.upload_id)))?;

        let last_idx = order.len() - 1;
        let mut final_size = 0u64;
        let mut final_locations: Vec<pb::KeyLocation> = Vec::new();
        let mut hasher = Md5::new();
        for (idx, pn) in order.iter().enumerate() {
            let part = upload
                .parts
                .get(pn)
                .ok_or_else(|| Status::invalid_argument(format!("part {pn} was not uploaded")))?;
            if idx != last_idx && part.size < MIN_PART_SIZE {
                return Err(Status::invalid_argument(format!(
                    "part {pn} ({} bytes) is smaller than the {MIN_PART_SIZE}-byte minimum",
                    part.size
                )));
            }
            final_size += part.size;
            final_locations.extend(part.locations.iter().cloned());
            hasher.update(&part.etag_binary);
        }
        let etag = format!("{:x}-{}", hasher.finalize(), order.len());

        let mut metadata: HashMap<String, String> = HashMap::new();
        metadata.insert(ETAG_METADATA_KEY.to_string(), etag.clone());
        let info = pb::OmKeyInfoLite {
            vbk: req.vbk,
            data_size: final_size,
            creation_time_ms: FAKE_OBJECT_TIME_MS,
            modification_time_ms: FAKE_OBJECT_TIME_MS,
            locations: final_locations,
            metadata,
            ec_config: Some(self.ec()),
        };
        // The `upload` borrow ended above; now mutate the map.
        st.uploads.remove(&req.upload_id);
        st.keys.insert(tuple, info);

        Ok(Response::new(pb::CompleteMultipartUploadResponse {
            etag: etag.into_bytes(),
            final_size,
        }))
    }

    /// List the stored parts of an in-flight upload in ascending part order.
    /// `NotFound` if the upload is unknown (matching the gateway's NoSuchUpload).
    /// `part_number_marker` excludes parts at or below it; `max_parts` is ignored
    /// (the gateway never paginates), so a single untruncated page is returned.
    async fn list_parts(
        &self,
        req: Request<pb::ListPartsRequest>,
    ) -> Result<Response<pb::ListPartsResponse>, Status> {
        let req = req.into_inner();
        let st = self.state.lock();
        let upload = st
            .uploads
            .get(&req.upload_id)
            .ok_or_else(|| Status::not_found(format!("no such upload: {}", req.upload_id)))?;
        let parts: Vec<pb::Part> = upload
            .parts
            .iter()
            .filter(|(pn, _)| **pn > req.part_number_marker)
            .map(|(pn, p)| pb::Part {
                part_number: *pn,
                etag: p.etag_binary.clone(),
                size: p.size,
                locations: p.locations.clone(),
                etag_hex: p.etag_hex.clone(),
            })
            .collect();
        Ok(Response::new(pb::ListPartsResponse {
            parts,
            next_part_number_marker: 0,
            is_truncated: false,
        }))
    }

    /// List in-flight uploads for a `(volume, bucket)` (filtered by `prefix` on
    /// the key), sorted by key then upload id. `max_uploads` is ignored (single
    /// untruncated page).
    async fn list_multipart_uploads(
        &self,
        req: Request<pb::ListMultipartUploadsRequest>,
    ) -> Result<Response<pb::ListMultipartUploadsResponse>, Status> {
        let req = req.into_inner();
        let st = self.state.lock();
        let mut uploads: Vec<pb::list_multipart_uploads_response::Upload> = st
            .uploads
            .iter()
            .filter(|(_, u)| {
                u.vbk.volume_name == req.volume_name
                    && u.vbk.bucket_name == req.bucket_name
                    && u.vbk.key_name.starts_with(&req.prefix)
            })
            .map(|(id, u)| pb::list_multipart_uploads_response::Upload {
                vbk: Some(u.vbk.clone()),
                upload_id: id.clone(),
                initiated_ms: u.initiated_ms,
            })
            .collect();
        uploads.sort_by(|a, b| {
            let ak = a.vbk.as_ref().map(|v| v.key_name.as_str()).unwrap_or("");
            let bk = b.vbk.as_ref().map(|v| v.key_name.as_str()).unwrap_or("");
            ak.cmp(bk).then_with(|| a.upload_id.cmp(&b.upload_id))
        });
        Ok(Response::new(pb::ListMultipartUploadsResponse {
            uploads,
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

        // Two parts, committed to the OM first. Part 1 (non-last) must meet the
        // 5 MiB minimum the OM now enforces; part 2 (last) may be small. Binary
        // 16-byte etags chosen (not real MD5s), one location each so concatenation
        // is observable.
        let part1_size: u64 = 5 * 1024 * 1024;
        let part2_size: u64 = 1_234;
        let total_size = part1_size + part2_size;
        let total_locations = 2usize;
        for (pn, byte, size, loc) in [(1u32, 0xAAu8, part1_size, 10u64), (2, 0xBB, part2_size, 20)] {
            om.commit_multipart_part(Request::new(pb::CommitMultipartPartRequest {
                vbk: Some(vbk("obj")),
                upload_id: upload_id.clone(),
                part_number: pn,
                etag_binary: vec![byte; 16],
                etag_hex: String::new(),
                size,
                locations: vec![part_location(loc)],
                auth: None,
            }))
            .await
            .unwrap();
        }

        // Independently recompute the AWS multipart ETag the impl should return.
        let mut hasher = Md5::new();
        hasher.update([0xAA; 16]);
        hasher.update([0xBB; 16]);
        let expected_etag = format!("{:x}-2", hasher.finalize());

        let resp = om
            .complete_multipart_upload(Request::new(pb::CompleteMultipartUploadRequest {
                vbk: Some(vbk("obj")),
                upload_id,
                ordered_part_numbers: vec![1, 2],
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
                ordered_part_numbers: vec![],
                auth: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }
}
