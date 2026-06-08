//! A compliant fake Ozone Manager speaking the REAL Apache Ozone
//! `OzoneManagerService.submitRequest(OMRequest) -> OMResponse` contract
//! (vendored `ozone_grpc_types::hadoop::ozone`), the OM-side COMPLIANCE harness
//! for the Rust S3 gateway's `OzoneOmClient`.
//!
//! This mirrors the bespoke [`crate::fake_om::FakeOm`] in-memory semantics — a
//! bucket set, a key map keyed by `(volume, bucket, key)`, monotonic
//! `local_id`/`client_id` counters, and exactly ONE statically configured EC
//! pipeline where `datanodes[i]` holds EC slot `i + 1` — but emits the ACTUAL OM
//! wire shapes (`OmRequest`/`OmResponse` multiplexed by `cmdType`, `KeyInfo`,
//! `KeyLocation`, `hdds::Pipeline`) instead of the bespoke `om::gw::v1` ones.
//!
//! # What this fixture is and is NOT
//! - It is enough OM to drive the gateway's CORE path: bucket head/create/delete/
//!   list and the key write/read/delete cycle (`CreateKey` -> `AllocateBlock`* ->
//!   `CommitKey`, then `GetKeyInfo` / `DeleteKey`). Multipart and `ListKeys` are
//!   OUT OF SCOPE here and return `INVALID_REQUEST` like any other unimplemented
//!   `cmdType` (no panic).
//! - It does NO auth checking: with Ozone security off the gateway attests the
//!   principal via `s3Authentication.accessId` and the OM trusts it. The fixture
//!   records the request verbatim so a test can assert the principal was sent,
//!   but it never rejects on auth.
//! - It performs NO placement, exclusion-list handling, or failover: every block
//!   lands on the one configured pipeline.
//!
//! # Invariants (mirrors of `FakeOm`'s, restated for the real proto)
//! - W3 EC slot fidelity: the emitted [`hdds::Pipeline`] carries
//!   `member_replica_indexes` PARALLEL to `members`, with
//!   `member_replica_indexes[i] == i + 1`, because `datanodes[i]` holds EC slot
//!   `i + 1`. A mismatch silently swaps data/parity shards on read.
//! - W2 commit lists what was written: `CommitKey` stores the request's
//!   `keyArgs.key_locations` VERBATIM (the actual written blocks/lengths), never
//!   a re-derived pre-allocation, so a later `GetKeyInfo` returns the real
//!   layout.
//! - Monotonic ids: `next_local_id` (block local id) and `next_client_id`
//!   (the `CreateKey` open-session id) each start at 1 and only increase;
//!   `next_container_id` starts at 1 and increases per `CreateKey` so distinct
//!   keys do not collide on `(container_id, local_id)`.

use std::collections::{BTreeMap, HashMap, HashSet};

use md5::{Digest, Md5};
use parking_lot::Mutex;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use ozone_grpc_types::hadoop::hdds as hdds;
use ozone_grpc_types::hadoop::ozone as oz;
use oz::ozone_manager_service_server::{OzoneManagerService, OzoneManagerServiceServer};

/// Fixed pipeline id reported in every emitted [`hdds::Pipeline`]. A single
/// non-HA pipeline needs only a stable identifier.
const PIPELINE_ID: &str = "pl-1";

/// Fixed object creation/modification time (2021-01-01T00:00:00Z in ms). `KeyInfo`
/// marks `creation_time`/`modification_time` as proto2 `required`, so they MUST be
/// set; a constant non-zero value keeps timestamp-derived behavior (Last-Modified,
/// date-conditional requests) deterministic yet exercisable.
const FAKE_OBJECT_TIME_MS: u64 = 1_609_459_200_000;

/// Reserved metadata key the multipart-complete handler writes the AWS ETag to,
/// and the key `list_keys` reads it back from. Mirrors the `CommitKey` /
/// `GetKeyInfo` path's `metadata["ETAG"]` convention (see the gateway's
/// `OzoneOmClient`, which surfaces `etag` from this same entry).
const ETAG_METADATA_KEY: &str = "ETAG";

/// Metadata-key prefix under which S3 object tags are stored on a `KeyInfo`.
/// Mirrors the gateway's `backend::TAG_META_PREFIX`: tags round-trip through key
/// metadata so they never collide with user `x-amz-meta-*` entries or the
/// reserved [`ETAG_METADATA_KEY`]. The `GetObjectTagging` handler strips this
/// prefix back off; the gateway's metadata-based tagging therefore works
/// unchanged whether it reads tags via `GetObjectTagging` or via `GetKeyInfo`.
const TAG_META_PREFIX: &str = "x-amz-tag-";

/// Minimum size, in bytes, of every multipart part EXCEPT the last. The OM holds
/// the authoritative per-part sizes, so `CompleteMultiPartUpload` is where this
/// is enforced; an undersized non-last part is rejected with `ENTITY_TOO_SMALL`
/// (S3 `EntityTooSmall`).
const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// Inclusive bounds on a multipart part number (S3 permits `1..=10000`). A
/// `CommitMultiPartUpload` outside this range is rejected with `INVALID_REQUEST`.
const MAX_PART_NUMBER: u32 = 10_000;

/// Default page size for `ListKeys` when the request's `count` is absent or
/// non-positive. The gateway always supplies a positive `max_keys`, but a fake
/// must still answer a `count <= 0` request without returning the whole bucket.
const DEFAULT_LIST_KEYS_LIMIT: usize = 1000;

/// One uploaded part, as the OM remembers it between `CommitMultiPartUpload` and
/// `CompleteMultiPartUpload`/`ListMultiPartUploadParts`.
///
/// `etag_hex` is the part's MD5 as a lowercase hex string exactly as the gateway
/// sent it on `KeyArgs.metadata["ETAG"]`; `CompleteMultiPartUpload` decodes it
/// back to the raw 16 digest bytes for the AWS multipart-ETag rollup (see
/// [`hex_decode`]). `locations` are the part's committed blocks, preserved
/// VERBATIM (W2: block_id/offset/length untouched) so the stitched final key
/// reads the exact bytes that were written.
struct StoredPart {
    /// Committed blocks of this part, kept verbatim from the commit request.
    locations: Vec<oz::KeyLocation>,
    /// Part size in bytes (drives the `>= 5 MiB` non-last-part check).
    size: u64,
    /// The part's MD5 as lowercase hex (the gateway's per-part S3 ETag).
    etag_hex: String,
}

/// An in-flight multipart upload the OM owns from `InitiateMultiPartUpload`
/// until `CompleteMultiPartUpload`/`AbortMultiPartUpload`.
///
/// Parts are keyed by part number in a `BTreeMap` so `ListMultiPartUploadParts`
/// and `CompleteMultiPartUpload` iterate them in ascending order for free.
struct StoredUpload {
    /// `(volume, bucket, key)` the upload targets; the stitched key lands here.
    vbk: (String, String, String),
    /// Uploaded parts by part number; a re-commit of a part overwrites it.
    parts: BTreeMap<u32, StoredPart>,
}

/// Decode a lowercase/uppercase hex string into bytes, or `None` if it is not
/// valid hex (odd length or a non-hex nibble). The workspace pulls in no `hex`
/// crate, so this lives here. Used by `CompleteMultiPartUpload` to turn each
/// part's hex ETag back into the raw 16 digest bytes BEFORE the MD5 rollup —
/// feeding the hex text instead would compute a different, wrong ETag.
fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(s.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        out.push((hi * 16 + lo) as u8);
    }
    Some(out)
}

/// The one static EC pipeline a [`CompliantOm`] allocates from.
///
/// The datanode order is load-bearing: `datanodes[i]` holds EC replica slot
/// `i + 1` (1-based; data shards `1..=k`, parity `k+1..=k+p`). Each
/// [`hdds::DatanodeDetailsProto`] must carry a `REPLICATION` port (the name the
/// Rust datanode advertises to SCM in Track 2) whose value is that datanode's
/// data port, so the gateway can dial it for shard I/O.
pub struct CompliantOmPipeline {
    /// Ordered datanodes; position `i` (0-based) owns EC replica slot `i + 1`.
    pub datanodes: Vec<hdds::DatanodeDetailsProto>,
    /// Erasure-coding parameters of this pipeline (real wire form).
    pub ec: hdds::EcReplicationConfig,
}

/// Mutable in-memory OM state, guarded by a single `parking_lot::Mutex`.
///
/// A single coarse lock is intentional: this is a test fixture, contention is
/// irrelevant, and one lock keeps the dispatch handlers trivially correct under
/// concurrent gateway calls.
struct OmState {
    /// Buckets that exist, keyed by `(volume, bucket)`. Backs
    /// CreateBucket/InfoBucket/DeleteBucket/ListBuckets.
    buckets: HashSet<(String, String)>,
    /// Committed keys, addressed by `(volume, bucket, key)`. The stored
    /// [`oz::KeyInfo`] carries the ACTUAL committed locations (W2).
    keys: HashMap<(String, String, String), oz::KeyInfo>,
    /// Next block `local_id` to hand out; first allocation returns 1.
    next_local_id: i64,
    /// Next `CreateKey` open-session id (the response `id`); first call returns 1.
    next_client_id: u64,
    /// Next container id to hand out; bumped once per `CreateKey` so blocks of
    /// distinct keys do not collide on `(container_id, local_id)`. First key uses 1.
    next_container_id: i64,
    /// In-flight multipart uploads, keyed by upload id (`upload-N`). Backs the
    /// Initiate/Commit/Complete/Abort/ListParts/ListMultipartUploads handlers.
    uploads: HashMap<String, StoredUpload>,
    /// Next multipart upload-id ordinal; first `InitiateMultiPartUpload` yields
    /// `upload-1`. Monotonic, so distinct uploads never reuse an id.
    next_upload_id: u64,
}

/// A compliant fake OM. Transfer ownership to a tonic server via
/// [`CompliantOm::into_server`]; clone [`CompliantOm::record`] BEFORE that to
/// retain the inspection handle.
pub struct CompliantOm {
    /// Every [`oz::OmRequest`] received, in arrival order (decoded real proto).
    /// Lets a test assert the envelope the client built (cmdType, clientId,
    /// `s3Authentication.accessId`, the typed sub-request).
    record: Arc<Mutex<Vec<oz::OmRequest>>>,
    state: Mutex<OmState>,
    pipeline: CompliantOmPipeline,
}

impl CompliantOm {
    /// Create a fake OM that allocates every block from `pipeline`.
    pub fn new(pipeline: CompliantOmPipeline) -> Self {
        Self {
            record: Arc::new(Mutex::new(Vec::new())),
            state: Mutex::new(OmState {
                buckets: HashSet::new(),
                keys: HashMap::new(),
                next_local_id: 1,
                next_client_id: 1,
                next_container_id: 1,
                uploads: HashMap::new(),
                next_upload_id: 1,
            }),
            pipeline,
        }
    }

    /// Handle to inspect every [`oz::OmRequest`] the client sent, in arrival
    /// order. Clone BEFORE [`Self::into_server`] (which consumes `self`).
    pub fn record(&self) -> Arc<Mutex<Vec<oz::OmRequest>>> {
        self.record.clone()
    }

    /// Consume into a tonic server for `Server::add_service`.
    pub fn into_server(self) -> OzoneManagerServiceServer<Self> {
        OzoneManagerServiceServer::new(self)
    }

    /// Build the static pipeline message: the configured datanodes plus the
    /// parallel `member_replica_indexes` where index `i` is `i + 1`.
    ///
    /// W3: `member_replica_indexes` is PARALLEL to `members`, and
    /// `member_replica_indexes[i] == i + 1` because `datanodes[i]` holds EC slot
    /// `i + 1`. The gateway maps shard `s` to the member whose index is `s`.
    fn pipeline_message(&self) -> hdds::Pipeline {
        let n = self.pipeline.datanodes.len() as u32;
        hdds::Pipeline {
            members: self.pipeline.datanodes.clone(),
            member_replica_indexes: (1..=n).collect(),
            ec_replication_config: Some(self.pipeline.ec.clone()),
            r#type: Some(hdds::ReplicationType::Ec as i32),
            id: hdds::PipelineId {
                id: Some(PIPELINE_ID.to_string()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Allocate one block on the configured pipeline within container
    /// `container_id`, bumping `next_local_id`. `offset`/`length` are 0 (the OM
    /// pre-allocates the block; the gateway fills real lengths on commit).
    fn alloc_block(&self, container_id: i64) -> oz::KeyLocation {
        let local_id = {
            let mut st = self.state.lock();
            let id = st.next_local_id;
            st.next_local_id += 1;
            id
        };
        oz::KeyLocation {
            block_id: hdds::BlockId {
                container_block_id: hdds::ContainerBlockId {
                    container_id,
                    local_id,
                },
                ..Default::default()
            },
            offset: 0,
            length: 0,
            pipeline: Some(self.pipeline_message()),
            ..Default::default()
        }
    }

    /// Build an `OmResponse` envelope: the echoed `cmd_type`, `success = true`,
    /// and `status = OK`. Callers set exactly the one relevant sub-response.
    fn ok_response(cmd_type: i32) -> oz::OmResponse {
        oz::OmResponse {
            cmd_type,
            status: oz::Status::Ok as i32,
            success: Some(true),
            ..Default::default()
        }
    }

    /// Build a non-OK `OmResponse`: echoed `cmd_type`, `success = false`, and the
    /// given 1-based `status`. No sub-response is set (W1: a non-OK status must
    /// stand on its own, never carry a stale success body).
    fn err_response(cmd_type: i32, status: oz::Status, message: &str) -> oz::OmResponse {
        oz::OmResponse {
            cmd_type,
            status: status as i32,
            success: Some(false),
            message: Some(message.to_string()),
            ..Default::default()
        }
    }
}

#[tonic::async_trait]
impl OzoneManagerService for CompliantOm {
    async fn submit_request(
        &self,
        req: Request<oz::OmRequest>,
    ) -> Result<Response<oz::OmResponse>, Status> {
        let req = req.into_inner();
        let cmd_type = req.cmd_type;
        self.record.lock().push(req.clone());

        let resp = match oz::Type::try_from(cmd_type) {
            Ok(oz::Type::CreateBucket) => {
                // CreateBucketRequest carries the full BucketInfo; the
                // (volume, bucket) names live on it.
                let info = req
                    .create_bucket_request
                    .map(|r| r.bucket_info)
                    .ok_or_else(|| Status::invalid_argument("missing createBucketRequest"))?;
                self.state
                    .lock()
                    .buckets
                    .insert((info.volume_name, info.bucket_name));
                let mut r = Self::ok_response(cmd_type);
                r.create_bucket_response = Some(oz::CreateBucketResponse {});
                r
            }
            Ok(oz::Type::InfoBucket) => {
                let ib = req
                    .info_bucket_request
                    .ok_or_else(|| Status::invalid_argument("missing infoBucketRequest"))?;
                let exists = self
                    .state
                    .lock()
                    .buckets
                    .contains(&(ib.volume_name.clone(), ib.bucket_name.clone()));
                if exists {
                    let mut r = Self::ok_response(cmd_type);
                    r.info_bucket_response = Some(oz::InfoBucketResponse {
                        bucket_info: Some(oz::BucketInfo {
                            volume_name: ib.volume_name,
                            bucket_name: ib.bucket_name,
                            is_version_enabled: false,
                            storage_type: hdds::StorageTypeProto::Disk as i32,
                            ..Default::default()
                        }),
                    });
                    r
                } else {
                    Self::err_response(cmd_type, oz::Status::BucketNotFound, "bucket not found")
                }
            }
            Ok(oz::Type::DeleteBucket) => {
                let db = req
                    .delete_bucket_request
                    .ok_or_else(|| Status::invalid_argument("missing deleteBucketRequest"))?;
                self.state
                    .lock()
                    .buckets
                    .remove(&(db.volume_name, db.bucket_name));
                let mut r = Self::ok_response(cmd_type);
                r.delete_bucket_response = Some(oz::DeleteBucketResponse {});
                r
            }
            Ok(oz::Type::ListBuckets) => {
                let lb = req
                    .list_buckets_request
                    .ok_or_else(|| Status::invalid_argument("missing listBucketsRequest"))?;
                let st = self.state.lock();
                let mut bucket_info: Vec<oz::BucketInfo> = st
                    .buckets
                    .iter()
                    .filter(|(v, _)| *v == lb.volume_name)
                    .map(|(v, b)| oz::BucketInfo {
                        volume_name: v.clone(),
                        bucket_name: b.clone(),
                        is_version_enabled: false,
                        storage_type: hdds::StorageTypeProto::Disk as i32,
                        ..Default::default()
                    })
                    .collect();
                bucket_info.sort_by(|a, b| a.bucket_name.cmp(&b.bucket_name));
                let mut r = Self::ok_response(cmd_type);
                r.list_buckets_response = Some(oz::ListBucketsResponse { bucket_info });
                r
            }
            Ok(oz::Type::CreateKey) => {
                let ka = req
                    .create_key_request
                    .map(|r| r.key_args)
                    .ok_or_else(|| Status::invalid_argument("missing createKeyRequest"))?;
                let (client_id, container_id) = {
                    let mut st = self.state.lock();
                    let cid = st.next_client_id;
                    st.next_client_id += 1;
                    let cont = st.next_container_id;
                    st.next_container_id += 1;
                    (cid, cont)
                };
                let location = self.alloc_block(container_id);
                // Echo the EC config the request asked for, else the pipeline's.
                let ec = ka
                    .ec_replication_config
                    .clone()
                    .or_else(|| Some(self.pipeline.ec.clone()));
                let key_info = oz::KeyInfo {
                    volume_name: ka.volume_name,
                    bucket_name: ka.bucket_name,
                    key_name: ka.key_name,
                    data_size: ka.data_size.unwrap_or(0),
                    r#type: hdds::ReplicationType::Ec as i32,
                    key_location_list: vec![oz::KeyLocationList {
                        version: Some(0),
                        key_locations: vec![location],
                        ..Default::default()
                    }],
                    creation_time: FAKE_OBJECT_TIME_MS,
                    modification_time: FAKE_OBJECT_TIME_MS,
                    ec_replication_config: ec,
                    ..Default::default()
                };
                let mut r = Self::ok_response(cmd_type);
                r.create_key_response = Some(oz::CreateKeyResponse {
                    key_info: Some(key_info),
                    id: Some(client_id),
                    open_version: Some(0),
                });
                r
            }
            Ok(oz::Type::AllocateBlock) => {
                let abr = req
                    .allocate_block_request
                    .ok_or_else(|| Status::invalid_argument("missing allocateBlockRequest"))?;
                // The same pipeline; a fresh monotonic block in the key's container.
                let container_id = abr
                    .key_args
                    .key_locations
                    .first()
                    .map(|l| l.block_id.container_block_id.container_id)
                    .unwrap_or(1);
                let location = self.alloc_block(container_id);
                let mut r = Self::ok_response(cmd_type);
                r.allocate_block_response = Some(oz::AllocateBlockResponse {
                    key_location: Some(location),
                });
                r
            }
            Ok(oz::Type::CommitKey) => {
                let ka = req
                    .commit_key_request
                    .map(|r| r.key_args)
                    .ok_or_else(|| Status::invalid_argument("missing commitKeyRequest"))?;
                // W2: keep the ACTUAL written block ids/lengths/offsets verbatim.
                // The client commits with pipeline-less locations (the OM owns
                // placement, so a commit need only name block_id + length). The
                // real OM re-associates each committed block with its pipeline so
                // a later GetKeyInfo returns the full location (members +
                // member_replica_indexes + EC) the gateway reads shards from.
                // Mirror that: preserve the committed extent, re-attach the
                // pipeline.
                let tuple = (
                    ka.volume_name.clone(),
                    ka.bucket_name.clone(),
                    ka.key_name.clone(),
                );
                let ec = ka
                    .ec_replication_config
                    .clone()
                    .or_else(|| Some(self.pipeline.ec.clone()));
                let committed: Vec<oz::KeyLocation> = ka
                    .key_locations
                    .into_iter()
                    .map(|mut loc| {
                        loc.pipeline = Some(self.pipeline_message());
                        loc
                    })
                    .collect();
                let key_info = oz::KeyInfo {
                    volume_name: ka.volume_name,
                    bucket_name: ka.bucket_name,
                    key_name: ka.key_name,
                    data_size: ka.data_size.unwrap_or(0),
                    r#type: hdds::ReplicationType::Ec as i32,
                    key_location_list: vec![oz::KeyLocationList {
                        version: Some(0),
                        key_locations: committed,
                        ..Default::default()
                    }],
                    creation_time: FAKE_OBJECT_TIME_MS,
                    modification_time: FAKE_OBJECT_TIME_MS,
                    metadata: ka.metadata,
                    ec_replication_config: ec,
                    ..Default::default()
                };
                self.state.lock().keys.insert(tuple, key_info);
                let mut r = Self::ok_response(cmd_type);
                r.commit_key_response = Some(oz::CommitKeyResponse {});
                r
            }
            Ok(oz::Type::GetKeyInfo) => {
                let ka = req
                    .get_key_info_request
                    .map(|r| r.key_args)
                    .ok_or_else(|| Status::invalid_argument("missing getKeyInfoRequest"))?;
                let tuple = (ka.volume_name, ka.bucket_name, ka.key_name);
                let found = self.state.lock().keys.get(&tuple).cloned();
                match found {
                    Some(key_info) => {
                        let mut r = Self::ok_response(cmd_type);
                        r.get_key_info_response = Some(oz::GetKeyInfoResponse {
                            key_info: Some(key_info),
                            ..Default::default()
                        });
                        r
                    }
                    None => Self::err_response(cmd_type, oz::Status::KeyNotFound, "key not found"),
                }
            }
            Ok(oz::Type::DeleteKey) => {
                let ka = req
                    .delete_key_request
                    .map(|r| r.key_args)
                    .ok_or_else(|| Status::invalid_argument("missing deleteKeyRequest"))?;
                let tuple = (ka.volume_name, ka.bucket_name, ka.key_name);
                self.state.lock().keys.remove(&tuple);
                let mut r = Self::ok_response(cmd_type);
                r.delete_key_response = Some(oz::DeleteKeyResponse::default());
                r
            }
            Ok(oz::Type::InitiateMultiPartUpload) => {
                // Mint `upload-N`, persist an empty upload bound to its
                // `(volume, bucket, key)`. The OM owns the upload until
                // complete/abort. `multipart_upload_id` is echoed back so the
                // gateway can drive subsequent CreateKey/Commit/Complete.
                let ka = req
                    .initiate_multi_part_upload_request
                    .map(|r| r.key_args)
                    .ok_or_else(|| Status::invalid_argument("missing initiateMultiPartUploadRequest"))?;
                let upload_id = {
                    let mut st = self.state.lock();
                    let n = st.next_upload_id;
                    st.next_upload_id += 1;
                    let upload_id = format!("upload-{n}");
                    st.uploads.insert(
                        upload_id.clone(),
                        StoredUpload {
                            vbk: (
                                ka.volume_name.clone(),
                                ka.bucket_name.clone(),
                                ka.key_name.clone(),
                            ),
                            parts: BTreeMap::new(),
                        },
                    );
                    upload_id
                };
                let mut r = Self::ok_response(cmd_type);
                r.initiate_multi_part_upload_response = Some(oz::MultipartInfoInitiateResponse {
                    volume_name: ka.volume_name,
                    bucket_name: ka.bucket_name,
                    key_name: ka.key_name,
                    multipart_upload_id: upload_id,
                });
                r
            }
            Ok(oz::Type::CommitMultiPartUpload) => {
                // Persist one part under its upload, keyed by
                // `key_args.multipart_number`. The part's blocks are taken from
                // `key_args.key_locations` VERBATIM (W2) and its hex ETag from
                // `key_args.metadata["ETAG"]`. Reject an out-of-range part
                // number (INVALID_REQUEST) or an unknown upload
                // (NO_SUCH_MULTIPART_UPLOAD_ERROR). Upsert: re-committing a part
                // number overwrites it (last writer wins).
                let ka = req
                    .commit_multi_part_upload_request
                    .map(|r| r.key_args)
                    .ok_or_else(|| Status::invalid_argument("missing commitMultiPartUploadRequest"))?;
                let upload_id = ka.multipart_upload_id.clone().unwrap_or_default();
                let part_number = ka.multipart_number.unwrap_or(0);
                if !(1..=MAX_PART_NUMBER).contains(&part_number) {
                    Self::err_response(
                        cmd_type,
                        oz::Status::InvalidRequest,
                        "part number out of range (1..=10000)",
                    )
                } else {
                    let etag_hex = ka
                        .metadata
                        .iter()
                        .find(|kv| kv.key == ETAG_METADATA_KEY)
                        .and_then(|kv| kv.value.clone())
                        .unwrap_or_default();
                    let size = ka.data_size.unwrap_or(0);
                    let part_name = format!("{}-{}-{}", ka.key_name, upload_id, part_number);
                    let mut st = self.state.lock();
                    match st.uploads.get_mut(&upload_id) {
                        Some(upload) => {
                            upload.parts.insert(
                                part_number,
                                StoredPart {
                                    locations: ka.key_locations,
                                    size,
                                    etag_hex: etag_hex.clone(),
                                },
                            );
                            let mut r = Self::ok_response(cmd_type);
                            r.commit_multi_part_upload_response =
                                Some(oz::MultipartCommitUploadPartResponse {
                                    part_name: Some(part_name),
                                    e_tag: Some(etag_hex),
                                });
                            r
                        }
                        None => Self::err_response(
                            cmd_type,
                            oz::Status::NoSuchMultipartUploadError,
                            "no such multipart upload",
                        ),
                    }
                }
            }
            Ok(oz::Type::CompleteMultiPartUpload) => {
                // Stitch the upload's stored parts in the client-supplied order
                // into the final key, then drop the upload. Validation order
                // matches `FakeOm`: non-empty parts; strictly ascending part
                // numbers, no dups (INVALID_PART_ORDER); every named part present
                // (INVALID_PART); every NON-LAST part >= 5 MiB (ENTITY_TOO_SMALL).
                // The AWS multipart ETag is hex(md5(concat of the raw 16-byte
                // per-part MD5 digests)) + "-N": each part's hex ETag is decoded
                // back to raw bytes BEFORE the rollup.
                let creq = req
                    .complete_multi_part_upload_request
                    .ok_or_else(|| Status::invalid_argument("missing completeMultiPartUploadRequest"))?;
                let ka = creq.key_args;
                let upload_id = ka.multipart_upload_id.clone().unwrap_or_default();
                let tuple = (
                    ka.volume_name.clone(),
                    ka.bucket_name.clone(),
                    ka.key_name.clone(),
                );
                let order: Vec<u32> = creq.parts_list.iter().map(|p| p.part_number).collect();
                if order.is_empty() {
                    Self::err_response(cmd_type, oz::Status::InvalidPart, "no parts to complete")
                } else if order.windows(2).any(|w| w[0] >= w[1]) {
                    Self::err_response(
                        cmd_type,
                        oz::Status::InvalidPartOrder,
                        "parts must be ascending with no duplicates",
                    )
                } else {
                    let mut st = self.state.lock();
                    let validated = match st.uploads.get(&upload_id) {
                        None => Err(Self::err_response(
                            cmd_type,
                            oz::Status::NoSuchMultipartUploadError,
                            "no such multipart upload",
                        )),
                        Some(upload) => {
                            let last_idx = order.len() - 1;
                            let mut final_size = 0u64;
                            let mut final_locations: Vec<oz::KeyLocation> = Vec::new();
                            let mut hasher = Md5::new();
                            let mut bad: Option<oz::OmResponse> = None;
                            for (idx, part) in creq.parts_list.iter().enumerate() {
                                let pn = part.part_number;
                                let stored = match upload.parts.get(&pn) {
                                    Some(s) => s,
                                    None => {
                                        bad = Some(Self::err_response(
                                            cmd_type,
                                            oz::Status::InvalidPart,
                                            "part was not uploaded",
                                        ));
                                        break;
                                    }
                                };
                                if idx != last_idx && stored.size < MIN_PART_SIZE {
                                    bad = Some(Self::err_response(
                                        cmd_type,
                                        oz::Status::EntityTooSmall,
                                        "non-last part smaller than 5 MiB",
                                    ));
                                    break;
                                }
                                // Roll the raw 16-byte digest in. Prefer the
                                // request's hex ETag (the gateway resends it on
                                // the parts list), else the stored one; decode
                                // hex -> raw so the rollup matches AWS exactly.
                                let etag_hex = part
                                    .e_tag
                                    .clone()
                                    .filter(|s| !s.is_empty())
                                    .unwrap_or_else(|| stored.etag_hex.clone());
                                let raw = hex_decode(&etag_hex).unwrap_or_default();
                                hasher.update(&raw);
                                final_size += stored.size;
                                final_locations.extend(stored.locations.iter().cloned());
                            }
                            match bad {
                                Some(resp) => Err(resp),
                                None => {
                                    let etag = format!("{:x}-{}", hasher.finalize(), order.len());
                                    Ok((etag, final_size, final_locations))
                                }
                            }
                        }
                    };
                    match validated {
                        Err(resp) => resp,
                        Ok((etag, final_size, final_locations)) => {
                            // Re-attach the pipeline to each stitched block, just
                            // as the CommitKey handler does, so a later
                            // GetKeyInfo returns full read locations.
                            let located: Vec<oz::KeyLocation> = final_locations
                                .into_iter()
                                .map(|mut loc| {
                                    loc.pipeline = Some(self.pipeline_message());
                                    loc
                                })
                                .collect();
                            let key_info = oz::KeyInfo {
                                volume_name: ka.volume_name.clone(),
                                bucket_name: ka.bucket_name.clone(),
                                key_name: ka.key_name.clone(),
                                data_size: final_size,
                                r#type: hdds::ReplicationType::Ec as i32,
                                key_location_list: vec![oz::KeyLocationList {
                                    version: Some(0),
                                    key_locations: located,
                                    ..Default::default()
                                }],
                                creation_time: FAKE_OBJECT_TIME_MS,
                                modification_time: FAKE_OBJECT_TIME_MS,
                                metadata: vec![hdds::KeyValue {
                                    key: ETAG_METADATA_KEY.to_string(),
                                    value: Some(etag.clone()),
                                }],
                                ec_replication_config: Some(self.pipeline.ec.clone()),
                                ..Default::default()
                            };
                            st.uploads.remove(&upload_id);
                            st.keys.insert(tuple, key_info);
                            drop(st);
                            let mut r = Self::ok_response(cmd_type);
                            r.complete_multi_part_upload_response =
                                Some(oz::MultipartUploadCompleteResponse {
                                    volume: Some(ka.volume_name),
                                    bucket: Some(ka.bucket_name),
                                    key: Some(ka.key_name),
                                    hash: Some(etag),
                                });
                            r
                        }
                    }
                }
            }
            Ok(oz::Type::AbortMultiPartUpload) => {
                // Drop the upload. Idempotent: aborting an unknown upload is a
                // no-op success (S3 abort is lenient; the gateway relies on it).
                let ka = req
                    .abort_multi_part_upload_request
                    .map(|r| r.key_args)
                    .ok_or_else(|| Status::invalid_argument("missing abortMultiPartUploadRequest"))?;
                let upload_id = ka.multipart_upload_id.unwrap_or_default();
                self.state.lock().uploads.remove(&upload_id);
                let mut r = Self::ok_response(cmd_type);
                r.abort_multi_part_upload_response = Some(oz::MultipartUploadAbortResponse {});
                r
            }
            Ok(oz::Type::ListMultiPartUploadParts) => {
                // Ascending parts of an in-flight upload, filtering to part
                // numbers strictly above `part_numbermarker`. Unknown upload ->
                // NO_SUCH_MULTIPART_UPLOAD_ERROR. `max_parts` is ignored (the
                // gateway never paginates): one untruncated page.
                let lp = req
                    .list_multipart_upload_parts_request
                    .ok_or_else(|| Status::invalid_argument("missing listMultiPartUploadPartsRequest"))?;
                let marker = lp.part_numbermarker.unwrap_or(0);
                let st = self.state.lock();
                match st.uploads.get(&lp.upload_id) {
                    None => Self::err_response(
                        cmd_type,
                        oz::Status::NoSuchMultipartUploadError,
                        "no such multipart upload",
                    ),
                    Some(upload) => {
                        let parts_list: Vec<oz::PartInfo> = upload
                            .parts
                            .iter()
                            .filter(|(pn, _)| **pn > marker)
                            .map(|(pn, p)| oz::PartInfo {
                                part_number: *pn,
                                part_name: format!("{}-{}-{}", lp.key, lp.upload_id, pn),
                                modification_time: FAKE_OBJECT_TIME_MS,
                                size: p.size,
                                e_tag: Some(p.etag_hex.clone()),
                            })
                            .collect();
                        let mut r = Self::ok_response(cmd_type);
                        r.list_multipart_upload_parts_response =
                            Some(oz::MultipartUploadListPartsResponse {
                                r#type: Some(hdds::ReplicationType::Ec as i32),
                                factor: None,
                                next_part_number_marker: Some(0),
                                is_truncated: Some(false),
                                parts_list,
                                ec_replication_config: Some(self.pipeline.ec.clone()),
                            });
                        r
                    }
                }
            }
            Ok(oz::Type::ListMultipartUploads) => {
                // In-flight uploads for a `(volume, bucket)` whose key starts
                // with `prefix`, sorted by (key, upload_id). `max_uploads` is
                // ignored: one untruncated page.
                let lu = req
                    .list_multipart_uploads_request
                    .ok_or_else(|| Status::invalid_argument("missing listMultipartUploadsRequest"))?;
                let st = self.state.lock();
                let mut uploads_list: Vec<oz::MultipartUploadInfo> = st
                    .uploads
                    .iter()
                    .filter(|(_, u)| {
                        u.vbk.0 == lu.volume
                            && u.vbk.1 == lu.bucket
                            && u.vbk.2.starts_with(&lu.prefix)
                    })
                    .map(|(id, u)| oz::MultipartUploadInfo {
                        volume_name: u.vbk.0.clone(),
                        bucket_name: u.vbk.1.clone(),
                        key_name: u.vbk.2.clone(),
                        upload_id: id.clone(),
                        creation_time: FAKE_OBJECT_TIME_MS,
                        r#type: hdds::ReplicationType::Ec as i32,
                        factor: None,
                        ec_replication_config: Some(self.pipeline.ec.clone()),
                    })
                    .collect();
                uploads_list.sort_by(|a, b| {
                    a.key_name
                        .cmp(&b.key_name)
                        .then_with(|| a.upload_id.cmp(&b.upload_id))
                });
                let mut r = Self::ok_response(cmd_type);
                r.list_multipart_uploads_response = Some(oz::ListMultipartUploadsResponse {
                    is_truncated: Some(false),
                    uploads_list,
                    next_key_marker: None,
                    next_upload_id_marker: None,
                });
                r
            }
            Ok(oz::Type::ListKeys) => {
                // Committed keys in `(volume, bucket)` whose name starts with
                // `prefix` and sorts strictly after `start_key`, ascending,
                // capped at `count` (<= 0 -> DEFAULT_LIST_KEYS_LIMIT). Each
                // KeyInfo is returned whole; the client reads name/size/ETAG.
                let lk = req
                    .list_keys_request
                    .ok_or_else(|| Status::invalid_argument("missing listKeysRequest"))?;
                let prefix = lk.prefix.unwrap_or_default();
                let start_key = lk.start_key.unwrap_or_default();
                let limit = match lk.count {
                    Some(c) if c > 0 => c as usize,
                    _ => DEFAULT_LIST_KEYS_LIMIT,
                };
                let st = self.state.lock();
                let mut matches: Vec<oz::KeyInfo> = st
                    .keys
                    .iter()
                    .filter(|((v, b, k), _)| {
                        *v == lk.volume_name
                            && *b == lk.bucket_name
                            && k.starts_with(&prefix)
                            && k.as_str() > start_key.as_str()
                    })
                    .map(|(_, ki)| ki.clone())
                    .collect();
                matches.sort_by(|a, b| a.key_name.cmp(&b.key_name));
                let truncated = matches.len() > limit;
                matches.truncate(limit);
                let mut r = Self::ok_response(cmd_type);
                r.list_keys_response = Some(oz::ListKeysResponse {
                    key_info: matches,
                    is_truncated: Some(truncated),
                });
                r
            }
            Ok(oz::Type::PutObjectTagging) => {
                // Replace the object's S3 tag set. DESIGN: tags live in the key
                // metadata under the `x-amz-tag-` prefix (matching the gateway's
                // TAG_META_PREFIX round-trip), so GetKeyInfo and GetObjectTagging
                // both see them. Drop all prior `x-amz-tag-*` entries, then add
                // the request's `key_args.tags`. Unknown key -> KEY_NOT_FOUND.
                let ka = req
                    .put_object_tagging_request
                    .map(|r| r.key_args)
                    .ok_or_else(|| Status::invalid_argument("missing putObjectTaggingRequest"))?;
                let tuple = (ka.volume_name, ka.bucket_name, ka.key_name);
                let mut st = self.state.lock();
                match st.keys.get_mut(&tuple) {
                    Some(ki) => {
                        ki.metadata.retain(|kv| !kv.key.starts_with(TAG_META_PREFIX));
                        for tag in ka.tags {
                            ki.metadata.push(hdds::KeyValue {
                                key: format!("{TAG_META_PREFIX}{}", tag.key),
                                value: tag.value,
                            });
                        }
                        let mut r = Self::ok_response(cmd_type);
                        r.put_object_tagging_response = Some(oz::PutObjectTaggingResponse {});
                        r
                    }
                    None => Self::err_response(cmd_type, oz::Status::KeyNotFound, "key not found"),
                }
            }
            Ok(oz::Type::GetObjectTagging) => {
                // Read the object's S3 tags back out of its metadata (stripping
                // the `x-amz-tag-` prefix) as a clean `tags` list, sorted by tag
                // key for a stable response. Unknown key -> KEY_NOT_FOUND.
                let ka = req
                    .get_object_tagging_request
                    .map(|r| r.key_args)
                    .ok_or_else(|| Status::invalid_argument("missing getObjectTaggingRequest"))?;
                let tuple = (ka.volume_name, ka.bucket_name, ka.key_name);
                let st = self.state.lock();
                match st.keys.get(&tuple) {
                    Some(ki) => {
                        let mut tags: Vec<hdds::KeyValue> = ki
                            .metadata
                            .iter()
                            .filter_map(|kv| {
                                kv.key.strip_prefix(TAG_META_PREFIX).map(|t| hdds::KeyValue {
                                    key: t.to_string(),
                                    value: kv.value.clone(),
                                })
                            })
                            .collect();
                        tags.sort_by(|a, b| a.key.cmp(&b.key));
                        let mut r = Self::ok_response(cmd_type);
                        r.get_object_tagging_response = Some(oz::GetObjectTaggingResponse { tags });
                        r
                    }
                    None => Self::err_response(cmd_type, oz::Status::KeyNotFound, "key not found"),
                }
            }
            Ok(oz::Type::DeleteObjectTagging) => {
                // Clear the object's S3 tag set (drop all `x-amz-tag-*` metadata).
                // Unknown key -> KEY_NOT_FOUND.
                let ka = req
                    .delete_object_tagging_request
                    .map(|r| r.key_args)
                    .ok_or_else(|| Status::invalid_argument("missing deleteObjectTaggingRequest"))?;
                let tuple = (ka.volume_name, ka.bucket_name, ka.key_name);
                let mut st = self.state.lock();
                match st.keys.get_mut(&tuple) {
                    Some(ki) => {
                        ki.metadata.retain(|kv| !kv.key.starts_with(TAG_META_PREFIX));
                        let mut r = Self::ok_response(cmd_type);
                        r.delete_object_tagging_response = Some(oz::DeleteObjectTaggingResponse {});
                        r
                    }
                    None => Self::err_response(cmd_type, oz::Status::KeyNotFound, "key not found"),
                }
            }
            // Anything else (volume ops, FS ops, snapshots, ...) is out of scope
            // for this fixture: report INVALID_REQUEST rather than panic.
            _ => Self::err_response(
                cmd_type,
                oz::Status::InvalidRequest,
                "cmdType not implemented by CompliantOm",
            ),
        };

        Ok(Response::new(resp))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oz::ozone_manager_service_client::OzoneManagerServiceClient as Client;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    /// Lowercase-hex encode (the inverse of [`hex_decode`]); the tests build part
    /// ETags from chosen raw 16-byte digests so the multipart ETag is checkable.
    fn hex_encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// A 3+2 RS pipeline with five distinct datanodes (slots 1..=5), each with a
    /// REPLICATION port the gateway would dial.
    fn test_pipeline() -> CompliantOmPipeline {
        let ec = hdds::EcReplicationConfig {
            data: 3,
            parity: 2,
            codec: "rs".to_string(),
            ec_chunk_size: 1024 * 1024,
        };
        let datanodes = (0..5u32)
            .map(|i| hdds::DatanodeDetailsProto {
                uuid: Some(format!("dn-{i}")),
                ip_address: format!("10.0.0.{}", i + 1),
                host_name: format!("host-{i}"),
                ports: vec![hdds::Port {
                    name: "REPLICATION".to_string(),
                    value: 19864 + i,
                }],
                ..Default::default()
            })
            .collect();
        CompliantOmPipeline { datanodes, ec }
    }

    /// Build an `OmRequest` envelope for `cmd_type` with a recognizable principal,
    /// so dispatch and the recorded `s3Authentication.accessId` are exercised.
    fn envelope(cmd_type: oz::Type) -> oz::OmRequest {
        oz::OmRequest {
            cmd_type: cmd_type as i32,
            client_id: "test-client".to_string(),
            version: Some(1),
            s3_authentication: Some(oz::S3Authentication {
                access_id: Some("the-principal".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn key_args(key: &str) -> oz::KeyArgs {
        oz::KeyArgs {
            volume_name: "s3v".to_string(),
            bucket_name: "bkt".to_string(),
            key_name: key.to_string(),
            ..Default::default()
        }
    }

    async fn serve(om: CompliantOm) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            Server::builder()
                .add_service(om.into_server())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .ok();
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn dispatch_roundtrips_over_real_service() {
        // Drives the real OzoneManagerService.submitRequest over a tonic channel
        // (not just inherent-method calls), proving the cmdType dispatch wiring.
        let om = CompliantOm::new(test_pipeline());
        let record = om.record();
        let mut client = Client::connect(serve(om).await).await.unwrap();

        // CreateBucket then InfoBucket: the bucket must now report as existing.
        let mut create = envelope(oz::Type::CreateBucket);
        create.create_bucket_request = Some(oz::CreateBucketRequest {
            bucket_info: oz::BucketInfo {
                volume_name: "s3v".to_string(),
                bucket_name: "bkt".to_string(),
                is_version_enabled: false,
                storage_type: hdds::StorageTypeProto::Disk as i32,
                ..Default::default()
            },
        });
        let cresp = client.submit_request(create).await.unwrap().into_inner();
        assert_eq!(cresp.status, oz::Status::Ok as i32);
        assert!(cresp.create_bucket_response.is_some());

        let mut info = envelope(oz::Type::InfoBucket);
        info.info_bucket_request = Some(oz::InfoBucketRequest {
            volume_name: "s3v".to_string(),
            bucket_name: "bkt".to_string(),
        });
        let iresp = client.submit_request(info).await.unwrap().into_inner();
        assert_eq!(iresp.status, oz::Status::Ok as i32);
        assert_eq!(
            iresp
                .info_bucket_response
                .unwrap()
                .bucket_info
                .unwrap()
                .bucket_name,
            "bkt"
        );

        // The fixture recorded both envelopes with the attested principal.
        let rec = record.lock();
        assert_eq!(rec.len(), 2);
        assert_eq!(
            rec[0].s3_authentication.as_ref().unwrap().access_id.as_deref(),
            Some("the-principal")
        );
    }

    #[tokio::test]
    async fn info_bucket_missing_is_bucket_not_found() {
        let om = CompliantOm::new(test_pipeline());
        let mut req = envelope(oz::Type::InfoBucket);
        req.info_bucket_request = Some(oz::InfoBucketRequest {
            volume_name: "s3v".to_string(),
            bucket_name: "absent".to_string(),
        });
        let resp = om
            .submit_request(Request::new(req))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.status, oz::Status::BucketNotFound as i32);
        assert_eq!(resp.success, Some(false));
        assert!(
            resp.info_bucket_response.is_none(),
            "a non-OK status must not carry a sub-response (W1)"
        );
    }

    #[tokio::test]
    async fn create_key_returns_client_id_and_identity_replica_indexes() {
        let om = CompliantOm::new(test_pipeline());
        let mut req = envelope(oz::Type::CreateKey);
        req.create_key_request = Some(oz::CreateKeyRequest {
            key_args: key_args("obj"),
            ..Default::default()
        });
        let resp = om
            .submit_request(Request::new(req))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.status, oz::Status::Ok as i32);
        let ck = resp.create_key_response.unwrap();
        assert_eq!(ck.id, Some(1), "first CreateKey open-session id is 1");

        let ki = ck.key_info.unwrap();
        assert_eq!(ki.key_location_list.len(), 1, "one pre-allocated block group");
        let locs = &ki.key_location_list[0].key_locations;
        assert_eq!(locs.len(), 1, "exactly one block pre-allocated");
        let pipe = locs[0].pipeline.as_ref().unwrap();
        assert_eq!(pipe.members.len(), 5, "k+p members");
        // W3: member_replica_indexes is parallel to members and == 1..=5.
        assert_eq!(pipe.member_replica_indexes, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn commit_then_get_key_info_round_trips() {
        let om = CompliantOm::new(test_pipeline());

        // Commit a key with one concrete written block (container 7, local 42).
        let written = oz::KeyLocation {
            block_id: hdds::BlockId {
                container_block_id: hdds::ContainerBlockId {
                    container_id: 7,
                    local_id: 42,
                },
                ..Default::default()
            },
            offset: 0,
            length: 4096,
            ..Default::default()
        };
        let mut commit = envelope(oz::Type::CommitKey);
        let mut ka = key_args("obj");
        ka.data_size = Some(4096);
        ka.key_locations = vec![written];
        ka.metadata = vec![hdds::KeyValue {
            key: "ETAG".to_string(),
            value: Some("deadbeef".to_string()),
        }];
        commit.commit_key_request = Some(oz::CommitKeyRequest {
            key_args: ka,
            client_id: 1,
            ..Default::default()
        });
        let cresp = om
            .submit_request(Request::new(commit))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(cresp.status, oz::Status::Ok as i32);
        assert!(cresp.commit_key_response.is_some());

        // GetKeyInfo must return the committed size + the verbatim written block.
        let mut get = envelope(oz::Type::GetKeyInfo);
        get.get_key_info_request = Some(oz::GetKeyInfoRequest {
            key_args: key_args("obj"),
            ..Default::default()
        });
        let gresp = om
            .submit_request(Request::new(get))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(gresp.status, oz::Status::Ok as i32);
        let ki = gresp.get_key_info_response.unwrap().key_info.unwrap();
        assert_eq!(ki.data_size, 4096);
        let locs = &ki.key_location_list[0].key_locations;
        assert_eq!(locs.len(), 1);
        // W2: the stored location is exactly what was committed.
        assert_eq!(locs[0].block_id.container_block_id.container_id, 7);
        assert_eq!(locs[0].block_id.container_block_id.local_id, 42);
        assert_eq!(locs[0].length, 4096);
        assert_eq!(
            ki.metadata
                .iter()
                .find(|kv| kv.key == "ETAG")
                .and_then(|kv| kv.value.as_deref()),
            Some("deadbeef")
        );
    }

    #[tokio::test]
    async fn get_key_info_missing_is_key_not_found() {
        let om = CompliantOm::new(test_pipeline());
        let mut req = envelope(oz::Type::GetKeyInfo);
        req.get_key_info_request = Some(oz::GetKeyInfoRequest {
            key_args: key_args("nope"),
            ..Default::default()
        });
        let resp = om
            .submit_request(Request::new(req))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.status, oz::Status::KeyNotFound as i32);
        assert_eq!(resp.success, Some(false));
        assert!(resp.get_key_info_response.is_none());
    }

    #[tokio::test]
    async fn allocate_block_local_ids_are_monotonic() {
        let om = CompliantOm::new(test_pipeline());
        let mut last = 0i64;
        for _ in 0..4 {
            let mut req = envelope(oz::Type::AllocateBlock);
            req.allocate_block_request = Some(oz::AllocateBlockRequest {
                key_args: key_args("obj"),
                client_id: 1,
                ..Default::default()
            });
            let resp = om
                .submit_request(Request::new(req))
                .await
                .unwrap()
                .into_inner();
            let id = resp
                .allocate_block_response
                .unwrap()
                .key_location
                .unwrap()
                .block_id
                .container_block_id
                .local_id;
            assert!(id > last, "local_id {id} must exceed previous {last}");
            last = id;
        }
    }

    /// A concrete written block for a part (container 7, the given local id), so
    /// the stitched final key's locations are observable and order-sensitive.
    fn part_location(local_id: i64, length: u64) -> oz::KeyLocation {
        oz::KeyLocation {
            block_id: hdds::BlockId {
                container_block_id: hdds::ContainerBlockId {
                    container_id: 7,
                    local_id,
                },
                ..Default::default()
            },
            offset: 0,
            length,
            ..Default::default()
        }
    }

    /// Initiate an upload directly on the fixture and return its upload id.
    async fn initiate(om: &CompliantOm, key: &str) -> String {
        let mut req = envelope(oz::Type::InitiateMultiPartUpload);
        req.initiate_multi_part_upload_request = Some(oz::MultipartInfoInitiateRequest {
            key_args: key_args(key),
        });
        om.submit_request(Request::new(req))
            .await
            .unwrap()
            .into_inner()
            .initiate_multi_part_upload_response
            .unwrap()
            .multipart_upload_id
    }

    /// Commit one part of `key` under `upload_id`, with a chosen hex ETag and a
    /// single block at `local_id`. Returns the resulting `OmResponse` so callers
    /// can assert status (e.g. NoSuchUpload / InvalidRequest).
    async fn commit_part(
        om: &CompliantOm,
        key: &str,
        upload_id: &str,
        pn: u32,
        size: u64,
        etag_hex: &str,
        local_id: i64,
    ) -> oz::OmResponse {
        let mut ka = key_args(key);
        ka.data_size = Some(size);
        ka.is_multipart_key = Some(true);
        ka.multipart_upload_id = Some(upload_id.to_string());
        ka.multipart_number = Some(pn);
        ka.key_locations = vec![part_location(local_id, size)];
        ka.metadata = vec![hdds::KeyValue {
            key: "ETAG".to_string(),
            value: Some(etag_hex.to_string()),
        }];
        let mut req = envelope(oz::Type::CommitMultiPartUpload);
        req.commit_multi_part_upload_request = Some(oz::MultipartCommitUploadPartRequest {
            key_args: ka,
            client_id: 1,
        });
        om.submit_request(Request::new(req)).await.unwrap().into_inner()
    }

    /// Complete `key`/`upload_id` with the given `(part_number, etag_hex)` order.
    async fn complete(
        om: &CompliantOm,
        key: &str,
        upload_id: &str,
        order: &[(u32, String)],
    ) -> oz::OmResponse {
        let mut ka = key_args(key);
        ka.is_multipart_key = Some(true);
        ka.multipart_upload_id = Some(upload_id.to_string());
        let parts_list = order
            .iter()
            .map(|(pn, etag)| oz::Part {
                part_number: *pn,
                part_name: format!("{key}-{upload_id}-{pn}"),
                e_tag: Some(etag.clone()),
            })
            .collect();
        let mut req = envelope(oz::Type::CompleteMultiPartUpload);
        req.complete_multi_part_upload_request = Some(oz::MultipartUploadCompleteRequest {
            key_args: ka,
            parts_list,
        });
        om.submit_request(Request::new(req)).await.unwrap().into_inner()
    }

    /// Fetch a committed key's `KeyInfo` (None if absent).
    async fn get_key(om: &CompliantOm, key: &str) -> Option<oz::KeyInfo> {
        let mut req = envelope(oz::Type::GetKeyInfo);
        req.get_key_info_request = Some(oz::GetKeyInfoRequest {
            key_args: key_args(key),
            ..Default::default()
        });
        om.submit_request(Request::new(req))
            .await
            .unwrap()
            .into_inner()
            .get_key_info_response
            .and_then(|r| r.key_info)
    }

    /// Commit a plain (non-multipart) key with a length and an ETag so listing
    /// has committed objects to return.
    async fn commit_plain_key(om: &CompliantOm, key: &str, size: u64, etag: &str) {
        let mut ka = key_args(key);
        ka.data_size = Some(size);
        ka.key_locations = vec![part_location(1, size)];
        ka.metadata = vec![hdds::KeyValue {
            key: "ETAG".to_string(),
            value: Some(etag.to_string()),
        }];
        let mut req = envelope(oz::Type::CommitKey);
        req.commit_key_request = Some(oz::CommitKeyRequest {
            key_args: ka,
            client_id: 1,
            ..Default::default()
        });
        let resp = om.submit_request(Request::new(req)).await.unwrap().into_inner();
        assert_eq!(resp.status, oz::Status::Ok as i32);
    }

    /// `AbortMultiPartUpload` for `(key, upload_id)`.
    async fn abort_req(om: &CompliantOm, key: &str, upload_id: &str) -> oz::OmResponse {
        let mut ka = key_args(key);
        ka.multipart_upload_id = Some(upload_id.to_string());
        let mut req = envelope(oz::Type::AbortMultiPartUpload);
        req.abort_multi_part_upload_request = Some(oz::MultipartUploadAbortRequest { key_args: ka });
        om.submit_request(Request::new(req)).await.unwrap().into_inner()
    }

    /// `ListMultiPartUploadParts` for `(obj, upload_id)` above `marker`.
    async fn list_parts_req(om: &CompliantOm, upload_id: &str, marker: u32) -> Vec<oz::PartInfo> {
        let mut req = envelope(oz::Type::ListMultiPartUploadParts);
        req.list_multipart_upload_parts_request = Some(oz::MultipartUploadListPartsRequest {
            volume: "s3v".into(),
            bucket: "bkt".into(),
            key: "obj".into(),
            upload_id: upload_id.to_string(),
            part_numbermarker: Some(marker),
            max_parts: None,
        });
        om.submit_request(Request::new(req))
            .await
            .unwrap()
            .into_inner()
            .list_multipart_upload_parts_response
            .unwrap()
            .parts_list
    }

    /// `ListMultipartUploads` in `(s3v, bkt)` filtered by key `prefix`.
    async fn list_uploads_req(om: &CompliantOm, prefix: &str) -> Vec<oz::MultipartUploadInfo> {
        let mut req = envelope(oz::Type::ListMultipartUploads);
        req.list_multipart_uploads_request = Some(oz::ListMultipartUploadsRequest {
            volume: "s3v".into(),
            bucket: "bkt".into(),
            prefix: prefix.to_string(),
            ..Default::default()
        });
        om.submit_request(Request::new(req))
            .await
            .unwrap()
            .into_inner()
            .list_multipart_uploads_response
            .unwrap()
            .uploads_list
    }

    /// `ListKeys` in `(s3v, bkt)` with `prefix`/`start_after`/`count`.
    async fn list_keys_req(
        om: &CompliantOm,
        prefix: &str,
        start_after: &str,
        count: i32,
    ) -> oz::ListKeysResponse {
        let mut req = envelope(oz::Type::ListKeys);
        req.list_keys_request = Some(oz::ListKeysRequest {
            volume_name: "s3v".into(),
            bucket_name: "bkt".into(),
            start_key: Some(start_after.to_string()),
            prefix: Some(prefix.to_string()),
            count: Some(count),
        });
        om.submit_request(Request::new(req))
            .await
            .unwrap()
            .into_inner()
            .list_keys_response
            .unwrap()
    }

    #[tokio::test]
    async fn multipart_complete_stitches_key_with_aws_etag() {
        // Drive the multipart flow over the REAL service to prove cmdType
        // dispatch for the new types, and independently recompute the AWS ETag.
        let om = CompliantOm::new(test_pipeline());
        let record = om.record();
        let mut client = Client::connect(serve(om).await).await.unwrap();

        let mut init = envelope(oz::Type::InitiateMultiPartUpload);
        init.initiate_multi_part_upload_request = Some(oz::MultipartInfoInitiateRequest {
            key_args: key_args("obj"),
        });
        let upload_id = client
            .submit_request(init)
            .await
            .unwrap()
            .into_inner()
            .initiate_multi_part_upload_response
            .unwrap()
            .multipart_upload_id;
        assert_eq!(upload_id, "upload-1");

        // Two parts; part 1 (non-last) clears 5 MiB, part 2 (last) is small.
        let part1_size: u64 = 5 * 1024 * 1024;
        let part2_size: u64 = 1234;
        let raw1 = [0xAAu8; 16];
        let raw2 = [0xBBu8; 16];
        let etag1 = hex_encode(&raw1);
        let etag2 = hex_encode(&raw2);

        for (pn, size, etag, local) in [
            (1u32, part1_size, &etag1, 10i64),
            (2, part2_size, &etag2, 20),
        ] {
            let mut ka = key_args("obj");
            ka.data_size = Some(size);
            ka.is_multipart_key = Some(true);
            ka.multipart_upload_id = Some(upload_id.clone());
            ka.multipart_number = Some(pn);
            ka.key_locations = vec![part_location(local, size)];
            ka.metadata = vec![hdds::KeyValue {
                key: "ETAG".to_string(),
                value: Some(etag.clone()),
            }];
            let mut req = envelope(oz::Type::CommitMultiPartUpload);
            req.commit_multi_part_upload_request = Some(oz::MultipartCommitUploadPartRequest {
                key_args: ka,
                client_id: 1,
            });
            let resp = client.submit_request(req).await.unwrap().into_inner();
            assert_eq!(resp.status, oz::Status::Ok as i32);
            let cp = resp.commit_multi_part_upload_response.unwrap();
            assert_eq!(cp.part_name.as_deref(), Some(format!("obj-{upload_id}-{pn}").as_str()));
            assert_eq!(cp.e_tag.as_deref(), Some(etag.as_str()));
        }

        // Independently recompute the AWS multipart ETag from the RAW digests.
        // The handler must hex-DECODE each part ETag before this rollup; feeding
        // the hex text would yield a different value and fail this assertion.
        let mut hasher = Md5::new();
        hasher.update(raw1);
        hasher.update(raw2);
        let expected_etag = format!("{:x}-2", hasher.finalize());

        let mut comp = envelope(oz::Type::CompleteMultiPartUpload);
        let mut cka = key_args("obj");
        cka.is_multipart_key = Some(true);
        cka.multipart_upload_id = Some(upload_id.clone());
        comp.complete_multi_part_upload_request = Some(oz::MultipartUploadCompleteRequest {
            key_args: cka,
            parts_list: vec![
                oz::Part { part_number: 1, part_name: "obj-upload-1-1".into(), e_tag: Some(etag1) },
                oz::Part { part_number: 2, part_name: "obj-upload-1-2".into(), e_tag: Some(etag2) },
            ],
        });
        let cresp = client.submit_request(comp).await.unwrap().into_inner();
        assert_eq!(cresp.status, oz::Status::Ok as i32);
        let hash = cresp.complete_multi_part_upload_response.unwrap().hash;
        assert_eq!(hash.as_deref(), Some(expected_etag.as_str()));

        // The completed object is a normal key: stitched size + both blocks in
        // order + the multipart ETag in metadata.
        let mut get = envelope(oz::Type::GetKeyInfo);
        get.get_key_info_request = Some(oz::GetKeyInfoRequest {
            key_args: key_args("obj"),
            ..Default::default()
        });
        let ki = client
            .submit_request(get)
            .await
            .unwrap()
            .into_inner()
            .get_key_info_response
            .unwrap()
            .key_info
            .unwrap();
        assert_eq!(ki.data_size, part1_size + part2_size);
        let locs = &ki.key_location_list[0].key_locations;
        assert_eq!(locs.len(), 2, "one block per part, in order");
        assert_eq!(locs[0].block_id.container_block_id.local_id, 10);
        assert_eq!(locs[1].block_id.container_block_id.local_id, 20);
        // Re-attached pipeline (W3) so reads can find shards.
        assert!(locs[0].pipeline.is_some());
        assert_eq!(
            ki.metadata.iter().find(|kv| kv.key == "ETAG").and_then(|kv| kv.value.as_deref()),
            Some(expected_etag.as_str())
        );

        // The upload is gone after complete (recorded envelopes are unused here).
        let _ = record;
    }

    #[tokio::test]
    async fn commit_part_unknown_upload_is_no_such_multipart() {
        let om = CompliantOm::new(test_pipeline());
        let resp = commit_part(&om, "obj", "upload-404", 1, 1024, "ab", 10).await;
        assert_eq!(resp.status, oz::Status::NoSuchMultipartUploadError as i32);
        assert!(resp.commit_multi_part_upload_response.is_none(), "W1");
    }

    #[tokio::test]
    async fn commit_part_out_of_range_is_invalid_request() {
        let om = CompliantOm::new(test_pipeline());
        let upload_id = initiate(&om, "obj").await;
        let resp = commit_part(&om, "obj", &upload_id, 0, 1024, "ab", 10).await;
        assert_eq!(resp.status, oz::Status::InvalidRequest as i32);
        let resp = commit_part(&om, "obj", &upload_id, MAX_PART_NUMBER + 1, 1024, "ab", 10).await;
        assert_eq!(resp.status, oz::Status::InvalidRequest as i32);
    }

    #[tokio::test]
    async fn complete_validation_statuses() {
        // EntityTooSmall: non-last part < 5 MiB.
        let om = CompliantOm::new(test_pipeline());
        let u = initiate(&om, "obj").await;
        commit_part(&om, "obj", &u, 1, 1024, &hex_encode(&[1u8; 16]), 10).await;
        commit_part(&om, "obj", &u, 2, 1024, &hex_encode(&[2u8; 16]), 20).await;
        let resp = complete(&om, "obj", &u, &[(1, hex_encode(&[1u8; 16])), (2, hex_encode(&[2u8; 16]))]).await;
        assert_eq!(resp.status, oz::Status::EntityTooSmall as i32);
        assert!(get_key(&om, "obj").await.is_none(), "failed complete commits nothing");

        // InvalidPart: a named part was never uploaded.
        let om = CompliantOm::new(test_pipeline());
        let u = initiate(&om, "obj").await;
        commit_part(&om, "obj", &u, 1, 5 * 1024 * 1024, &hex_encode(&[1u8; 16]), 10).await;
        let resp = complete(&om, "obj", &u, &[(1, hex_encode(&[1u8; 16])), (2, hex_encode(&[2u8; 16]))]).await;
        assert_eq!(resp.status, oz::Status::InvalidPart as i32);

        // InvalidPartOrder: descending / duplicate part numbers.
        let om = CompliantOm::new(test_pipeline());
        let u = initiate(&om, "obj").await;
        commit_part(&om, "obj", &u, 1, 5 * 1024 * 1024, &hex_encode(&[1u8; 16]), 10).await;
        commit_part(&om, "obj", &u, 2, 1234, &hex_encode(&[2u8; 16]), 20).await;
        let resp = complete(&om, "obj", &u, &[(2, hex_encode(&[2u8; 16])), (1, hex_encode(&[1u8; 16]))]).await;
        assert_eq!(resp.status, oz::Status::InvalidPartOrder as i32);

        // Empty parts list -> InvalidPart.
        let om = CompliantOm::new(test_pipeline());
        let u = initiate(&om, "obj").await;
        let resp = complete(&om, "obj", &u, &[]).await;
        assert_eq!(resp.status, oz::Status::InvalidPart as i32);

        // Unknown upload -> NoSuchMultipartUpload.
        let om = CompliantOm::new(test_pipeline());
        let resp = complete(&om, "obj", "upload-404", &[(1, hex_encode(&[1u8; 16]))]).await;
        assert_eq!(resp.status, oz::Status::NoSuchMultipartUploadError as i32);
    }

    #[tokio::test]
    async fn abort_is_idempotent_and_removes_upload() {
        let om = CompliantOm::new(test_pipeline());
        let u = initiate(&om, "obj").await;
        assert_eq!(abort_req(&om, "obj", &u).await.status, oz::Status::Ok as i32);
        // Second abort is a lenient no-op success.
        assert_eq!(abort_req(&om, "obj", &u).await.status, oz::Status::Ok as i32);
        // After abort, committing into the upload fails as NoSuchUpload.
        let resp = commit_part(&om, "obj", &u, 1, 1024, "ab", 10).await;
        assert_eq!(resp.status, oz::Status::NoSuchMultipartUploadError as i32);
    }

    #[tokio::test]
    async fn list_parts_ascending_and_marker() {
        let om = CompliantOm::new(test_pipeline());
        let u = initiate(&om, "obj").await;
        for pn in [2u32, 1, 3] {
            commit_part(&om, "obj", &u, pn, 4096, &hex_encode(&[pn as u8; 16]), pn as i64).await;
        }
        let all = list_parts_req(&om, &u, 0).await;
        assert_eq!(all.iter().map(|p| p.part_number).collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_eq!(all[0].size, 4096);
        assert_eq!(all[0].e_tag.as_deref(), Some(hex_encode(&[1u8; 16]).as_str()));
        let after1 = list_parts_req(&om, &u, 1).await;
        assert_eq!(after1.iter().map(|p| p.part_number).collect::<Vec<_>>(), vec![2, 3]);
    }

    #[tokio::test]
    async fn list_parts_unknown_upload_is_no_such_multipart() {
        let om = CompliantOm::new(test_pipeline());
        let mut req = envelope(oz::Type::ListMultiPartUploadParts);
        req.list_multipart_upload_parts_request = Some(oz::MultipartUploadListPartsRequest {
            volume: "s3v".into(),
            bucket: "bkt".into(),
            key: "obj".into(),
            upload_id: "upload-404".into(),
            part_numbermarker: None,
            max_parts: None,
        });
        let resp = om.submit_request(Request::new(req)).await.unwrap().into_inner();
        assert_eq!(resp.status, oz::Status::NoSuchMultipartUploadError as i32);
    }

    #[tokio::test]
    async fn list_multipart_uploads_prefix_and_sort() {
        let om = CompliantOm::new(test_pipeline());
        let a1 = initiate(&om, "a/one").await;
        let a2 = initiate(&om, "a/two").await;
        let _b = initiate(&om, "b/three").await;
        let a = list_uploads_req(&om, "a/").await;
        assert_eq!(
            a.iter().map(|u| (u.key_name.clone(), u.upload_id.clone())).collect::<Vec<_>>(),
            vec![("a/one".to_string(), a1), ("a/two".to_string(), a2)]
        );
        let all = list_uploads_req(&om, "").await;
        assert_eq!(
            all.iter().map(|u| u.key_name.clone()).collect::<Vec<_>>(),
            vec!["a/one", "a/two", "b/three"]
        );
    }

    #[tokio::test]
    async fn list_keys_prefix_start_after_limit() {
        let om = CompliantOm::new(test_pipeline());
        for key in ["a/1", "a/2", "a/3", "b/1"] {
            commit_plain_key(&om, key, 10, "etag-x").await;
        }
        let r = list_keys_req(&om, "a/", "", 100).await;
        assert_eq!(
            r.key_info.iter().map(|k| k.key_name.clone()).collect::<Vec<_>>(),
            vec!["a/1", "a/2", "a/3"]
        );
        assert_eq!(r.key_info[0].data_size, 10);
        assert_eq!(r.is_truncated, Some(false));
        // start_after excludes a/1.
        let r = list_keys_req(&om, "a/", "a/1", 100).await;
        assert_eq!(
            r.key_info.iter().map(|k| k.key_name.clone()).collect::<Vec<_>>(),
            vec!["a/2", "a/3"]
        );
        // limit caps and flags truncation.
        let r = list_keys_req(&om, "a/", "", 2).await;
        assert_eq!(r.key_info.len(), 2);
        assert_eq!(r.is_truncated, Some(true));
    }

    #[tokio::test]
    async fn object_tagging_round_trips_through_metadata() {
        let om = CompliantOm::new(test_pipeline());
        commit_plain_key(&om, "obj", 10, "e").await;

        // PutObjectTagging with two tags.
        let mut put = envelope(oz::Type::PutObjectTagging);
        let mut ka = key_args("obj");
        ka.tags = vec![
            hdds::KeyValue { key: "zeta".into(), value: Some("z".into()) },
            hdds::KeyValue { key: "alpha".into(), value: Some("a".into()) },
        ];
        put.put_object_tagging_request = Some(oz::PutObjectTaggingRequest { key_args: ka });
        let resp = om.submit_request(Request::new(put)).await.unwrap().into_inner();
        assert_eq!(resp.status, oz::Status::Ok as i32);

        // GetObjectTagging returns them sorted by key.
        let mut get = envelope(oz::Type::GetObjectTagging);
        get.get_object_tagging_request = Some(oz::GetObjectTaggingRequest { key_args: key_args("obj") });
        let tags = om
            .submit_request(Request::new(get))
            .await
            .unwrap()
            .into_inner()
            .get_object_tagging_response
            .unwrap()
            .tags;
        assert_eq!(
            tags.iter().map(|t| (t.key.clone(), t.value.clone().unwrap_or_default())).collect::<Vec<_>>(),
            vec![("alpha".to_string(), "a".to_string()), ("zeta".to_string(), "z".to_string())]
        );

        // Tags round-trip through key metadata under x-amz-tag-, so GetKeyInfo
        // also sees them (and the ETag survives a tag replace).
        let ki = get_key(&om, "obj").await.unwrap();
        assert!(ki.metadata.iter().any(|kv| kv.key == "x-amz-tag-alpha"));
        assert_eq!(
            ki.metadata.iter().find(|kv| kv.key == "ETAG").and_then(|kv| kv.value.as_deref()),
            Some("e")
        );

        // DeleteObjectTagging clears all tags.
        let mut del = envelope(oz::Type::DeleteObjectTagging);
        del.delete_object_tagging_request = Some(oz::DeleteObjectTaggingRequest { key_args: key_args("obj") });
        let resp = om.submit_request(Request::new(del)).await.unwrap().into_inner();
        assert_eq!(resp.status, oz::Status::Ok as i32);
        let mut get = envelope(oz::Type::GetObjectTagging);
        get.get_object_tagging_request = Some(oz::GetObjectTaggingRequest { key_args: key_args("obj") });
        let tags = om
            .submit_request(Request::new(get))
            .await
            .unwrap()
            .into_inner()
            .get_object_tagging_response
            .unwrap()
            .tags;
        assert!(tags.is_empty());

        // Tagging an absent key is KEY_NOT_FOUND.
        let mut put = envelope(oz::Type::PutObjectTagging);
        put.put_object_tagging_request = Some(oz::PutObjectTaggingRequest { key_args: key_args("ghost") });
        let resp = om.submit_request(Request::new(put)).await.unwrap().into_inner();
        assert_eq!(resp.status, oz::Status::KeyNotFound as i32);
    }
}
