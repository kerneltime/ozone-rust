//! Compliant Ozone Manager client: a DOMAIN method surface over the real
//! `OzoneManagerService.submitRequest(OMRequest) -> OMResponse` contract.
//!
//! This is the single decoupling boundary between the S3 gateway and the OM wire
//! protocol (`ozone_grpc_types::hadoop::ozone`). Each public method builds the
//! `OmRequest` envelope, submits it, checks `status == OK`, and returns small
//! gateway-facing domain structs ([`OpenKey`], [`BlockLocation`], [`KeyMeta`]).
//! The gateway therefore depends on NEITHER the OM proto nor `hadoop::ozone`
//! types; do not let those leak past this module.
//!
//! # Scope
//! The full S3-object surface the gateway needs:
//! - The CORE path: bucket head/create/delete/list and the key write/read/delete
//!   cycle (`create_key` -> `allocate_block`* -> `commit_key`, then
//!   `get_key_info` / `delete_key`).
//! - Multipart: `initiate_multipart` -> `create_multipart_part_key` ->
//!   `commit_part` -> `complete_multipart` (or `abort_multipart`), plus
//!   `list_parts` and `list_multipart_uploads`.
//! - Key listing (`list_keys`) and object tagging (`get_object_tagging` /
//!   `put_object_tagging`).
//!
//! # Invariants enforced here
//! - **W1 status-checked.** [`OzoneOmClient::check`] rejects any non-OK
//!   `OmResponse` BEFORE its sub-response is read. The `Status` enum is 1-based,
//!   so a zero/default status is NOT success; a non-OK status with a stale
//!   sub-message must never be treated as success.
//! - **W3 EC slot fidelity.** [`key_location_to_block`] takes each member's
//!   replica index from the pipeline's `member_replica_indexes[i]`, NEVER from
//!   the member's position `i`. A position-based mapping would silently swap
//!   data and parity shards on a non-identity pipeline, corrupting reads.
//! - **W4 principal attested, never forged.** The envelope stamps
//!   `s3Authentication.accessId` with the proxy-attested principal as-is; the
//!   client neither synthesizes nor verifies it.

// `OmError` wraps `tonic::Status` (~176 bytes), so any `Result<_, OmError>` over
// a small Ok type (a `BlockLocation`, an EC config) trips `result_large_err`.
// Boxing the error would just add an unbox at every call site for no real win;
// the error type is intentionally `Status`-shaped (see the sibling
// `ozone-scm-client` precedent). Allow it module-wide.
#![allow(clippy::result_large_err)]

use std::str::FromStr;

use ozone_grpc_types::hadoop::hdds as hdds;
use ozone_grpc_types::hadoop::ozone as oz;
use ozone_types::{EcCodec, EcReplicationConfig};
use tonic::transport::Channel;

/// The port name the Rust datanode advertises for shard I/O (it registers a
/// `REPLICATION` port to SCM in Track 2). [`key_location_to_block`] reads this
/// port off each pipeline member to build the dial endpoint.
const REPLICATION_PORT_NAME: &str = "REPLICATION";

/// One EC slot mapped to the datanode that holds it.
///
/// `replica_index` is 1-based (data shards `1..=k`, parity `k+1..=k+p`) and is
/// taken from the pipeline's `member_replica_indexes`, not the member's
/// position. `endpoint` is the datanode's shard-I/O URL, `http://ip:port`,
/// where `port` is the member's `REPLICATION` port value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatanodeSlot {
    /// 1-based EC replica slot this datanode holds.
    pub replica_index: u8,
    /// Datanode shard-I/O endpoint, `http://ip:port`.
    pub endpoint: String,
}

/// A single block of a key: its container/local id, byte extent, EC config, and
/// the per-slot datanode endpoints the gateway reads/writes shards to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockLocation {
    /// Container id of the block.
    pub container_id: u64,
    /// Local id of the block within its container.
    pub local_id: u64,
    /// Byte offset of this block within the key.
    pub offset: u64,
    /// Byte length of this block.
    pub length: u64,
    /// Erasure-coding parameters for this block's pipeline.
    pub ec: EcReplicationConfig,
    /// EC slot -> datanode endpoint, one per pipeline member.
    pub members: Vec<DatanodeSlot>,
}

/// Result of [`OzoneOmClient::create_key`]: the open-session id to echo on
/// follow-up `allocate_block`/`commit_key` calls, plus any pre-allocated blocks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenKey {
    /// The OM open-session id (the `CreateKeyResponse.id` cookie).
    pub client_id: u64,
    /// Blocks the OM pre-allocated for the first write(s).
    pub blocks: Vec<BlockLocation>,
}

/// Result of [`OzoneOmClient::get_key_info`]: the committed object's size, ETag,
/// user metadata, modification time, and block layout for reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyMeta {
    /// Total object size in bytes.
    pub size: u64,
    /// The object ETag, from `metadata["ETAG"]` if present.
    pub etag: Option<String>,
    /// User metadata as `(key, value)` pairs (includes any `ETAG` entry).
    pub metadata: Vec<(String, String)>,
    /// Last-modified time in epoch milliseconds (`KeyInfo.modification_time`).
    /// The gateway surfaces this as the S3 `Last-Modified` and uses it for
    /// date-conditional requests; `KeyListing` has no analog because S3
    /// `ListObjectsV2` derives each entry's `LastModified` from the same field
    /// the gateway already folds in.
    pub modification_time: u64,
    /// The committed block layout (W2: what was actually written).
    pub blocks: Vec<BlockLocation>,
}

/// One entry of [`OzoneOmClient::list_keys`]: a committed object's name, size,
/// and ETag. The gateway shapes this into an S3 `ListObjectsV2` `Contents` item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyListing {
    /// The object's key name.
    pub key: String,
    /// Object size in bytes.
    pub size: u64,
    /// The object ETag, from `metadata["ETAG"]` if present.
    pub etag: Option<String>,
}

/// One entry of [`OzoneOmClient::list_parts`]: a previously committed multipart
/// part. The gateway shapes this into an S3 `ListParts` `Part` element.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartListing {
    /// 1-based part number.
    pub part_number: u32,
    /// The part's S3 ETag (per-part MD5 as lowercase hex), if the OM recorded one.
    pub etag: Option<String>,
    /// Part size in bytes.
    pub size: u64,
}

/// One entry of [`OzoneOmClient::list_multipart_uploads`]: an in-flight upload's
/// key and id. The gateway shapes this into an S3 `ListMultipartUploads` upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadListing {
    /// The key the upload targets.
    pub key: String,
    /// The OM's upload id for this in-flight upload.
    pub upload_id: String,
}

/// Errors returned by [`OzoneOmClient`].
#[derive(Debug, thiserror::Error)]
pub enum OmError {
    /// Failed to establish the transport channel during
    /// [`OzoneOmClient::connect`].
    #[error("failed to connect to OM: {0}")]
    Connect(#[from] tonic::transport::Error),

    /// An RPC returned a non-OK gRPC [`tonic::Status`] (transport-level failure,
    /// e.g. `UNAVAILABLE`), as opposed to an OM application status.
    #[error("OM RPC failed: {0}")]
    Rpc(tonic::Status),

    /// The OM reported `KEY_NOT_FOUND` (S3 `NoSuchKey`).
    #[error("key not found")]
    NotFound,

    /// The OM reported `BUCKET_NOT_FOUND` (S3 `NoSuchBucket`).
    #[error("bucket not found")]
    BucketNotFound,

    /// The OM reported `NO_SUCH_MULTIPART_UPLOAD_ERROR` (S3 `NoSuchUpload`): a
    /// commit/complete/list against an upload id the OM does not know.
    #[error("no such multipart upload")]
    NoSuchUpload,

    /// The OM reported `INVALID_PART` (S3 `InvalidPart`): a completed part number
    /// names a part that was never uploaded.
    #[error("invalid multipart part")]
    InvalidPart,

    /// The OM reported `INVALID_PART_ORDER` (S3 `InvalidPartOrder`): the completed
    /// part numbers are not strictly ascending / contain duplicates.
    #[error("invalid multipart part order")]
    InvalidPartOrder,

    /// The OM reported `ENTITY_TOO_SMALL` (S3 `EntityTooSmall`): a non-last
    /// multipart part is below the 5 MiB minimum.
    #[error("multipart part too small")]
    EntityTooSmall,

    /// The OM reported some other non-OK application status. Carries the 1-based
    /// `Status` code and the OM's message for the caller to classify.
    #[error("OM returned status {status}: {message}")]
    Om {
        /// The 1-based `Status` enum value the OM returned.
        status: i32,
        /// The OM's human-readable message (may be empty).
        message: String,
    },

    /// An OK response was missing an expected field (e.g. a sub-response, a
    /// block id, or the EC config). This is an OM-contract violation, not a
    /// normal outcome. The `&'static str` names the missing field.
    #[error("OM response missing expected field: {0}")]
    Missing(&'static str),
}

/// Map a `tonic::Status` from an RPC into [`OmError::Rpc`]. RPC-level failures
/// are transport problems (the OM never replied with a body); OM application
/// statuses are handled separately in [`OzoneOmClient::check`].
impl From<tonic::Status> for OmError {
    fn from(s: tonic::Status) -> Self {
        OmError::Rpc(s)
    }
}

/// Map one OM [`hdds::EcReplicationConfig`] to the domain
/// [`EcReplicationConfig`]. The OM stores `data`/`parity`/`ec_chunk_size` as
/// `i32` and the codec as a string; this narrows them and parses the codec.
/// A negative/oversized field or an unknown codec is an OM-contract violation.
fn ec_config_from_proto(ec: &hdds::EcReplicationConfig) -> Result<EcReplicationConfig, OmError> {
    let codec = EcCodec::from_str(&ec.codec).map_err(|_| OmError::Missing("ec.codec"))?;
    let data = u8::try_from(ec.data).map_err(|_| OmError::Missing("ec.data"))?;
    let parity = u8::try_from(ec.parity).map_err(|_| OmError::Missing("ec.parity"))?;
    let chunk = u32::try_from(ec.ec_chunk_size).map_err(|_| OmError::Missing("ec.chunkSize"))?;
    Ok(EcReplicationConfig {
        data,
        parity,
        ec_chunk_size: chunk,
        codec,
    })
}

/// Convert one OM [`oz::KeyLocation`] into a domain [`BlockLocation`] (W3 core).
///
/// Pulls `container_id`/`local_id` from `block_id.container_block_id`, the EC
/// config from the pipeline, and zips `pipeline.members` with
/// `pipeline.member_replica_indexes` so each [`DatanodeSlot::replica_index`]
/// comes from the parallel index array, NOT the member's position. The endpoint
/// is `http://<ip>:<REPLICATION port>`.
///
/// Errors with [`OmError::Missing`] if the location lacks a pipeline, the
/// members/indexes arrays disagree in length, a member lacks a `REPLICATION`
/// port, or the EC config is absent/invalid.
fn key_location_to_block(loc: &oz::KeyLocation) -> Result<BlockLocation, OmError> {
    let cbid = &loc.block_id.container_block_id;
    let container_id = u64::try_from(cbid.container_id).map_err(|_| OmError::Missing("container_id"))?;
    let local_id = u64::try_from(cbid.local_id).map_err(|_| OmError::Missing("local_id"))?;

    let pipeline = loc
        .pipeline
        .as_ref()
        .ok_or(OmError::Missing("pipeline"))?;
    let ec_proto = pipeline
        .ec_replication_config
        .as_ref()
        .ok_or(OmError::Missing("pipeline.ec"))?;
    let ec = ec_config_from_proto(ec_proto)?;

    if pipeline.members.len() != pipeline.member_replica_indexes.len() {
        return Err(OmError::Missing("pipeline.member_replica_indexes"));
    }

    let mut members = Vec::with_capacity(pipeline.members.len());
    for (dd, idx) in pipeline
        .members
        .iter()
        .zip(pipeline.member_replica_indexes.iter())
    {
        let replica_index = u8::try_from(*idx).map_err(|_| OmError::Missing("replica_index"))?;
        let port = dd
            .ports
            .iter()
            .find(|p| p.name == REPLICATION_PORT_NAME)
            .ok_or(OmError::Missing("REPLICATION port"))?;
        members.push(DatanodeSlot {
            replica_index,
            endpoint: format!("http://{}:{}", dd.ip_address, port.value),
        });
    }

    Ok(BlockLocation {
        container_id,
        local_id,
        offset: loc.offset,
        length: loc.length,
        ec,
        members,
    })
}

/// Build the OM `KeyLocation` to send back on `CommitKey` from a domain
/// [`BlockLocation`]. The OM only needs `block_id` + `offset` + `length` to
/// record the committed layout; the pipeline is omitted (the OM owns placement
/// and ignores a client-sent pipeline on commit).
fn block_to_key_location(block: &BlockLocation) -> oz::KeyLocation {
    oz::KeyLocation {
        block_id: hdds::BlockId {
            container_block_id: hdds::ContainerBlockId {
                container_id: block.container_id as i64,
                local_id: block.local_id as i64,
            },
            ..Default::default()
        },
        offset: block.offset,
        length: block.length,
        ..Default::default()
    }
}

/// Collect a key's blocks from its (latest) `KeyLocationList`. A `KeyInfo`
/// carries versioned location lists; for the S3 path we read the last one.
fn blocks_of_key_info(ki: &oz::KeyInfo) -> Result<Vec<BlockLocation>, OmError> {
    let list = match ki.key_location_list.last() {
        Some(l) => l,
        None => return Ok(Vec::new()),
    };
    list.key_locations.iter().map(key_location_to_block).collect()
}

/// Async domain client for the real Ozone Manager `submitRequest` contract.
///
/// Holds one `tonic` [`Channel`] plus the stable `client_id` and attested
/// `principal` every request carries. Construct with [`OzoneOmClient::connect`]
/// (eager dial) or [`OzoneOmClient::from_channel`] (adopt a channel). The
/// `client_id` is supplied by the caller (the gateway's session UUID) and MUST
/// be stable for the life of the client; this type never derives it from a clock
/// or randomness.
#[derive(Debug, Clone)]
pub struct OzoneOmClient {
    inner: oz::ozone_manager_service_client::OzoneManagerServiceClient<Channel>,
    client_id: String,
    principal: String,
}

impl OzoneOmClient {
    /// Dial `endpoint` and return a ready client stamping `client_id` /
    /// `principal` on every request.
    ///
    /// Makes exactly one connection attempt; on failure returns
    /// [`OmError::Connect`] and does not retry. `client_id` is the gateway's
    /// stable session id; `principal` is the proxy-attested caller used as
    /// `s3Authentication.accessId`.
    pub async fn connect(
        endpoint: impl Into<String>,
        client_id: impl Into<String>,
        principal: impl Into<String>,
    ) -> Result<Self, OmError> {
        let channel = tonic::transport::Endpoint::from_shared(endpoint.into())?
            .connect()
            .await?;
        Ok(Self::from_channel(channel, client_id, principal))
    }

    /// Wrap an already-constructed [`Channel`]. No network I/O happens here.
    /// `client_id` / `principal` are stamped on every request (see
    /// [`OzoneOmClient::connect`]).
    pub fn from_channel(
        channel: Channel,
        client_id: impl Into<String>,
        principal: impl Into<String>,
    ) -> Self {
        Self {
            inner: oz::ozone_manager_service_client::OzoneManagerServiceClient::new(channel),
            client_id: client_id.into(),
            principal: principal.into(),
        }
    }

    /// Build the `OmRequest` envelope for `cmd_type`: the stable `client_id`,
    /// `version = 1`, and `s3Authentication.accessId = principal` (security-off
    /// auth, W4/A1 — no signature is set). The caller fills in the one typed
    /// sub-request field before submitting.
    fn envelope(&self, cmd_type: oz::Type) -> oz::OmRequest {
        oz::OmRequest {
            cmd_type: cmd_type as i32,
            client_id: self.client_id.clone(),
            version: Some(1),
            s3_authentication: Some(oz::S3Authentication {
                access_id: Some(self.principal.clone()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// W1: reject any non-OK `OmResponse` before its sub-response is read. Maps
    /// `KEY_NOT_FOUND` -> [`OmError::NotFound`], `BUCKET_NOT_FOUND` ->
    /// [`OmError::BucketNotFound`], and any other non-OK status ->
    /// [`OmError::Om`]. On OK, returns the response for sub-field extraction.
    fn check(resp: oz::OmResponse) -> Result<oz::OmResponse, OmError> {
        if resp.status == oz::Status::Ok as i32 {
            return Ok(resp);
        }
        if resp.status == oz::Status::KeyNotFound as i32 {
            return Err(OmError::NotFound);
        }
        if resp.status == oz::Status::BucketNotFound as i32 {
            return Err(OmError::BucketNotFound);
        }
        if resp.status == oz::Status::NoSuchMultipartUploadError as i32 {
            return Err(OmError::NoSuchUpload);
        }
        if resp.status == oz::Status::InvalidPart as i32 {
            return Err(OmError::InvalidPart);
        }
        if resp.status == oz::Status::InvalidPartOrder as i32 {
            return Err(OmError::InvalidPartOrder);
        }
        if resp.status == oz::Status::EntityTooSmall as i32 {
            return Err(OmError::EntityTooSmall);
        }
        Err(OmError::Om {
            status: resp.status,
            message: resp.message.unwrap_or_default(),
        })
    }

    /// `InfoBucket` — does the bucket exist? `BUCKET_NOT_FOUND` maps to
    /// `Ok(false)` (a HEAD-bucket miss is not an error to the caller).
    pub async fn head_bucket(&mut self, volume: &str, bucket: &str) -> Result<bool, OmError> {
        let mut req = self.envelope(oz::Type::InfoBucket);
        req.info_bucket_request = Some(oz::InfoBucketRequest {
            volume_name: volume.to_string(),
            bucket_name: bucket.to_string(),
        });
        let resp = self.inner.submit_request(req).await?.into_inner();
        match Self::check(resp) {
            Ok(_) => Ok(true),
            Err(OmError::BucketNotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// `CreateBucket` — create a bucket under `volume`.
    pub async fn create_bucket(&mut self, volume: &str, bucket: &str) -> Result<(), OmError> {
        let mut req = self.envelope(oz::Type::CreateBucket);
        req.create_bucket_request = Some(oz::CreateBucketRequest {
            bucket_info: oz::BucketInfo {
                volume_name: volume.to_string(),
                bucket_name: bucket.to_string(),
                is_version_enabled: false,
                storage_type: hdds::StorageTypeProto::Disk as i32,
                ..Default::default()
            },
        });
        let resp = self.inner.submit_request(req).await?.into_inner();
        Self::check(resp)?;
        Ok(())
    }

    /// `DeleteBucket` — delete `bucket` under `volume`.
    pub async fn delete_bucket(&mut self, volume: &str, bucket: &str) -> Result<(), OmError> {
        let mut req = self.envelope(oz::Type::DeleteBucket);
        req.delete_bucket_request = Some(oz::DeleteBucketRequest {
            volume_name: volume.to_string(),
            bucket_name: bucket.to_string(),
        });
        let resp = self.inner.submit_request(req).await?.into_inner();
        Self::check(resp)?;
        Ok(())
    }

    /// `ListBuckets` — names of the buckets in `volume`.
    pub async fn list_buckets(&mut self, volume: &str) -> Result<Vec<String>, OmError> {
        let mut req = self.envelope(oz::Type::ListBuckets);
        req.list_buckets_request = Some(oz::ListBucketsRequest {
            volume_name: volume.to_string(),
            ..Default::default()
        });
        let resp = Self::check(self.inner.submit_request(req).await?.into_inner())?;
        let lb = resp
            .list_buckets_response
            .ok_or(OmError::Missing("list_buckets_response"))?;
        Ok(lb.bucket_info.into_iter().map(|b| b.bucket_name).collect())
    }

    /// `CreateKey` — open a key for writing under EC `ec` (omit to inherit the
    /// bucket default), returning the open-session id and any pre-allocated
    /// blocks. `size` is the expected total (a hint; the real size is set on
    /// commit).
    pub async fn create_key(
        &mut self,
        volume: &str,
        bucket: &str,
        key: &str,
        ec: Option<EcReplicationConfig>,
        size: u64,
    ) -> Result<OpenKey, OmError> {
        let mut key_args = oz::KeyArgs {
            volume_name: volume.to_string(),
            bucket_name: bucket.to_string(),
            key_name: key.to_string(),
            data_size: Some(size),
            ..Default::default()
        };
        if let Some(ec) = ec {
            key_args.r#type = Some(hdds::ReplicationType::Ec as i32);
            key_args.ec_replication_config = Some(hdds::EcReplicationConfig {
                data: ec.data as i32,
                parity: ec.parity as i32,
                codec: ec.codec.to_string(),
                ec_chunk_size: ec.ec_chunk_size as i32,
            });
        }
        let mut req = self.envelope(oz::Type::CreateKey);
        req.create_key_request = Some(oz::CreateKeyRequest {
            key_args,
            ..Default::default()
        });
        let resp = Self::check(self.inner.submit_request(req).await?.into_inner())?;
        let ck = resp
            .create_key_response
            .ok_or(OmError::Missing("create_key_response"))?;
        let client_id = ck.id.ok_or(OmError::Missing("create_key_response.id"))?;
        let ki = ck.key_info.ok_or(OmError::Missing("create_key_response.key_info"))?;
        Ok(OpenKey {
            client_id,
            blocks: blocks_of_key_info(&ki)?,
        })
    }

    /// `AllocateBlock` — request one more block for the open key identified by
    /// `client_id`.
    pub async fn allocate_block(
        &mut self,
        volume: &str,
        bucket: &str,
        key: &str,
        client_id: u64,
    ) -> Result<BlockLocation, OmError> {
        let mut req = self.envelope(oz::Type::AllocateBlock);
        req.allocate_block_request = Some(oz::AllocateBlockRequest {
            key_args: oz::KeyArgs {
                volume_name: volume.to_string(),
                bucket_name: bucket.to_string(),
                key_name: key.to_string(),
                ..Default::default()
            },
            client_id,
            ..Default::default()
        });
        let resp = Self::check(self.inner.submit_request(req).await?.into_inner())?;
        let loc = resp
            .allocate_block_response
            .and_then(|r| r.key_location)
            .ok_or(OmError::Missing("allocate_block_response.key_location"))?;
        key_location_to_block(&loc)
    }

    /// `CommitKey` — finalize the open key with the ACTUAL written `blocks`
    /// (W2), the total `size`, an optional `etag` (stored under
    /// `metadata["ETAG"]`), and any user `metadata`.
    ///
    /// The arity mirrors the OM commit contract (the open-session id, the real
    /// block layout, the size, the ETag, and the user metadata are all distinct
    /// inputs the gateway supplies); collapsing them into a struct would only
    /// move the same fields behind one more type for no clarity gain.
    #[allow(clippy::too_many_arguments)]
    pub async fn commit_key(
        &mut self,
        volume: &str,
        bucket: &str,
        key: &str,
        client_id: u64,
        blocks: &[BlockLocation],
        size: u64,
        etag: Option<&str>,
        metadata: &[(String, String)],
    ) -> Result<(), OmError> {
        let mut md: Vec<hdds::KeyValue> = metadata
            .iter()
            .map(|(k, v)| hdds::KeyValue {
                key: k.clone(),
                value: Some(v.clone()),
            })
            .collect();
        if let Some(etag) = etag {
            md.push(hdds::KeyValue {
                key: "ETAG".to_string(),
                value: Some(etag.to_string()),
            });
        }
        let key_args = oz::KeyArgs {
            volume_name: volume.to_string(),
            bucket_name: bucket.to_string(),
            key_name: key.to_string(),
            data_size: Some(size),
            r#type: Some(hdds::ReplicationType::Ec as i32),
            key_locations: blocks.iter().map(block_to_key_location).collect(),
            metadata: md,
            ..Default::default()
        };
        let mut req = self.envelope(oz::Type::CommitKey);
        req.commit_key_request = Some(oz::CommitKeyRequest {
            key_args,
            client_id,
            ..Default::default()
        });
        let resp = self.inner.submit_request(req).await?.into_inner();
        Self::check(resp)?;
        Ok(())
    }

    /// `GetKeyInfo` — resolve a committed key's size, ETag, metadata, and block
    /// layout for reads (the S3 read entrypoint, R1). `KEY_NOT_FOUND` maps to
    /// [`OmError::NotFound`].
    pub async fn get_key_info(
        &mut self,
        volume: &str,
        bucket: &str,
        key: &str,
    ) -> Result<KeyMeta, OmError> {
        let mut req = self.envelope(oz::Type::GetKeyInfo);
        req.get_key_info_request = Some(oz::GetKeyInfoRequest {
            key_args: oz::KeyArgs {
                volume_name: volume.to_string(),
                bucket_name: bucket.to_string(),
                key_name: key.to_string(),
                ..Default::default()
            },
            ..Default::default()
        });
        let resp = Self::check(self.inner.submit_request(req).await?.into_inner())?;
        let ki = resp
            .get_key_info_response
            .and_then(|r| r.key_info)
            .ok_or(OmError::Missing("get_key_info_response.key_info"))?;
        let metadata: Vec<(String, String)> = ki
            .metadata
            .iter()
            .map(|kv| (kv.key.clone(), kv.value.clone().unwrap_or_default()))
            .collect();
        let etag = metadata
            .iter()
            .find(|(k, _)| k == "ETAG")
            .map(|(_, v)| v.clone());
        Ok(KeyMeta {
            size: ki.data_size,
            etag,
            metadata,
            modification_time: ki.modification_time,
            blocks: blocks_of_key_info(&ki)?,
        })
    }

    /// `DeleteKey` — delete a committed key. Lenient at the OM (deleting an
    /// absent key is not surfaced as an error here).
    pub async fn delete_key(&mut self, volume: &str, bucket: &str, key: &str) -> Result<(), OmError> {
        let mut req = self.envelope(oz::Type::DeleteKey);
        req.delete_key_request = Some(oz::DeleteKeyRequest {
            key_args: oz::KeyArgs {
                volume_name: volume.to_string(),
                bucket_name: bucket.to_string(),
                key_name: key.to_string(),
                ..Default::default()
            },
        });
        let resp = self.inner.submit_request(req).await?.into_inner();
        Self::check(resp)?;
        Ok(())
    }

    /// `InitiateMultiPartUpload` — begin a multipart upload, returning the OM's
    /// `multipart_upload_id`. The OM owns the in-flight upload from here until
    /// [`Self::complete_multipart`] or [`Self::abort_multipart`].
    pub async fn initiate_multipart(
        &mut self,
        volume: &str,
        bucket: &str,
        key: &str,
    ) -> Result<String, OmError> {
        let mut req = self.envelope(oz::Type::InitiateMultiPartUpload);
        req.initiate_multi_part_upload_request = Some(oz::MultipartInfoInitiateRequest {
            key_args: oz::KeyArgs {
                volume_name: volume.to_string(),
                bucket_name: bucket.to_string(),
                key_name: key.to_string(),
                ..Default::default()
            },
        });
        let resp = Self::check(self.inner.submit_request(req).await?.into_inner())?;
        let mp = resp
            .initiate_multi_part_upload_response
            .ok_or(OmError::Missing("initiate_multi_part_upload_response"))?;
        Ok(mp.multipart_upload_id)
    }

    /// `CreateKey` for one multipart part — open a per-part write session under
    /// `upload_id`/`part_number`, returning the open-session id and pre-allocated
    /// block(s). This is the normal `CreateKey` with the multipart flags set
    /// (`is_multipart_key`, `multipart_upload_id`, `multipart_number`); the OM
    /// pre-allocates a block exactly as for a plain key.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_multipart_part_key(
        &mut self,
        volume: &str,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        ec: Option<EcReplicationConfig>,
        size: u64,
    ) -> Result<OpenKey, OmError> {
        let mut key_args = oz::KeyArgs {
            volume_name: volume.to_string(),
            bucket_name: bucket.to_string(),
            key_name: key.to_string(),
            data_size: Some(size),
            is_multipart_key: Some(true),
            multipart_upload_id: Some(upload_id.to_string()),
            multipart_number: Some(part_number),
            ..Default::default()
        };
        if let Some(ec) = ec {
            key_args.r#type = Some(hdds::ReplicationType::Ec as i32);
            key_args.ec_replication_config = Some(hdds::EcReplicationConfig {
                data: ec.data as i32,
                parity: ec.parity as i32,
                codec: ec.codec.to_string(),
                ec_chunk_size: ec.ec_chunk_size as i32,
            });
        }
        let mut req = self.envelope(oz::Type::CreateKey);
        req.create_key_request = Some(oz::CreateKeyRequest {
            key_args,
            ..Default::default()
        });
        let resp = Self::check(self.inner.submit_request(req).await?.into_inner())?;
        let ck = resp
            .create_key_response
            .ok_or(OmError::Missing("create_key_response"))?;
        let client_id = ck.id.ok_or(OmError::Missing("create_key_response.id"))?;
        let ki = ck
            .key_info
            .ok_or(OmError::Missing("create_key_response.key_info"))?;
        Ok(OpenKey {
            client_id,
            blocks: blocks_of_key_info(&ki)?,
        })
    }

    /// `CommitMultiPartUpload` — record one written part under `upload_id`.
    ///
    /// Carries the ACTUAL written `blocks` (W2: block_id/offset/length verbatim),
    /// the part `size`, and the part's S3 ETag (`etag_hex`, the per-part MD5 as
    /// lowercase hex) under `metadata["ETAG"]`. Returns the OM's `part_name`.
    /// Maps `NO_SUCH_MULTIPART_UPLOAD_ERROR` -> [`OmError::NoSuchUpload`].
    #[allow(clippy::too_many_arguments)]
    pub async fn commit_part(
        &mut self,
        volume: &str,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        client_id: u64,
        blocks: &[BlockLocation],
        size: u64,
        etag_hex: &str,
    ) -> Result<String, OmError> {
        let key_args = oz::KeyArgs {
            volume_name: volume.to_string(),
            bucket_name: bucket.to_string(),
            key_name: key.to_string(),
            data_size: Some(size),
            r#type: Some(hdds::ReplicationType::Ec as i32),
            key_locations: blocks.iter().map(block_to_key_location).collect(),
            is_multipart_key: Some(true),
            multipart_upload_id: Some(upload_id.to_string()),
            multipart_number: Some(part_number),
            metadata: vec![hdds::KeyValue {
                key: "ETAG".to_string(),
                value: Some(etag_hex.to_string()),
            }],
            ..Default::default()
        };
        let mut req = self.envelope(oz::Type::CommitMultiPartUpload);
        req.commit_multi_part_upload_request = Some(oz::MultipartCommitUploadPartRequest {
            key_args,
            client_id,
        });
        let resp = Self::check(self.inner.submit_request(req).await?.into_inner())?;
        let cp = resp
            .commit_multi_part_upload_response
            .ok_or(OmError::Missing("commit_multi_part_upload_response"))?;
        cp.part_name
            .ok_or(OmError::Missing("commit_multi_part_upload_response.part_name"))
    }

    /// `CompleteMultiPartUpload` — assemble the listed parts into the final key.
    ///
    /// `parts` is `(part_number, part_name, etag_hex)` in the client's intended
    /// order. The OM validates the order/sizes authoritatively (strictly
    /// ascending no-dups -> [`OmError::InvalidPartOrder`], every part present ->
    /// [`OmError::InvalidPart`], every non-last part >= 5 MiB ->
    /// [`OmError::EntityTooSmall`], unknown upload -> [`OmError::NoSuchUpload`]),
    /// stitches the parts' blocks, and returns the AWS multipart ETag. Returns
    /// `(etag, total_size)`.
    pub async fn complete_multipart(
        &mut self,
        volume: &str,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[(u32, String, String)],
    ) -> Result<(String, u64), OmError> {
        let parts_list: Vec<oz::Part> = parts
            .iter()
            .map(|(pn, name, etag)| oz::Part {
                part_number: *pn,
                part_name: name.clone(),
                e_tag: Some(etag.clone()),
            })
            .collect();
        let key_args = oz::KeyArgs {
            volume_name: volume.to_string(),
            bucket_name: bucket.to_string(),
            key_name: key.to_string(),
            is_multipart_key: Some(true),
            multipart_upload_id: Some(upload_id.to_string()),
            ..Default::default()
        };
        let mut req = self.envelope(oz::Type::CompleteMultiPartUpload);
        req.complete_multi_part_upload_request = Some(oz::MultipartUploadCompleteRequest {
            key_args,
            parts_list,
        });
        let resp = Self::check(self.inner.submit_request(req).await?.into_inner())?;
        let cm = resp
            .complete_multi_part_upload_response
            .ok_or(OmError::Missing("complete_multi_part_upload_response"))?;
        let etag = cm
            .hash
            .ok_or(OmError::Missing("complete_multi_part_upload_response.hash"))?;
        // Resolve the final size from the now-committed key (the complete
        // response carries the ETag but not the size).
        let size = self.get_key_info(volume, bucket, key).await?.size;
        Ok((etag, size))
    }

    /// `AbortMultiPartUpload` — discard an in-progress multipart upload. Lenient
    /// at the OM (aborting an unknown upload succeeds), matching S3.
    pub async fn abort_multipart(
        &mut self,
        volume: &str,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), OmError> {
        let mut req = self.envelope(oz::Type::AbortMultiPartUpload);
        req.abort_multi_part_upload_request = Some(oz::MultipartUploadAbortRequest {
            key_args: oz::KeyArgs {
                volume_name: volume.to_string(),
                bucket_name: bucket.to_string(),
                key_name: key.to_string(),
                multipart_upload_id: Some(upload_id.to_string()),
                ..Default::default()
            },
        });
        let resp = self.inner.submit_request(req).await?.into_inner();
        Self::check(resp)?;
        Ok(())
    }

    /// `ListMultiPartUploadParts` — the parts uploaded so far for `upload_id`,
    /// ascending, filtered to part numbers strictly above `part_number_marker`
    /// (pass 0 for the first page). Unknown upload -> [`OmError::NoSuchUpload`].
    pub async fn list_parts(
        &mut self,
        volume: &str,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number_marker: u32,
    ) -> Result<Vec<PartListing>, OmError> {
        let mut req = self.envelope(oz::Type::ListMultiPartUploadParts);
        req.list_multipart_upload_parts_request = Some(oz::MultipartUploadListPartsRequest {
            volume: volume.to_string(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            upload_id: upload_id.to_string(),
            part_numbermarker: Some(part_number_marker),
            max_parts: None,
        });
        let resp = Self::check(self.inner.submit_request(req).await?.into_inner())?;
        let lp = resp
            .list_multipart_upload_parts_response
            .ok_or(OmError::Missing("list_multipart_upload_parts_response"))?;
        Ok(lp
            .parts_list
            .into_iter()
            .map(|p| PartListing {
                part_number: p.part_number,
                etag: p.e_tag,
                size: p.size,
            })
            .collect())
    }

    /// `ListMultipartUploads` — in-flight uploads in `(volume, bucket)` whose key
    /// starts with `prefix`, sorted by `(key, upload_id)`.
    pub async fn list_multipart_uploads(
        &mut self,
        volume: &str,
        bucket: &str,
        prefix: &str,
    ) -> Result<Vec<UploadListing>, OmError> {
        let mut req = self.envelope(oz::Type::ListMultipartUploads);
        req.list_multipart_uploads_request = Some(oz::ListMultipartUploadsRequest {
            volume: volume.to_string(),
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            ..Default::default()
        });
        let resp = Self::check(self.inner.submit_request(req).await?.into_inner())?;
        let lu = resp
            .list_multipart_uploads_response
            .ok_or(OmError::Missing("list_multipart_uploads_response"))?;
        Ok(lu
            .uploads_list
            .into_iter()
            .map(|u| UploadListing {
                key: u.key_name,
                upload_id: u.upload_id,
            })
            .collect())
    }

    /// `ListKeys` — committed objects in `(volume, bucket)` whose name starts
    /// with `prefix` and sorts strictly after `start_after`, ascending, capped at
    /// `limit` (a non-positive `limit` lets the OM apply its default page size).
    /// Each entry's ETag comes from the key's `metadata["ETAG"]` if present.
    pub async fn list_keys(
        &mut self,
        volume: &str,
        bucket: &str,
        prefix: &str,
        start_after: &str,
        limit: i32,
    ) -> Result<Vec<KeyListing>, OmError> {
        let mut req = self.envelope(oz::Type::ListKeys);
        req.list_keys_request = Some(oz::ListKeysRequest {
            volume_name: volume.to_string(),
            bucket_name: bucket.to_string(),
            start_key: Some(start_after.to_string()),
            prefix: Some(prefix.to_string()),
            count: Some(limit),
        });
        let resp = Self::check(self.inner.submit_request(req).await?.into_inner())?;
        let lk = resp
            .list_keys_response
            .ok_or(OmError::Missing("list_keys_response"))?;
        Ok(lk
            .key_info
            .into_iter()
            .map(|ki| {
                let etag = ki
                    .metadata
                    .iter()
                    .find(|kv| kv.key == "ETAG")
                    .and_then(|kv| kv.value.clone());
                KeyListing {
                    key: ki.key_name,
                    size: ki.data_size,
                    etag,
                }
            })
            .collect())
    }

    /// `GetObjectTagging` — the object's S3 tag set as `(key, value)` pairs.
    /// `KEY_NOT_FOUND` maps to [`OmError::NotFound`].
    pub async fn get_object_tagging(
        &mut self,
        volume: &str,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<(String, String)>, OmError> {
        let mut req = self.envelope(oz::Type::GetObjectTagging);
        req.get_object_tagging_request = Some(oz::GetObjectTaggingRequest {
            key_args: oz::KeyArgs {
                volume_name: volume.to_string(),
                bucket_name: bucket.to_string(),
                key_name: key.to_string(),
                ..Default::default()
            },
        });
        let resp = Self::check(self.inner.submit_request(req).await?.into_inner())?;
        let gt = resp
            .get_object_tagging_response
            .ok_or(OmError::Missing("get_object_tagging_response"))?;
        Ok(gt
            .tags
            .into_iter()
            .map(|kv| (kv.key, kv.value.unwrap_or_default()))
            .collect())
    }

    /// `PutObjectTagging` — replace the object's S3 tag set with `tags`. An empty
    /// `tags` clears all tags (the gateway serves `DeleteObjectTagging` this way).
    /// `KEY_NOT_FOUND` maps to [`OmError::NotFound`].
    ///
    /// DESIGN: tags are carried on `KeyArgs.tags` and the OM persists them in the
    /// key metadata under the `x-amz-tag-` prefix (the same place the gateway's
    /// metadata-based tagging reads/writes), so the S3 PUT/GET/DELETE tagging
    /// behavior round-trips whether the gateway reads back via `GetObjectTagging`
    /// or via `GetKeyInfo`.
    pub async fn put_object_tagging(
        &mut self,
        volume: &str,
        bucket: &str,
        key: &str,
        tags: &[(String, String)],
    ) -> Result<(), OmError> {
        let key_args = oz::KeyArgs {
            volume_name: volume.to_string(),
            bucket_name: bucket.to_string(),
            key_name: key.to_string(),
            tags: tags
                .iter()
                .map(|(k, v)| hdds::KeyValue {
                    key: k.clone(),
                    value: Some(v.clone()),
                })
                .collect(),
            ..Default::default()
        };
        let mut req = self.envelope(oz::Type::PutObjectTagging);
        req.put_object_tagging_request = Some(oz::PutObjectTaggingRequest { key_args });
        let resp = self.inner.submit_request(req).await?.into_inner();
        Self::check(resp)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_fixtures::compliant_om::{CompliantOm, CompliantOmPipeline};
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    const VOL: &str = "s3v";
    const BKT: &str = "bkt";
    const PRINCIPAL: &str = "the-principal";
    const CLIENT_ID: &str = "rust-s3g-test-0001";

    fn ec_3_2() -> hdds::EcReplicationConfig {
        hdds::EcReplicationConfig {
            data: 3,
            parity: 2,
            codec: "rs".to_string(),
            ec_chunk_size: 1024 * 1024,
        }
    }

    /// Build a pipeline of `n` datanodes whose `member_replica_indexes` will be
    /// the identity `1..=n` (the fixture assigns slot i+1 to datanodes[i]). Each
    /// member carries a REPLICATION port so the endpoint can be formed.
    fn datanodes(n: u32) -> Vec<hdds::DatanodeDetailsProto> {
        (0..n)
            .map(|i| hdds::DatanodeDetailsProto {
                uuid: Some(format!("dn-{i}")),
                ip_address: format!("10.0.0.{}", i + 1),
                host_name: format!("host-{i}"),
                ports: vec![hdds::Port {
                    name: REPLICATION_PORT_NAME.to_string(),
                    value: 19864 + i,
                }],
                ..Default::default()
            })
            .collect()
    }

    async fn serve(om: CompliantOm) -> Channel {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            Server::builder()
                .add_service(om.into_server())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .ok();
        });
        tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect_lazy()
    }

    async fn client_for(pipeline: CompliantOmPipeline) -> (OzoneOmClient, std::sync::Arc<parking_lot::Mutex<Vec<oz::OmRequest>>>) {
        let om = CompliantOm::new(pipeline);
        let record = om.record();
        let channel = serve(om).await;
        (
            OzoneOmClient::from_channel(channel, CLIENT_ID, PRINCIPAL),
            record,
        )
    }

    #[tokio::test]
    async fn create_key_maps_slots_from_member_replica_indexes() {
        let pipeline = CompliantOmPipeline {
            datanodes: datanodes(5),
            ec: ec_3_2(),
        };
        let (mut client, _record) = client_for(pipeline).await;

        let open = client
            .create_key(VOL, BKT, "obj", Some(EcReplicationConfig::RS_3_2_1MIB), 0)
            .await
            .unwrap();
        assert_eq!(open.client_id, 1);
        assert_eq!(open.blocks.len(), 1);
        let block = &open.blocks[0];
        assert_eq!(block.members.len(), 5);
        // Identity pipeline: slot i+1 on datanodes[i], endpoint from its REPLICATION port.
        for (i, slot) in block.members.iter().enumerate() {
            assert_eq!(slot.replica_index, (i + 1) as u8);
            assert_eq!(slot.endpoint, format!("http://10.0.0.{}:{}", i + 1, 19864 + i as u32));
        }
        assert_eq!(block.ec, EcReplicationConfig::RS_3_2_1MIB);
    }

    #[tokio::test]
    async fn commit_then_get_key_info_round_trips_size_and_blocks() {
        let pipeline = CompliantOmPipeline {
            datanodes: datanodes(5),
            ec: ec_3_2(),
        };
        let (mut client, _record) = client_for(pipeline).await;

        let open = client.create_key(VOL, BKT, "obj", None, 0).await.unwrap();
        // Pretend the gateway wrote the pre-allocated block at length 4096.
        let mut written = open.blocks.clone();
        written[0].length = 4096;

        client
            .commit_key(
                VOL,
                BKT,
                "obj",
                open.client_id,
                &written,
                4096,
                Some("deadbeef"),
                &[("Content-Type".to_string(), "text/plain".to_string())],
            )
            .await
            .unwrap();

        let meta = client.get_key_info(VOL, BKT, "obj").await.unwrap();
        assert_eq!(meta.size, 4096);
        assert_eq!(meta.etag.as_deref(), Some("deadbeef"));
        assert_eq!(meta.blocks.len(), 1);
        // W2: the committed block id/length round-trip.
        assert_eq!(meta.blocks[0].container_id, written[0].container_id);
        assert_eq!(meta.blocks[0].local_id, written[0].local_id);
        assert_eq!(meta.blocks[0].length, 4096);
        assert!(meta
            .metadata
            .iter()
            .any(|(k, v)| k == "Content-Type" && v == "text/plain"));
    }

    #[tokio::test]
    async fn get_key_info_missing_is_not_found() {
        let pipeline = CompliantOmPipeline {
            datanodes: datanodes(5),
            ec: ec_3_2(),
        };
        let (mut client, _record) = client_for(pipeline).await;
        let err = client.get_key_info(VOL, BKT, "nope").await.unwrap_err();
        assert!(
            matches!(err, OmError::NotFound),
            "missing key must map to NotFound, got {err:?}"
        );
    }

    #[tokio::test]
    async fn head_bucket_true_false_and_lifecycle() {
        let pipeline = CompliantOmPipeline {
            datanodes: datanodes(5),
            ec: ec_3_2(),
        };
        let (mut client, _record) = client_for(pipeline).await;

        assert!(!client.head_bucket(VOL, BKT).await.unwrap(), "absent bucket");
        client.create_bucket(VOL, BKT).await.unwrap();
        assert!(client.head_bucket(VOL, BKT).await.unwrap(), "now present");
        assert_eq!(client.list_buckets(VOL).await.unwrap(), vec![BKT.to_string()]);
        client.delete_bucket(VOL, BKT).await.unwrap();
        assert!(!client.head_bucket(VOL, BKT).await.unwrap(), "deleted");
    }

    #[tokio::test]
    async fn envelope_carries_cmd_type_and_attested_principal() {
        let pipeline = CompliantOmPipeline {
            datanodes: datanodes(5),
            ec: ec_3_2(),
        };
        let (mut client, record) = client_for(pipeline).await;
        client
            .create_key(VOL, BKT, "obj", None, 0)
            .await
            .unwrap();

        let rec = record.lock();
        assert_eq!(rec.len(), 1);
        let env = &rec[0];
        assert_eq!(env.cmd_type, oz::Type::CreateKey as i32);
        assert_eq!(env.client_id, CLIENT_ID);
        assert_eq!(
            env.s3_authentication.as_ref().unwrap().access_id.as_deref(),
            Some(PRINCIPAL),
            "W4: the attested principal rides s3Authentication.accessId"
        );
        // A1: no signature is set under security-off auth.
        assert!(env.s3_authentication.as_ref().unwrap().signature.is_none());
    }

    /// Build an `oz::KeyLocation` whose pipeline pairs the given members with an
    /// explicit (possibly non-identity) `member_replica_indexes` array, so the
    /// W3 mapping can be exercised against orderings the fixture never emits.
    fn key_location_with_indexes(
        members: Vec<hdds::DatanodeDetailsProto>,
        indexes: Vec<u32>,
    ) -> oz::KeyLocation {
        oz::KeyLocation {
            block_id: hdds::BlockId {
                container_block_id: hdds::ContainerBlockId {
                    container_id: 1,
                    local_id: 1,
                },
                ..Default::default()
            },
            offset: 0,
            length: 0,
            pipeline: Some(hdds::Pipeline {
                members,
                member_replica_indexes: indexes,
                ec_replication_config: Some(ec_3_2()),
                id: hdds::PipelineId {
                    id: Some("pl-1".to_string()),
                    ..Default::default()
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Load-bearing by mutation (W3): with a NON-identity pipeline whose
    /// `member_replica_indexes` are `[2, 1, 3, 4, 5]`, the mapping MUST take each
    /// slot from the index array, NOT the member's position. A position-based
    /// mapping would bind members[0] -> slot 1 and members[1] -> slot 2; here
    /// the indexes say members[0] -> slot 2 and members[1] -> slot 1, so this
    /// fails if the impl ever uses position instead of member_replica_indexes.
    #[test]
    fn ec_mapping_follows_member_replica_indexes_not_position() {
        let members = datanodes(5);
        // members[0] = dn-0 (ip .1, port 19864), members[1] = dn-1 (ip .2, 19865).
        let loc = key_location_with_indexes(members, vec![2, 1, 3, 4, 5]);
        let block = key_location_to_block(&loc).unwrap();

        let by_slot = |s: u8| {
            block
                .members
                .iter()
                .find(|m| m.replica_index == s)
                .unwrap_or_else(|| panic!("no member at slot {s}"))
        };
        // The index array binds slot 2 to members[0] (dn-0, ip .1) and slot 1 to
        // members[1] (dn-1, ip .2). A position-based bug would invert both.
        assert_eq!(by_slot(2).endpoint, "http://10.0.0.1:19864", "slot 2 -> members[0] (dn-0)");
        assert_eq!(by_slot(1).endpoint, "http://10.0.0.2:19865", "slot 1 -> members[1] (dn-1)");
        // And the parallel-array order is preserved on the domain side.
        assert_eq!(block.members[0].replica_index, 2);
        assert_eq!(block.members[1].replica_index, 1);
    }

    /// A `KeyLocation` missing the REPLICATION port cannot form a dial endpoint;
    /// the mapping must surface that as a contract violation, not silently drop
    /// the member or use a wrong port.
    #[test]
    fn ec_mapping_requires_replication_port() {
        let mut members = datanodes(2);
        members[0].ports.clear();
        let loc = key_location_with_indexes(members, vec![1, 2]);
        assert!(matches!(
            key_location_to_block(&loc),
            Err(OmError::Missing("REPLICATION port"))
        ));
    }

    /// Lowercase-hex encode, mirroring the `{:x}` MD5 formatting the OM/gateway
    /// use for a part's S3 ETag. The tests build part ETags from chosen raw
    /// digests so the multipart-ETag rollup is independently checkable.
    fn hex_encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Drive a full multipart write through `CompliantOm`: open the upload,
    /// open+commit two parts (the gateway would EC-write between), complete, and
    /// confirm `get_key_info` returns the stitched size and the AWS multipart
    /// ETag. The ETag is INDEPENDENTLY recomputed here as `md5(raw1 ++ raw2)-2`
    /// from the chosen per-part raw digests; this fails if the OM rolls up the
    /// HEX text instead of the raw bytes, or stitches the parts out of order.
    #[tokio::test]
    async fn multipart_happy_path_stitches_size_and_aws_etag() {
        use md5::{Digest, Md5};

        let pipeline = CompliantOmPipeline {
            datanodes: datanodes(5),
            ec: ec_3_2(),
        };
        let (mut client, _record) = client_for(pipeline).await;

        let upload_id = client.initiate_multipart(VOL, BKT, "obj").await.unwrap();
        assert_eq!(upload_id, "upload-1");

        // Two parts; part 1 (non-last) must clear the 5 MiB floor, part 2 (last)
        // may be small. Distinct raw 16-byte digests so the rollup order matters.
        let part1_size: u64 = 5 * 1024 * 1024;
        let part2_size: u64 = 1234;
        let raw1 = [0xAAu8; 16];
        let raw2 = [0xBBu8; 16];
        let etag1 = hex_encode(&raw1);
        let etag2 = hex_encode(&raw2);

        // Part 1.
        let open1 = client
            .create_multipart_part_key(VOL, BKT, "obj", &upload_id, 1, None, part1_size)
            .await
            .unwrap();
        let mut blocks1 = open1.blocks.clone();
        blocks1[0].length = part1_size;
        let name1 = client
            .commit_part(VOL, BKT, "obj", &upload_id, 1, open1.client_id, &blocks1, part1_size, &etag1)
            .await
            .unwrap();
        assert_eq!(name1, format!("obj-{upload_id}-1"));

        // Part 2.
        let open2 = client
            .create_multipart_part_key(VOL, BKT, "obj", &upload_id, 2, None, part2_size)
            .await
            .unwrap();
        let mut blocks2 = open2.blocks.clone();
        blocks2[0].length = part2_size;
        let name2 = client
            .commit_part(VOL, BKT, "obj", &upload_id, 2, open2.client_id, &blocks2, part2_size, &etag2)
            .await
            .unwrap();

        // Independently recompute the AWS multipart ETag: md5 over the raw
        // per-part digests, in part order, then "-<count>".
        let mut hasher = Md5::new();
        hasher.update(raw1);
        hasher.update(raw2);
        let expected_etag = format!("{:x}-2", hasher.finalize());

        let (etag, size) = client
            .complete_multipart(
                VOL,
                BKT,
                "obj",
                &upload_id,
                &[(1, name1, etag1), (2, name2, etag2)],
            )
            .await
            .unwrap();
        assert_eq!(size, part1_size + part2_size);
        assert_eq!(etag, expected_etag, "multipart ETag must be md5(raw||raw)-N");

        // The completed object is now a normal key: stitched size, both parts'
        // blocks, and the multipart ETag in metadata.
        let meta = client.get_key_info(VOL, BKT, "obj").await.unwrap();
        assert_eq!(meta.size, part1_size + part2_size);
        assert_eq!(meta.etag.as_deref(), Some(expected_etag.as_str()));
        assert_eq!(meta.blocks.len(), 2, "one block group per part, in order");
        assert_eq!(meta.blocks[0].length, part1_size);
        assert_eq!(meta.blocks[1].length, part2_size);
    }

    /// Complete-time validation maps each OM status to the right [`OmError`], so
    /// the gateway can emit the matching S3 code. Uses three independent uploads.
    #[tokio::test]
    async fn complete_validation_maps_statuses() {
        let pipeline = CompliantOmPipeline {
            datanodes: datanodes(5),
            ec: ec_3_2(),
        };
        let (mut client, _record) = client_for(pipeline).await;

        // Helper to open an upload and commit one part of a given size.
        async fn part(
            client: &mut OzoneOmClient,
            upload_id: &str,
            pn: u32,
            size: u64,
        ) -> (String, String) {
            let raw = [pn as u8; 16];
            let etag = hex_encode(&raw);
            let open = client
                .create_multipart_part_key(VOL, BKT, "obj", upload_id, pn, None, size)
                .await
                .unwrap();
            let mut blocks = open.blocks.clone();
            blocks[0].length = size;
            let name = client
                .commit_part(VOL, BKT, "obj", upload_id, pn, open.client_id, &blocks, size, &etag)
                .await
                .unwrap();
            (name, etag)
        }

        // EntityTooSmall: a non-last part below 5 MiB.
        let u1 = client.initiate_multipart(VOL, BKT, "obj").await.unwrap();
        let (n1, e1) = part(&mut client, &u1, 1, 1024).await;
        let (n2, e2) = part(&mut client, &u1, 2, 1024).await;
        let err = client
            .complete_multipart(VOL, BKT, "obj", &u1, &[(1, n1, e1), (2, n2, e2)])
            .await
            .unwrap_err();
        assert!(matches!(err, OmError::EntityTooSmall), "got {err:?}");

        // InvalidPart: name a part number that was never uploaded.
        let u2 = client.initiate_multipart(VOL, BKT, "obj").await.unwrap();
        let (n1, e1) = part(&mut client, &u2, 1, 5 * 1024 * 1024).await;
        let err = client
            .complete_multipart(
                VOL,
                BKT,
                "obj",
                &u2,
                &[(1, n1, e1), (2, "missing".into(), hex_encode(&[2u8; 16]))],
            )
            .await
            .unwrap_err();
        assert!(matches!(err, OmError::InvalidPart), "got {err:?}");

        // InvalidPartOrder: descending / duplicate part numbers.
        let u3 = client.initiate_multipart(VOL, BKT, "obj").await.unwrap();
        let (n1, e1) = part(&mut client, &u3, 1, 5 * 1024 * 1024).await;
        let (n2, e2) = part(&mut client, &u3, 2, 1234).await;
        let err = client
            .complete_multipart(
                VOL,
                BKT,
                "obj",
                &u3,
                &[(2, n2, e2), (1, n1, e1)],
            )
            .await
            .unwrap_err();
        assert!(matches!(err, OmError::InvalidPartOrder), "got {err:?}");
    }

    /// Committing a part to an upload id the OM never minted is `NoSuchUpload`.
    #[tokio::test]
    async fn commit_part_unknown_upload_is_no_such_upload() {
        let pipeline = CompliantOmPipeline {
            datanodes: datanodes(5),
            ec: ec_3_2(),
        };
        let (mut client, _record) = client_for(pipeline).await;
        // Open a part key so we have a real block to commit, but commit it under
        // a bogus upload id.
        let open = client
            .create_multipart_part_key(VOL, BKT, "obj", "upload-404", 1, None, 1024)
            .await
            .unwrap();
        let err = client
            .commit_part(
                VOL,
                BKT,
                "obj",
                "upload-404",
                1,
                open.client_id,
                &open.blocks,
                1024,
                "00112233445566778899aabbccddeeff",
            )
            .await
            .unwrap_err();
        assert!(matches!(err, OmError::NoSuchUpload), "got {err:?}");
    }

    /// `abort_multipart` removes the upload (and is lenient on a second abort).
    #[tokio::test]
    async fn abort_multipart_then_commit_is_no_such_upload() {
        let pipeline = CompliantOmPipeline {
            datanodes: datanodes(5),
            ec: ec_3_2(),
        };
        let (mut client, _record) = client_for(pipeline).await;
        let upload_id = client.initiate_multipart(VOL, BKT, "obj").await.unwrap();
        client.abort_multipart(VOL, BKT, "obj", &upload_id).await.unwrap();
        // Aborting again is a lenient no-op success.
        client.abort_multipart(VOL, BKT, "obj", &upload_id).await.unwrap();
        // Committing into the aborted upload now fails as NoSuchUpload.
        let open = client
            .create_multipart_part_key(VOL, BKT, "obj", &upload_id, 1, None, 1024)
            .await
            .unwrap();
        let err = client
            .commit_part(VOL, BKT, "obj", &upload_id, 1, open.client_id, &open.blocks, 1024, "ab")
            .await
            .unwrap_err();
        assert!(matches!(err, OmError::NoSuchUpload), "got {err:?}");
    }

    /// `list_parts` returns the committed parts ascending and honors the marker.
    #[tokio::test]
    async fn list_parts_ascending_with_marker() {
        let pipeline = CompliantOmPipeline {
            datanodes: datanodes(5),
            ec: ec_3_2(),
        };
        let (mut client, _record) = client_for(pipeline).await;
        let upload_id = client.initiate_multipart(VOL, BKT, "obj").await.unwrap();
        // Commit parts 1, 2, 3 (out of insertion order to prove sorting).
        for pn in [2u32, 1, 3] {
            let raw = [pn as u8; 16];
            let open = client
                .create_multipart_part_key(VOL, BKT, "obj", &upload_id, pn, None, 4096)
                .await
                .unwrap();
            client
                .commit_part(VOL, BKT, "obj", &upload_id, pn, open.client_id, &open.blocks, 4096, &hex_encode(&raw))
                .await
                .unwrap();
        }
        let all = client.list_parts(VOL, BKT, "obj", &upload_id, 0).await.unwrap();
        assert_eq!(
            all.iter().map(|p| p.part_number).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "parts must be ascending"
        );
        assert_eq!(all[0].size, 4096);
        assert_eq!(all[0].etag.as_deref(), Some(hex_encode(&[1u8; 16]).as_str()));
        // Marker filters out parts at or below it.
        let after1 = client.list_parts(VOL, BKT, "obj", &upload_id, 1).await.unwrap();
        assert_eq!(
            after1.iter().map(|p| p.part_number).collect::<Vec<_>>(),
            vec![2, 3]
        );
        // Listing an unknown upload is NoSuchUpload.
        let err = client.list_parts(VOL, BKT, "obj", "upload-404", 0).await.unwrap_err();
        assert!(matches!(err, OmError::NoSuchUpload), "got {err:?}");
    }

    /// `list_multipart_uploads` filters by key prefix and sorts by (key, id).
    #[tokio::test]
    async fn list_multipart_uploads_prefix_and_sort() {
        let pipeline = CompliantOmPipeline {
            datanodes: datanodes(5),
            ec: ec_3_2(),
        };
        let (mut client, _record) = client_for(pipeline).await;
        // Three uploads; two under prefix "a/", one under "b/".
        let a1 = client.initiate_multipart(VOL, BKT, "a/one").await.unwrap();
        let a2 = client.initiate_multipart(VOL, BKT, "a/two").await.unwrap();
        let _b = client.initiate_multipart(VOL, BKT, "b/three").await.unwrap();

        let listed = client.list_multipart_uploads(VOL, BKT, "a/").await.unwrap();
        assert_eq!(
            listed,
            vec![
                UploadListing { key: "a/one".into(), upload_id: a1 },
                UploadListing { key: "a/two".into(), upload_id: a2 },
            ],
            "only the a/ prefix, sorted by key"
        );
        // Empty prefix sees all three, sorted by key.
        let all = client.list_multipart_uploads(VOL, BKT, "").await.unwrap();
        assert_eq!(
            all.iter().map(|u| u.key.clone()).collect::<Vec<_>>(),
            vec!["a/one", "a/two", "b/three"]
        );
    }

    /// `list_keys` filters by prefix + start_after and caps at limit.
    #[tokio::test]
    async fn list_keys_prefix_start_after_limit() {
        let pipeline = CompliantOmPipeline {
            datanodes: datanodes(5),
            ec: ec_3_2(),
        };
        let (mut client, _record) = client_for(pipeline).await;

        // Commit four keys: a/1, a/2, a/3 (under prefix a/) and b/1 (excluded).
        for key in ["a/1", "a/2", "a/3", "b/1"] {
            let open = client.create_key(VOL, BKT, key, None, 0).await.unwrap();
            let mut blocks = open.blocks.clone();
            blocks[0].length = 10;
            client
                .commit_key(VOL, BKT, key, open.client_id, &blocks, 10, Some("etag-x"), &[])
                .await
                .unwrap();
        }

        // Prefix a/, no start, generous limit -> a/1, a/2, a/3 ascending.
        let listed = client.list_keys(VOL, BKT, "a/", "", 100).await.unwrap();
        assert_eq!(
            listed.iter().map(|k| k.key.clone()).collect::<Vec<_>>(),
            vec!["a/1", "a/2", "a/3"]
        );
        assert_eq!(listed[0].size, 10);
        assert_eq!(listed[0].etag.as_deref(), Some("etag-x"));

        // start_after = "a/1" excludes it.
        let after = client.list_keys(VOL, BKT, "a/", "a/1", 100).await.unwrap();
        assert_eq!(
            after.iter().map(|k| k.key.clone()).collect::<Vec<_>>(),
            vec!["a/2", "a/3"]
        );

        // limit caps the page.
        let capped = client.list_keys(VOL, BKT, "a/", "", 2).await.unwrap();
        assert_eq!(capped.len(), 2);
        assert_eq!(capped[0].key, "a/1");
    }

    /// Object tagging round-trips: put a tag set, get it back (sorted), then an
    /// empty put clears it.
    #[tokio::test]
    async fn object_tagging_round_trips() {
        let pipeline = CompliantOmPipeline {
            datanodes: datanodes(5),
            ec: ec_3_2(),
        };
        let (mut client, _record) = client_for(pipeline).await;

        // The key must exist before it can be tagged.
        let open = client.create_key(VOL, BKT, "obj", None, 0).await.unwrap();
        let mut blocks = open.blocks.clone();
        blocks[0].length = 10;
        client
            .commit_key(VOL, BKT, "obj", open.client_id, &blocks, 10, Some("e"), &[])
            .await
            .unwrap();

        client
            .put_object_tagging(
                VOL,
                BKT,
                "obj",
                &[("zeta".into(), "z".into()), ("alpha".into(), "a".into())],
            )
            .await
            .unwrap();
        let tags = client.get_object_tagging(VOL, BKT, "obj").await.unwrap();
        assert_eq!(
            tags,
            vec![("alpha".to_string(), "a".to_string()), ("zeta".to_string(), "z".to_string())],
            "tags returned sorted by key"
        );

        // Tagging round-trips through key metadata under x-amz-tag-, so the ETag
        // and other metadata survive a tag replace.
        assert_eq!(client.get_key_info(VOL, BKT, "obj").await.unwrap().etag.as_deref(), Some("e"));

        // An empty put clears all tags (this is how DeleteObjectTagging is served).
        client.put_object_tagging(VOL, BKT, "obj", &[]).await.unwrap();
        assert!(client.get_object_tagging(VOL, BKT, "obj").await.unwrap().is_empty());

        // Tagging an absent key is NotFound.
        let err = client.put_object_tagging(VOL, BKT, "ghost", &[]).await.unwrap_err();
        assert!(matches!(err, OmError::NotFound), "got {err:?}");
    }
}
