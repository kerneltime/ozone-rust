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
//! The CORE path only: bucket head/create/delete/list and the key write/read/
//! delete cycle (`create_key` -> `allocate_block`* -> `commit_key`, then
//! `get_key_info` / `delete_key`). Multipart and key listing are out of scope
//! here (a later increment).
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
// `ozone-scm-client` and `fake_om` precedents). Allow it module-wide.
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
/// user metadata, and block layout for reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyMeta {
    /// Total object size in bytes.
    pub size: u64,
    /// The object ETag, from `metadata["ETAG"]` if present.
    pub etag: Option<String>,
    /// User metadata as `(key, value)` pairs (includes any `ETAG` entry).
    pub metadata: Vec<(String, String)>,
    /// The committed block layout (W2: what was actually written).
    pub blocks: Vec<BlockLocation>,
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
}
