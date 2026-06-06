//! Gateway backend: the data path behind the S3 HTTP surface.
//!
//! Maps each S3 object operation onto OM metadata calls + EC + datanode chunk
//! I/O. The gateway owns the erasure-coding math: on PUT it encodes the object
//! into `k+p` shards and fans them out, one shard per datanode slot in the OM
//! pipeline; on GET it gathers surviving shards and decodes (reconstructing any
//! missing data shards).
//!
//! # Assumptions of this slice
//! - One block group per object (the object fits a single OM-allocated block).
//!   Multi-block (very large) objects are a later extension.
//! - One chunk ("0") per shard, holding that shard's full bytes.
//! - Trust-the-proxy auth: the principal is taken verbatim from the gateway's
//!   caller (the secure proxy), and `proxy_attested` is set true. The gateway
//!   does NOT verify SigV4.

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use md5::{Digest, Md5};
use thiserror::Error;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use ozone_dn_client::{DnClient, DnClientError};
use ozone_grpc_types::dn::v1 as dn;
use ozone_grpc_types::om::gw::v1 as om;
use ozone_om_client::{OmClient, OmClientError};
use ozone_storage::checksum;
use ozone_types::{
    BlockData, BlockId, ChecksumType, ChunkInfo, ContainerId, EcReplicationConfig, LocalId,
    ReplicaIndex,
};

/// S3 buckets live under a single fixed Ozone volume.
const S3_VOLUME: &str = "s3v";
/// The metadata key under which OM stores the object ETag.
const ETAG_META_KEY: &str = "ETAG";
/// Prefix for object tag entries persisted in OM key metadata. Keeps the S3 tag
/// set namespaced away from user `x-amz-meta-*` metadata and the reserved ETag.
const TAG_META_PREFIX: &str = "x-amz-tag-";
/// Minimum size of every multipart part except the last (S3 `EntityTooSmall`).
const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;
/// Inclusive upper bound on multipart part numbers (S3 allows 1..=10000).
const MAX_PART_NUMBER: u32 = 10_000;

/// Errors surfaced from the gateway data path. The HTTP layer maps these onto
/// S3 status codes.
#[derive(Debug, Error)]
pub enum GatewayError {
    /// The requested object does not exist.
    #[error("no such key")]
    NoSuchKey,
    /// The requested bucket does not exist.
    #[error("no such bucket")]
    NoSuchBucket,
    /// The referenced multipart upload id is unknown.
    #[error("no such upload")]
    NoSuchUpload,
    /// The request was malformed (bad path, missing field).
    #[error("bad request: {0}")]
    BadRequest(String),
    /// OM RPC failure. Boxed because the inner `tonic::Status` is large and
    /// would otherwise bloat every `Result` in the request path.
    #[error(transparent)]
    Om(Box<OmClientError>),
    /// Datanode RPC failure. Boxed for the same reason as [`GatewayError::Om`].
    #[error(transparent)]
    Dn(Box<DnClientError>),
    /// Erasure-coding failure.
    #[error(transparent)]
    Ec(#[from] ozone_ec::EcError),
    /// An internal invariant was violated (e.g. OM returned no pipeline).
    #[error("internal error: {0}")]
    Internal(String),
}

impl GatewayError {
    /// The S3 `<Code>` string for this error. Shared by the top-level error
    /// response and the per-key entries of a batch `DeleteObjects` result so the
    /// two never drift.
    pub fn s3_code(&self) -> &'static str {
        match self {
            GatewayError::NoSuchKey => "NoSuchKey",
            GatewayError::NoSuchBucket => "NoSuchBucket",
            GatewayError::NoSuchUpload => "NoSuchUpload",
            GatewayError::BadRequest(_) => "InvalidRequest",
            _ => "InternalError",
        }
    }
}

impl From<OmClientError> for GatewayError {
    fn from(e: OmClientError) -> Self {
        GatewayError::Om(Box::new(e))
    }
}

impl From<DnClientError> for GatewayError {
    fn from(e: DnClientError) -> Self {
        GatewayError::Dn(Box::new(e))
    }
}

/// One object in a `ListObjectsV2` result.
#[derive(Debug, Clone)]
pub struct ObjectEntry {
    /// Full object key.
    pub key: String,
    /// Object size in bytes.
    pub size: u64,
    /// ETag (unquoted; the HTTP layer adds quotes).
    pub etag: String,
    /// Last-modified time in epoch milliseconds.
    pub last_modified_ms: u64,
}

/// A `ListObjectsV2` result: the keys (after prefix/delimiter folding) plus the
/// folded common prefixes and the continuation cursor.
#[derive(Debug, Clone)]
pub struct Listing {
    /// Bucket name.
    pub name: String,
    /// Echoed request prefix.
    pub prefix: String,
    /// Echoed request delimiter (empty if none).
    pub delimiter: String,
    /// Echoed request max-keys.
    pub max_keys: u32,
    /// Whether the result was truncated.
    pub is_truncated: bool,
    /// Continuation token for the next page (empty if none).
    pub next_continuation_token: String,
    /// Objects in this page.
    pub contents: Vec<ObjectEntry>,
    /// Folded common prefixes (directory-like grouping under the delimiter).
    pub common_prefixes: Vec<String>,
}

/// One uploaded part of an in-flight multipart upload.
struct MpuPart {
    /// AWS part ETag (MD5 hex, unquoted).
    etag_hex: String,
    /// Raw 16-byte MD5 digest, fed to the final multipart-ETag computation.
    etag_binary: Vec<u8>,
    /// Part size in bytes.
    size: u64,
    /// The part's stored block group.
    location: om::KeyLocation,
}

/// In-flight multipart upload: its target key plus the parts uploaded so far.
struct MpuUpload {
    bucket: String,
    key: String,
    parts: BTreeMap<u32, MpuPart>,
}

/// A part summary for `ListParts`: `(part_number, etag_hex, size)`.
pub type PartSummary = (u32, String, u64);

/// An upload summary for `ListMultipartUploads`: `(upload_id, key)`.
pub type UploadSummary = (String, String);

/// CopyObject directives resolved from the request headers. With both directives
/// at their `COPY` default the destination clones the source's metadata and tags.
#[derive(Default)]
pub struct CopyDirectives {
    /// `x-amz-metadata-directive: REPLACE` — set the destination's user metadata
    /// from [`CopyDirectives::metadata`] instead of cloning the source's.
    pub replace_metadata: bool,
    /// Replacement user metadata (Content-Type + `x-amz-meta-*`); used only when
    /// `replace_metadata`.
    pub metadata: HashMap<String, String>,
    /// `x-amz-tagging-directive: REPLACE` — set the destination's tags from
    /// [`CopyDirectives::tags`] instead of cloning the source's.
    pub replace_tags: bool,
    /// Replacement tags; used only when `replace_tags`.
    pub tags: Vec<(String, String)>,
}

/// The gateway backend. Holds the OM channel, a per-datanode channel cache, and
/// the in-flight multipart registry; per-call clients are built from the
/// cloneable channels.
///
/// The multipart registry tracks parts in-process (the OM only finalizes at
/// completion), so the gateway is stateful across the UploadPart/Complete
/// requests of a single upload — acceptable for a single instance; a production
/// deployment would persist part records in OM so any replica can complete.
pub struct Gateway {
    om_channel: Channel,
    dn_channels: Mutex<HashMap<String, Channel>>,
    mpu: Mutex<HashMap<String, MpuUpload>>,
    /// S3 region reported in responses (informational for OBS).
    pub region: String,
}

impl Gateway {
    /// Connect to OM at `om_endpoint` (e.g. `http://host:port`).
    pub async fn connect(
        om_endpoint: impl Into<String>,
        region: impl Into<String>,
    ) -> Result<Self, GatewayError> {
        let om_channel = Channel::from_shared(om_endpoint.into())
            .map_err(|e| GatewayError::BadRequest(format!("bad OM endpoint: {e}")))?
            .connect()
            .await
            .map_err(|e| GatewayError::Internal(format!("OM connect: {e}")))?;
        Ok(Self {
            om_channel,
            dn_channels: Mutex::new(HashMap::new()),
            mpu: Mutex::new(HashMap::new()),
            region: region.into(),
        })
    }

    fn om(&self) -> OmClient {
        OmClient::from_channel(self.om_channel.clone())
    }

    /// Get-or-connect a datanode client for `endpoint`, caching the channel.
    async fn dn(&self, endpoint: &str) -> Result<DnClient, GatewayError> {
        {
            let cache = self.dn_channels.lock().await;
            if let Some(ch) = cache.get(endpoint) {
                return Ok(DnClient::from_channel(ch.clone()));
            }
        }
        let ch = Channel::from_shared(endpoint.to_string())
            .map_err(|e| GatewayError::Internal(format!("bad DN endpoint {endpoint}: {e}")))?
            .connect()
            .await
            .map_err(|e| GatewayError::Internal(format!("DN connect {endpoint}: {e}")))?;
        self.dn_channels
            .lock()
            .await
            .insert(endpoint.to_string(), ch.clone());
        Ok(DnClient::from_channel(ch))
    }

    /// HEAD bucket: succeeds if OM reports the bucket exists.
    pub async fn head_bucket(&self, bucket: &str, principal: &str) -> Result<(), GatewayError> {
        let resp = self
            .om()
            .head_bucket(om::HeadBucketRequest {
                volume_name: S3_VOLUME.to_string(),
                bucket_name: bucket.to_string(),
                auth: Some(auth(principal)),
            })
            .await?;
        if resp.exists {
            Ok(())
        } else {
            Err(GatewayError::NoSuchBucket)
        }
    }

    /// CreateBucket. Returns true if newly created, false if it already existed.
    pub async fn create_bucket(&self, bucket: &str, principal: &str) -> Result<bool, GatewayError> {
        let resp = self
            .om()
            .create_bucket(om::CreateBucketRequest {
                volume_name: S3_VOLUME.to_string(),
                bucket_name: bucket.to_string(),
                default_ec_config: None,
                auth: Some(auth(principal)),
            })
            .await?;
        Ok(resp.created)
    }

    /// DeleteBucket.
    pub async fn delete_bucket(&self, bucket: &str, principal: &str) -> Result<(), GatewayError> {
        self.om()
            .delete_bucket(om::DeleteBucketRequest {
                volume_name: S3_VOLUME.to_string(),
                bucket_name: bucket.to_string(),
                auth: Some(auth(principal)),
            })
            .await?;
        Ok(())
    }

    /// ListBuckets in the S3 volume, as `(name, creation_time_ms)`.
    pub async fn list_buckets(
        &self,
        principal: &str,
    ) -> Result<Vec<(String, u64)>, GatewayError> {
        let resp = self
            .om()
            .list_buckets(om::ListBucketsRequest {
                volume_name: S3_VOLUME.to_string(),
                auth: Some(auth(principal)),
            })
            .await?;
        Ok(resp
            .buckets
            .into_iter()
            .map(|b| (b.bucket_name, b.creation_time_ms))
            .collect())
    }

    /// List objects in a bucket (S3 `ListObjectsV2` semantics). OM performs the
    /// prefix filtering and delimiter folding; the gateway just shapes the
    /// result.
    #[allow(clippy::too_many_arguments)]
    pub async fn list_objects(
        &self,
        bucket: &str,
        principal: &str,
        prefix: String,
        delimiter: String,
        max_keys: u32,
        continuation_token: String,
    ) -> Result<Listing, GatewayError> {
        let mut stream = self
            .om()
            .list_keys(om::ListKeysRequest {
                volume_name: S3_VOLUME.to_string(),
                bucket_name: bucket.to_string(),
                prefix: prefix.clone(),
                delimiter: delimiter.clone(),
                continuation_token,
                max_keys,
                auth: Some(auth(principal)),
            })
            .await?;

        let mut contents = Vec::new();
        let mut common_prefixes = Vec::new();
        let mut next_continuation_token = String::new();
        let mut is_truncated = false;
        while let Some(resp) = stream
            .message()
            .await
            .map_err(|s| GatewayError::Om(Box::new(OmClientError::Rpc(s))))?
        {
            for k in resp.keys {
                let key = k.vbk.as_ref().map(|v| v.key_name.clone()).unwrap_or_default();
                let etag = etag_of(&k);
                contents.push(ObjectEntry {
                    key,
                    size: k.data_size,
                    etag,
                    last_modified_ms: k.modification_time_ms,
                });
            }
            common_prefixes.extend(resp.common_prefixes);
            next_continuation_token = resp.next_continuation_token;
            is_truncated = resp.is_truncated;
        }

        Ok(Listing {
            name: bucket.to_string(),
            prefix,
            delimiter,
            max_keys,
            is_truncated,
            next_continuation_token,
            contents,
            common_prefixes,
        })
    }

    /// Resolve a bucket's EC config (and confirm the bucket exists).
    async fn bucket_ec(
        &self,
        bucket: &str,
        principal: &str,
    ) -> Result<(EcReplicationConfig, dn::EcReplicationConfig), GatewayError> {
        let hb = self
            .om()
            .head_bucket(om::HeadBucketRequest {
                volume_name: S3_VOLUME.to_string(),
                bucket_name: bucket.to_string(),
                auth: Some(auth(principal)),
            })
            .await?;
        if !hb.exists {
            return Err(GatewayError::NoSuchBucket);
        }
        let ec_wire = hb
            .default_ec_config
            .ok_or_else(|| GatewayError::Internal("bucket has no EC config".into()))?;
        let ec = to_domain_ec(&ec_wire)?;
        Ok((ec, ec_wire))
    }

    /// Allocate a block from OM, EC-encode `body`, and write the k+p shards to
    /// the pipeline datanodes (one shard per replica slot). Returns the
    /// committed-shape [`om::KeyLocation`] plus the OM `client_id`/`open_version`
    /// (the simple PUT path needs them for `CommitKey`; multipart discards them).
    async fn allocate_and_write(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
        ec: EcReplicationConfig,
        ec_wire: &dn::EcReplicationConfig,
        body: &[u8],
    ) -> Result<(om::KeyLocation, u64, u64), GatewayError> {
        let profile = ec_profile(ec);
        let shards = ozone_ec::stripe::encode_object(profile, body)?;

        let ck = self
            .om()
            .create_key(om::CreateKeyRequest {
                vbk: Some(vbk(bucket, key)),
                expected_size: body.len() as u64,
                ec_config: Some(ec_wire.clone()),
                metadata: HashMap::new(),
                auth: Some(auth(principal)),
            })
            .await?;
        let (client_id, open_version) = (ck.client_id, ck.open_version);
        let block = ck
            .pre_allocated_blocks
            .into_iter()
            .next()
            .ok_or_else(|| GatewayError::Internal("OM returned no pre-allocated block".into()))?;
        let pipeline = block
            .pipeline
            .ok_or_else(|| GatewayError::Internal("block has no pipeline".into()))?;
        let group = block
            .block_id
            .ok_or_else(|| GatewayError::Internal("block has no id".into()))?;
        let container = ContainerId(group.container_id);
        let local = LocalId(group.local_id);

        let k = ec.data as usize;
        let p = ec.parity as usize;
        for slot in 1..=(k + p) {
            let shard: &[u8] = if slot <= k {
                &shards.data[slot - 1]
            } else {
                &shards.parity[slot - 1 - k]
            };
            let endpoint = endpoint_for_slot(&pipeline, slot as u32)?;
            let mut dnc = self.dn(&endpoint).await?;
            // Ensure the container exists on this datanode (idempotent).
            match dnc.create_container(container, ec).await {
                Ok(_) => {}
                Err(DnClientError::Rpc(s)) if s.code() == tonic::Code::AlreadyExists => {}
                Err(e) => return Err(e.into()),
            }
            let cd = checksum::compute(shard, ec.ec_chunk_size, ChecksumType::Crc32c)
                .map_err(|e| GatewayError::Internal(format!("checksum: {e}")))?;
            let chunk = ChunkInfo {
                chunk_name: "0".to_string(),
                offset: 0,
                len: shard.len() as u64,
                checksum_data: Some(cd),
                stripe_checksum: None,
            };
            let block_slot = BlockId::ec(container, local, ReplicaIndex::new(slot as u8));
            dnc.write_chunk(&block_slot, &chunk, Bytes::copy_from_slice(shard))
                .await?;
            let mut bd = BlockData::new(block_slot);
            bd.chunks.push(chunk);
            bd.set_block_group_len(body.len() as u64);
            dnc.put_block(&bd, true).await?;
        }

        let loc = om::KeyLocation {
            block_id: Some(group),
            offset: 0,
            length: body.len() as u64,
            pipeline: Some(pipeline),
            block_token: Vec::new(),
        };
        Ok((loc, client_id, open_version))
    }

    /// PUT object: encode + store one block group, then commit the key. Returns
    /// the object's ETag (MD5 hex, unquoted). `tags` are the object tags from a
    /// PUT-time `x-amz-tagging` header; they are persisted alongside `metadata`
    /// under the reserved tag prefix, identical to a later `PutObjectTagging`.
    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
        body: Bytes,
        mut metadata: HashMap<String, String>,
        tags: Vec<(String, String)>,
    ) -> Result<String, GatewayError> {
        let (ec, ec_wire) = self.bucket_ec(bucket, principal).await?;
        let (loc, client_id, open_version) = self
            .allocate_and_write(bucket, key, principal, ec, &ec_wire, &body)
            .await?;
        for (k, v) in tags {
            metadata.insert(format!("{TAG_META_PREFIX}{k}"), v);
        }
        let etag = md5_hex(&body);
        self.om()
            .commit_key(om::CommitKeyRequest {
                client_id,
                open_version,
                vbk: Some(vbk(bucket, key)),
                final_size: body.len() as u64,
                final_locations: vec![loc],
                etag: etag.clone().into_bytes(),
                metadata,
                auth: Some(auth(principal)),
            })
            .await?;
        Ok(etag)
    }

    /// Server-side copy: clone the source key's metadata under the destination
    /// (an OBS-style reference copy — the destination shares the source's block
    /// groups). Returns `(etag, size)`. A missing source maps to `NoSuchKey`.
    pub async fn copy_object(
        &self,
        dest_bucket: &str,
        dest_key: &str,
        src_bucket: &str,
        src_key: &str,
        principal: &str,
        directives: CopyDirectives,
    ) -> Result<(String, u64), GatewayError> {
        match self
            .om()
            .copy_key(om::CopyKeyRequest {
                source: Some(vbk(src_bucket, src_key)),
                dest: Some(vbk(dest_bucket, dest_key)),
                auth: Some(auth(principal)),
                replace_metadata: directives.replace_metadata,
                metadata: directives.metadata,
                replace_tags: directives.replace_tags,
                tags: directives
                    .tags
                    .into_iter()
                    .map(|(key, value)| om::Tag { key, value })
                    .collect(),
            })
            .await
        {
            Ok(resp) => Ok((String::from_utf8_lossy(&resp.etag).into_owned(), resp.size)),
            Err(OmClientError::Rpc(s)) if s.code() == tonic::Code::NotFound => {
                Err(GatewayError::NoSuchKey)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// PUT object tagging: replace the object's full tag set. Tags are persisted
    /// in OM key metadata under the `x-amz-tag-` prefix (so they never collide
    /// with user `x-amz-meta-*` metadata or the reserved ETag). An empty `tags`
    /// clears all tags, which is exactly how `DeleteObjectTagging` is served. A
    /// missing key maps to `NoSuchKey`.
    pub async fn put_object_tagging(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
        tags: Vec<(String, String)>,
    ) -> Result<(), GatewayError> {
        match self
            .om()
            .put_object_tagging(om::PutObjectTaggingRequest {
                vbk: Some(vbk(bucket, key)),
                tags: tags
                    .into_iter()
                    .map(|(key, value)| om::Tag { key, value })
                    .collect(),
                auth: Some(auth(principal)),
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(OmClientError::Rpc(s)) if s.code() == tonic::Code::NotFound => {
                Err(GatewayError::NoSuchKey)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// GET object tagging: read the object's tag set from OM key metadata,
    /// stripping the `x-amz-tag-` prefix. Returns the tags sorted by key for a
    /// stable response. A missing key maps to `NoSuchKey`.
    pub async fn get_object_tagging(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
    ) -> Result<Vec<(String, String)>, GatewayError> {
        let info = self.lookup(bucket, key, principal).await?;
        let mut tags: Vec<(String, String)> = info
            .metadata
            .into_iter()
            .filter_map(|(k, v)| k.strip_prefix(TAG_META_PREFIX).map(|t| (t.to_string(), v)))
            .collect();
        tags.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(tags)
    }

    /// GetObjectAttributes data: `(etag, size, last_modified_ms)` from OM, no
    /// data read. A missing key maps to `NoSuchKey`. (Additional checksums and
    /// post-completion part records are not stored, so the HTTP layer only
    /// surfaces ETag and ObjectSize.)
    pub async fn object_attributes(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
    ) -> Result<(String, u64, u64), GatewayError> {
        let info = self.lookup(bucket, key, principal).await?;
        Ok((etag_of(&info), info.data_size, info.modification_time_ms))
    }

    /// HEAD object: `(size, etag, metadata, last_modified_ms)` from OM, no data
    /// read.
    pub async fn head_object(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
    ) -> Result<(u64, String, HashMap<String, String>, u64), GatewayError> {
        let info = self.lookup(bucket, key, principal).await?;
        let mod_ms = info.modification_time_ms;
        Ok((info.data_size, etag_of(&info), info.metadata, mod_ms))
    }

    /// GET object: gather shards, decode, return `(bytes, etag, metadata,
    /// last_modified_ms)`.
    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
    ) -> Result<(Bytes, String, HashMap<String, String>, u64), GatewayError> {
        let info = self.lookup(bucket, key, principal).await?;
        let etag = etag_of(&info);
        let mod_ms = info.modification_time_ms;
        let ec = to_domain_ec(
            info.ec_config
                .as_ref()
                .ok_or_else(|| GatewayError::Internal("key has no EC config".into()))?,
        )?;
        let profile = ec_profile(ec);

        // The object is the concatenation of its locations' block groups (one
        // group for a simple PUT; one per part for a completed multipart upload).
        let mut out = Vec::with_capacity(info.data_size as usize);
        for loc in &info.locations {
            let pipeline = loc
                .pipeline
                .as_ref()
                .ok_or_else(|| GatewayError::Internal("location has no pipeline".into()))?;
            let group = loc
                .block_id
                .as_ref()
                .ok_or_else(|| GatewayError::Internal("location has no block id".into()))?;
            let part = self
                .read_block_group(
                    profile,
                    ContainerId(group.container_id),
                    LocalId(group.local_id),
                    pipeline,
                    loc.length as usize,
                )
                .await?;
            out.extend_from_slice(&part);
        }
        Ok((Bytes::from(out), etag, info.metadata, mod_ms))
    }

    /// Read and EC-decode a single block group of `length` user bytes. Gathers
    /// every shard it can; missing/failed reads stay absent and the decoder
    /// reconstructs as long as `>= k` shards survive.
    async fn read_block_group(
        &self,
        profile: ozone_ec::Profile,
        container: ContainerId,
        local: LocalId,
        pipeline: &om::Pipeline,
        length: usize,
    ) -> Result<Vec<u8>, GatewayError> {
        let total = profile.data + profile.parity;
        let mut shard_bufs: Vec<Option<Vec<u8>>> = vec![None; total];
        for slot in 1..=total {
            let endpoint = match endpoint_for_slot(pipeline, slot as u32) {
                Ok(e) => e,
                Err(_) => continue,
            };
            let mut dnc = match self.dn(&endpoint).await {
                Ok(d) => d,
                Err(_) => continue,
            };
            let block_slot = BlockId::ec(container, local, ReplicaIndex::new(slot as u8));
            let chunk = ChunkInfo {
                chunk_name: "0".to_string(),
                offset: 0,
                len: 0,
                checksum_data: None,
                stripe_checksum: None,
            };
            // verify=true: the datanode checks the shard against its stored
            // checksum. A corrupted shard then fails the read and is left absent,
            // so the EC decoder reconstructs it from the survivors rather than
            // decoding corrupt bytes.
            if let Ok(b) = dnc.read_chunk(&block_slot, &chunk, true).await {
                shard_bufs[slot - 1] = Some(b.to_vec());
            }
        }
        let views: Vec<Option<&[u8]>> = shard_bufs.iter().map(|o| o.as_deref()).collect();
        Ok(ozone_ec::stripe::decode_object(profile, length, &views)?)
    }

    /// DELETE object: remove OM metadata and best-effort delete shards. S3
    /// DELETE is idempotent, so a missing key still succeeds.
    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
    ) -> Result<(), GatewayError> {
        let mut om = self.om();
        let vbk = om::VolumeBucketKey {
            volume_name: S3_VOLUME.to_string(),
            bucket_name: bucket.to_string(),
            key_name: key.to_string(),
        };

        // Look up first so we can tear down the shards; absent key -> just 204.
        if let Ok(resp) = om
            .lookup_key(om::LookupKeyRequest {
                vbk: Some(vbk.clone()),
                auth: Some(auth(principal)),
            })
            .await
        {
            if let Some(info) = resp.key_info {
                let total = info
                    .ec_config
                    .as_ref()
                    .map(|c| (c.data + c.parity) as usize)
                    .unwrap_or(0);
                for loc in &info.locations {
                    let (Some(group), Some(pipeline)) = (&loc.block_id, &loc.pipeline) else {
                        continue;
                    };
                    let container = ContainerId(group.container_id);
                    let local = LocalId(group.local_id);
                    for slot in 1..=total {
                        if let Ok(endpoint) = endpoint_for_slot(pipeline, slot as u32) {
                            if let Ok(mut dnc) = self.dn(&endpoint).await {
                                let bslot =
                                    BlockId::ec(container, local, ReplicaIndex::new(slot as u8));
                                let _ = dnc.delete_block(&bslot).await;
                            }
                        }
                    }
                }
            }
        }

        om.delete_key(om::DeleteKeyRequest {
            vbk: Some(vbk),
            auth: Some(auth(principal)),
        })
        .await?;
        Ok(())
    }

    /// Batch delete (S3 `DeleteObjects`). Deletes each key independently and
    /// returns per-key outcomes: `(key, None)` on success, `(key, Some((code,
    /// message)))` on error, where `code` is the S3 error code. Since object
    /// DELETE is idempotent, deleting an absent key succeeds.
    pub async fn delete_objects(
        &self,
        bucket: &str,
        keys: &[String],
        principal: &str,
    ) -> Vec<(String, Option<(String, String)>)> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            match self.delete_object(bucket, key, principal).await {
                Ok(()) => results.push((key.clone(), None)),
                Err(e) => results.push((key.clone(), Some((e.s3_code().to_string(), e.to_string())))),
            }
        }
        results
    }

    /// Initiate a multipart upload: validate the bucket, get an upload id from
    /// OM, and register in-flight state. Returns the upload id.
    pub async fn initiate_multipart(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
    ) -> Result<String, GatewayError> {
        let (_, ec_wire) = self.bucket_ec(bucket, principal).await?;
        let resp = self
            .om()
            .initiate_multipart_upload(om::InitiateMultipartUploadRequest {
                vbk: Some(vbk(bucket, key)),
                ec_config: Some(ec_wire),
                metadata: HashMap::new(),
                auth: Some(auth(principal)),
            })
            .await?;
        let upload_id = resp.upload_id;
        self.mpu.lock().await.insert(
            upload_id.clone(),
            MpuUpload {
                bucket: bucket.to_string(),
                key: key.to_string(),
                parts: BTreeMap::new(),
            },
        );
        Ok(upload_id)
    }

    /// Upload one part: EC-encode + store its block group, record it under the
    /// upload, and return the part ETag (MD5 hex, unquoted).
    pub async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
        upload_id: &str,
        part_number: u32,
        body: Bytes,
    ) -> Result<String, GatewayError> {
        if !(1..=MAX_PART_NUMBER).contains(&part_number) {
            return Err(GatewayError::BadRequest(format!(
                "part number {part_number} out of range (1..={MAX_PART_NUMBER})"
            )));
        }
        if !self.mpu.lock().await.contains_key(upload_id) {
            return Err(GatewayError::NoSuchUpload);
        }
        let (ec, ec_wire) = self.bucket_ec(bucket, principal).await?;
        let (location, _, _) = self
            .allocate_and_write(bucket, key, principal, ec, &ec_wire, &body)
            .await?;
        let etag_binary = md5_binary(&body);
        let etag_hex = hex(&etag_binary);
        let mut reg = self.mpu.lock().await;
        let up = reg.get_mut(upload_id).ok_or(GatewayError::NoSuchUpload)?;
        up.parts.insert(
            part_number,
            MpuPart {
                etag_hex: etag_hex.clone(),
                etag_binary,
                size: body.len() as u64,
                location,
            },
        );
        Ok(etag_hex)
    }

    /// Complete a multipart upload: assemble the named parts in order, hand them
    /// to OM (which finalizes the key and computes the multipart ETag), and
    /// return the final ETag (`<hash>-N`, unquoted).
    pub async fn complete_multipart(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
        upload_id: &str,
        ordered_part_numbers: &[u32],
    ) -> Result<String, GatewayError> {
        if ordered_part_numbers.is_empty() {
            return Err(GatewayError::BadRequest("complete with no parts".into()));
        }
        // Parts must be strictly ascending with no duplicates (S3 InvalidPartOrder).
        // The OM trusts this order verbatim, so the gateway must enforce it here
        // rather than silently re-sorting a malformed request.
        if ordered_part_numbers.windows(2).any(|w| w[0] >= w[1]) {
            return Err(GatewayError::BadRequest(
                "parts must be in ascending order with no duplicates".into(),
            ));
        }
        let parts: Vec<om::Part> = {
            let reg = self.mpu.lock().await;
            let stored = reg.get(upload_id).ok_or(GatewayError::NoSuchUpload)?;
            let mut v = Vec::with_capacity(ordered_part_numbers.len());
            let last_idx = ordered_part_numbers.len() - 1;
            for (idx, &pn) in ordered_part_numbers.iter().enumerate() {
                let part = stored.parts.get(&pn).ok_or_else(|| {
                    GatewayError::BadRequest(format!("part {pn} was not uploaded"))
                })?;
                // Every part except the last must meet the 5 MiB minimum
                // (S3 EntityTooSmall).
                if idx != last_idx && part.size < MIN_PART_SIZE {
                    return Err(GatewayError::BadRequest(format!(
                        "part {pn} ({} bytes) is smaller than the {MIN_PART_SIZE}-byte minimum",
                        part.size
                    )));
                }
                v.push(om::Part {
                    part_number: pn,
                    etag: part.etag_binary.clone(),
                    size: part.size,
                    locations: vec![part.location.clone()],
                });
            }
            v
        };
        let resp = self
            .om()
            .complete_multipart_upload(om::CompleteMultipartUploadRequest {
                vbk: Some(vbk(bucket, key)),
                upload_id: upload_id.to_string(),
                parts,
                auth: Some(auth(principal)),
            })
            .await?;
        self.mpu.lock().await.remove(upload_id);
        Ok(String::from_utf8_lossy(&resp.etag).into_owned())
    }

    /// Abort a multipart upload: drop in-flight state and tell OM. Already-
    /// written part blocks become garbage reclaimed by container GC (a
    /// background reclaimer is out of scope for this slice).
    pub async fn abort_multipart(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
        upload_id: &str,
    ) -> Result<(), GatewayError> {
        self.mpu.lock().await.remove(upload_id);
        self.om()
            .abort_multipart_upload(om::AbortMultipartUploadRequest {
                vbk: Some(vbk(bucket, key)),
                upload_id: upload_id.to_string(),
                auth: Some(auth(principal)),
            })
            .await?;
        Ok(())
    }

    /// List the parts uploaded so far for an in-flight upload.
    pub async fn list_parts(&self, upload_id: &str) -> Result<Vec<PartSummary>, GatewayError> {
        let reg = self.mpu.lock().await;
        let up = reg.get(upload_id).ok_or(GatewayError::NoSuchUpload)?;
        Ok(up
            .parts
            .iter()
            .map(|(pn, p)| (*pn, p.etag_hex.clone(), p.size))
            .collect())
    }

    /// List in-flight multipart uploads for a bucket as `(upload_id, key)`,
    /// sorted by key then upload id for stable output.
    pub async fn list_multipart_uploads(&self, bucket: &str) -> Vec<UploadSummary> {
        let reg = self.mpu.lock().await;
        let mut out: Vec<UploadSummary> = reg
            .iter()
            .filter(|(_, up)| up.bucket == bucket)
            .map(|(id, up)| (id.clone(), up.key.clone()))
            .collect();
        out.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        out
    }

    /// Shared OM lookup that maps `NotFound` -> [`GatewayError::NoSuchKey`].
    async fn lookup(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
    ) -> Result<om::OmKeyInfoLite, GatewayError> {
        let vbk = om::VolumeBucketKey {
            volume_name: S3_VOLUME.to_string(),
            bucket_name: bucket.to_string(),
            key_name: key.to_string(),
        };
        match self
            .om()
            .lookup_key(om::LookupKeyRequest {
                vbk: Some(vbk),
                auth: Some(auth(principal)),
            })
            .await
        {
            Ok(resp) => resp.key_info.ok_or(GatewayError::NoSuchKey),
            Err(OmClientError::Rpc(s)) if s.code() == tonic::Code::NotFound => {
                Err(GatewayError::NoSuchKey)
            }
            Err(e) => Err(e.into()),
        }
    }
}

// ---- helpers ----

fn auth(principal: &str) -> om::AuthContext {
    om::AuthContext {
        principal: principal.to_string(),
        proxy_attested: true,
        request_id: String::new(),
    }
}

fn vbk(bucket: &str, key: &str) -> om::VolumeBucketKey {
    om::VolumeBucketKey {
        volume_name: S3_VOLUME.to_string(),
        bucket_name: bucket.to_string(),
        key_name: key.to_string(),
    }
}

fn ec_profile(ec: EcReplicationConfig) -> ozone_ec::Profile {
    ozone_ec::Profile {
        data: ec.data as usize,
        parity: ec.parity as usize,
        chunk_size: ec.ec_chunk_size as usize,
    }
}

fn to_domain_ec(w: &dn::EcReplicationConfig) -> Result<EcReplicationConfig, GatewayError> {
    w.clone()
        .try_into()
        .map_err(|e| GatewayError::Internal(format!("bad EC config: {e}")))
}

/// Resolve the datanode endpoint holding EC replica `slot` (1-indexed) from the
/// pipeline's `member_replica_indexes` map.
fn endpoint_for_slot(pipeline: &om::Pipeline, slot: u32) -> Result<String, GatewayError> {
    let uuid = pipeline
        .member_replica_indexes
        .iter()
        .find(|(_, &v)| v == slot)
        .map(|(k, _)| k.clone())
        .ok_or_else(|| GatewayError::Internal(format!("pipeline has no member for slot {slot}")))?;
    let member = pipeline
        .members
        .iter()
        .find(|m| m.uuid == uuid)
        .ok_or_else(|| GatewayError::Internal(format!("pipeline member {uuid} missing details")))?;
    Ok(format!("http://{}:{}", member.ip_address, member.gateway_port))
}

fn etag_of(info: &om::OmKeyInfoLite) -> String {
    info.metadata.get(ETAG_META_KEY).cloned().unwrap_or_default()
}

fn md5_binary(data: &[u8]) -> Vec<u8> {
    let mut h = Md5::new();
    h.update(data);
    h.finalize().to_vec()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn md5_hex(data: &[u8]) -> String {
    hex(&md5_binary(data))
}
