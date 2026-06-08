//! Gateway backend: the data path behind the S3 HTTP surface.
//!
//! Maps each S3 object operation onto OM metadata calls + EC + datanode chunk
//! I/O. The gateway owns the erasure-coding math: on PUT it encodes the object
//! into `k+p` shards and fans them out, one shard per datanode slot in the OM
//! pipeline; on GET it gathers surviving shards and decodes (reconstructing any
//! missing data shards).
//!
//! # Assumptions of this slice
//! - An object (or multipart part) is split into block groups of at most
//!   `block_group_size` user bytes; each block group is independently
//!   erasure-coded and allocated from OM (`CreateKey` pre-allocation, then
//!   `AllocateBlock`). A read concatenates the groups in committed order.
//! - One chunk ("0") per shard, holding that shard's full bytes.
//! - Trust-the-proxy auth: the principal is taken verbatim from the gateway's
//!   caller (the secure proxy), and `proxy_attested` is set true. The gateway
//!   does NOT verify SigV4.

use std::collections::HashMap;

use bytes::Bytes;
use md5::{Digest, Md5};
use thiserror::Error;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use ozone_dn_client::{DnClient, DnClientError};
use ozone_om_client::compliant::{BlockLocation, OmError, OpenKey, OzoneOmClient};
use ozone_storage::checksum;
use ozone_types::{
    BlockData, BlockId, ChecksumType, ChunkInfo, ContainerId, EcReplicationConfig, LocalId,
    ReplicaIndex,
};

/// S3 buckets live under a single fixed Ozone volume.
const S3_VOLUME: &str = "s3v";
/// Stable OM client id stamped on every request's envelope (the OM open-session
/// `client_id` is a separate per-key id the OM mints). A fixed constant is
/// deliberate: the id must be stable for the gateway's life and must NOT be
/// derived from a clock or randomness.
const OM_CLIENT_ID: &str = "rust-s3g";
/// Fixed creation time (2021-01-01T00:00:00Z, ms) reported for every bucket in
/// `ListBuckets`. The compliant OM `list_buckets` returns only names; S3 clients
/// require a `CreationDate`, so a stable value is surfaced (it matches the OM
/// fixture's `FAKE_OBJECT_TIME_MS`). No test asserts a specific bucket date.
const FAKE_BUCKET_TIME_MS: u64 = 1_609_459_200_000;
/// Fixed last-modified time (2021-01-01T00:00:00Z, ms) reported for each object
/// in a `ListObjectsV2` page. The compliant `KeyListing` carries no mtime, and
/// the OM fixture stamps every key with this same value, so a `LastModified` is
/// surfaced consistently with HEAD/GET (which read `KeyMeta.modification_time`).
const FAKE_OBJECT_TIME_MS: u64 = 1_609_459_200_000;
/// Prefix for object tag entries persisted in OM key metadata. Keeps the S3 tag
/// set namespaced away from user `x-amz-meta-*` metadata and the reserved ETag.
const TAG_META_PREFIX: &str = "x-amz-tag-";
/// Inclusive upper bound on multipart part numbers (S3 allows 1..=10000). The
/// gateway front-doors this before any datanode write; the OM enforces it too.
const MAX_PART_NUMBER: u32 = 10_000;
/// Default maximum user bytes per EC block group. An object (or multipart part)
/// larger than this is split across several block groups, each independently
/// erasure-coded and allocated from OM. 256 MiB mirrors Ozone's default block
/// size; tests override it (see [`Gateway::set_block_group_size`]) so a small
/// object exercises the multi-block path.
const DEFAULT_BLOCK_GROUP_SIZE: usize = 256 * 1024 * 1024;

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
    /// OM RPC failure. Boxed because the inner `OmError` wraps a large
    /// `tonic::Status` and would otherwise bloat every `Result` in the path.
    #[error(transparent)]
    Om(Box<OmError>),
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

/// Map an OM domain error onto the right S3-facing gateway error. The OM is the
/// authority for these outcomes, so the mapping lives here once and serves every
/// call site (a missing key, a missing bucket, and the multipart validations are
/// all OM statuses, distinguished by variant):
/// - `NotFound` -> `NoSuchKey` (key reads/tagging),
/// - `BucketNotFound` -> `NoSuchBucket`,
/// - `NoSuchUpload` -> `NoSuchUpload`,
/// - the part validations (`InvalidPart`/`InvalidPartOrder`/`EntityTooSmall`)
///   -> `BadRequest` (400 `InvalidRequest`), NOT 404,
/// - anything else (transport, contract violation) -> `Om` (500).
impl From<OmError> for GatewayError {
    fn from(e: OmError) -> Self {
        match e {
            OmError::NotFound => GatewayError::NoSuchKey,
            OmError::BucketNotFound => GatewayError::NoSuchBucket,
            OmError::NoSuchUpload => GatewayError::NoSuchUpload,
            OmError::InvalidPart => GatewayError::BadRequest("invalid multipart part".into()),
            OmError::InvalidPartOrder => {
                GatewayError::BadRequest("invalid multipart part order".into())
            }
            OmError::EntityTooSmall => {
                GatewayError::BadRequest("multipart part too small".into())
            }
            other => GatewayError::Om(Box::new(other)),
        }
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

/// The gateway backend. Holds the OM channel and a per-datanode channel cache;
/// per-call clients are built from the cloneable channels.
///
/// The gateway is STATELESS for multipart: in-flight part records live in the
/// OM (persisted at UploadPart via `CommitMultipartPart`), so any gateway replica
/// can upload, complete, list, or abort an upload, and a gateway restart loses
/// nothing.
pub struct Gateway {
    om_channel: Channel,
    /// Stable OM client id stamped on every request envelope (see
    /// [`OM_CLIENT_ID`]).
    client_id: String,
    dn_channels: Mutex<HashMap<String, Channel>>,
    /// S3 region reported in responses (informational for OBS).
    pub region: String,
    /// Max user bytes per EC block group; larger writes span multiple groups.
    block_group_size: usize,
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
            client_id: OM_CLIENT_ID.to_string(),
            dn_channels: Mutex::new(HashMap::new()),
            region: region.into(),
            block_group_size: DEFAULT_BLOCK_GROUP_SIZE,
        })
    }

    /// Override the max user bytes per EC block group. Clamped to at least 1.
    /// Intended for tests (and tuning) so a small object spans multiple groups.
    pub fn set_block_group_size(&mut self, bytes: usize) {
        self.block_group_size = bytes.max(1);
    }

    /// Build an OM client for one request, baking in the gateway's stable
    /// `client_id` and the per-request attested `principal` (the compliant
    /// client stamps `principal` on the envelope at construction; the gateway
    /// has a per-call principal, so a fresh client is built per call).
    fn om(&self, principal: &str) -> OzoneOmClient {
        OzoneOmClient::from_channel(self.om_channel.clone(), &self.client_id, principal)
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
        if self.om(principal).head_bucket(S3_VOLUME, bucket).await? {
            Ok(())
        } else {
            Err(GatewayError::NoSuchBucket)
        }
    }

    /// CreateBucket. Returns true if newly created, false if it already existed
    /// (the OM create is idempotent, so the prior-existence probe is a HEAD).
    pub async fn create_bucket(&self, bucket: &str, principal: &str) -> Result<bool, GatewayError> {
        let existed = self.om(principal).head_bucket(S3_VOLUME, bucket).await?;
        self.om(principal).create_bucket(S3_VOLUME, bucket).await?;
        Ok(!existed)
    }

    /// DeleteBucket.
    pub async fn delete_bucket(&self, bucket: &str, principal: &str) -> Result<(), GatewayError> {
        self.om(principal).delete_bucket(S3_VOLUME, bucket).await?;
        Ok(())
    }

    /// ListBuckets in the S3 volume, as `(name, creation_time_ms)`. The compliant
    /// OM surfaces only names; a stable [`FAKE_BUCKET_TIME_MS`] fills the date S3
    /// clients expect.
    pub async fn list_buckets(
        &self,
        principal: &str,
    ) -> Result<Vec<(String, u64)>, GatewayError> {
        let names = self.om(principal).list_buckets(S3_VOLUME).await?;
        Ok(names
            .into_iter()
            .map(|name| (name, FAKE_BUCKET_TIME_MS))
            .collect())
    }

    /// List objects in a bucket (S3 `ListObjectsV2` semantics). The compliant OM
    /// `list_keys` returns a FLAT, prefix-filtered, ascending key list with no
    /// delimiter folding and no S3 continuation cursor, so the gateway owns the
    /// S3 folding/pagination here:
    ///
    /// - `continuation_token` IS the OM `start_after` start key (exclusive). The
    ///   gateway resumes a page by passing the token straight through.
    /// - Each returned key has `prefix` stripped; if `delimiter` is non-empty and
    ///   the remainder contains it, the span from `prefix` through the first
    ///   delimiter is a CommonPrefix (deduped, and the key is NOT added to
    ///   contents); otherwise the key is a content entry.
    /// - A content key and each DISTINCT common prefix each count ONE toward
    ///   `max_keys`. Keys that fold into an already-emitted common prefix are
    ///   consumed but never counted, so the resume cursor lands cleanly past the
    ///   whole folded group.
    /// - `max_keys == 0` yields an empty, NON-truncated page with no token (an S3
    ///   client paginating on a truncated-but-cursorless page would livelock).
    ///
    /// Pagination is bounded: keys are pulled from the OM in batches of `max_keys`
    /// (advancing `start_after` by the last raw key each batch) until the page is
    /// full with one entry to spare (truncated) or the OM is exhausted.
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
        let mut contents: Vec<ObjectEntry> = Vec::new();
        let mut common_prefixes: Vec<String> = Vec::new();
        let mut next_continuation_token = String::new();
        let mut is_truncated = false;

        if max_keys == 0 {
            return Ok(Listing {
                name: bucket.to_string(),
                prefix,
                delimiter,
                max_keys,
                is_truncated,
                next_continuation_token,
                contents,
                common_prefixes,
            });
        }

        // The OM page size: fetch max_keys keys at a time. Folding can collapse a
        // batch into fewer page entries, so we loop and refetch until the page is
        // full (plus one, to detect truncation) or the OM runs dry.
        let batch = max_keys as i32;
        let mut start_after = continuation_token;
        let mut count: u32 = 0;
        // The raw OM key after which the NEXT page resumes (exclusive start_after).
        // Advanced for every key folded into the page; left untouched by the key
        // that trips truncation (that key begins the next page).
        let mut resume_key = String::new();
        'outer: loop {
            let keys = self
                .om(principal)
                .list_keys(S3_VOLUME, bucket, &prefix, &start_after, batch)
                .await?;
            if keys.is_empty() {
                break;
            }
            let fetched = keys.len();
            // Advance the OM cursor for the next batch by the last raw key seen.
            start_after = keys[fetched - 1].key.clone();

            for k in keys {
                // The folded prefix this key maps to, if delimiter folding applies.
                let folded = if delimiter.is_empty() {
                    None
                } else {
                    let rest = k.key.strip_prefix(&prefix).unwrap_or(&k.key);
                    rest.find(&delimiter)
                        .map(|i| format!("{prefix}{}", &rest[..i + delimiter.len()]))
                };

                match folded {
                    // Folds into an ALREADY-emitted common prefix: consume it
                    // (advance the resume cursor) without counting, so the next
                    // page clears the entire group.
                    Some(p) if common_prefixes.contains(&p) => {
                        resume_key = k.key;
                    }
                    // A NEW page entry (new common prefix or a content key). If the
                    // page is already full, this one entry beyond it means the
                    // result is truncated; the resume cursor already points at the
                    // last included key, so stop without consuming this one.
                    _ if count == max_keys => {
                        is_truncated = true;
                        break 'outer;
                    }
                    Some(p) => {
                        resume_key = k.key;
                        common_prefixes.push(p);
                        count += 1;
                    }
                    None => {
                        resume_key = k.key.clone();
                        contents.push(ObjectEntry {
                            key: k.key,
                            size: k.size,
                            etag: k.etag.unwrap_or_default(),
                            last_modified_ms: FAKE_OBJECT_TIME_MS,
                        });
                        count += 1;
                    }
                }
            }

            // A short batch (fewer than requested) means the OM is exhausted.
            if fetched < batch as usize {
                break;
            }
        }

        if is_truncated {
            next_continuation_token = resume_key;
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

    /// Erasure-code `body` and store it as one or more block groups, splitting at
    /// `block_group_size` and allocating blocks from OM as needed (the first
    /// from the open key's pre-allocation, the rest via `AllocateBlock`). Returns
    /// the COMMITTED-shape blocks in object order plus the OM open-session
    /// `client_id` (the simple PUT path needs it for `CommitKey`; multipart keeps
    /// the blocks for `CommitPart` and ignores the id). An empty body yields no
    /// block groups (a 0-byte object has no data blocks).
    ///
    /// `multipart` selects the open verb: `None` opens a plain key via
    /// `create_key`; `Some((upload_id, part_number))` opens a per-part key via
    /// `create_multipart_part_key`. Either way the EC profile for encoding comes
    /// from the opened key's first block (`open.blocks[0].ec`) — the bucket
    /// default the OM stamped — NOT from a bucket lookup.
    async fn allocate_and_write(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
        body: &[u8],
        multipart: Option<(&str, u32)>,
    ) -> Result<(Vec<BlockLocation>, u64), GatewayError> {
        let size = body.len() as u64;
        let open: OpenKey = match multipart {
            None => self.om(principal).create_key(S3_VOLUME, bucket, key, None, size).await?,
            Some((upload_id, part_number)) => {
                self.om(principal)
                    .create_multipart_part_key(S3_VOLUME, bucket, key, upload_id, part_number, None, size)
                    .await?
            }
        };
        let client_id = open.client_id;
        // EC for encoding is the opened key's block EC (the bucket default). Only
        // needed when there is data to write; an empty body skips it.
        let ec = open.blocks.first().map(|b| b.ec);
        let mut pre_allocated: std::collections::VecDeque<BlockLocation> = open.blocks.into();

        let mut blocks = Vec::new();
        for segment in body.chunks(self.block_group_size) {
            let block = match pre_allocated.pop_front() {
                Some(b) => b,
                None => {
                    self.om(principal)
                        .allocate_block(S3_VOLUME, bucket, key, client_id)
                        .await?
                }
            };
            let ec = ec.ok_or_else(|| GatewayError::Internal("open key has no EC block".into()))?;
            blocks.push(self.write_block_group(block, ec, segment).await?);
        }
        Ok((blocks, client_id))
    }

    /// EC-encode one `segment` and write its `k+p` shards to the datanodes of the
    /// given pre-allocated `block`'s members (one shard per replica slot: shard
    /// for slot `s` goes to the member with `replica_index == s` — W3). Returns
    /// the COMMITTED [`BlockLocation`] (the same block, with `length` set to the
    /// segment size).
    async fn write_block_group(
        &self,
        block: BlockLocation,
        ec: EcReplicationConfig,
        segment: &[u8],
    ) -> Result<BlockLocation, GatewayError> {
        let profile = ec_profile(ec);
        let shards = ozone_ec::stripe::encode_object(profile, segment)?;
        let container = ContainerId(block.container_id);
        let local = LocalId(block.local_id);

        let k = ec.data as usize;
        let p = ec.parity as usize;
        for slot in 1..=(k + p) {
            let shard: &[u8] = if slot <= k {
                &shards.data[slot - 1]
            } else {
                &shards.parity[slot - 1 - k]
            };
            let endpoint = endpoint_for_slot(&block, slot as u8)?;
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
            bd.set_block_group_len(segment.len() as u64);
            dnc.put_block(&bd, true).await?;
        }

        Ok(BlockLocation {
            length: segment.len() as u64,
            ..block
        })
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
        let (blocks, client_id) = self
            .allocate_and_write(bucket, key, principal, &body, None)
            .await?;
        for (k, v) in tags {
            metadata.insert(format!("{TAG_META_PREFIX}{k}"), v);
        }
        let etag = md5_hex(&body);
        let metadata: Vec<(String, String)> = metadata.into_iter().collect();
        self.om(principal)
            .commit_key(
                S3_VOLUME,
                bucket,
                key,
                client_id,
                &blocks,
                body.len() as u64,
                Some(&etag),
                &metadata,
            )
            .await?;
        Ok(etag)
    }

    /// Server-side DEEP copy: read the source object and write the destination as
    /// brand-new, independent EC blocks (it shares NO block ids with the source, so
    /// deleting the source never affects the copy). Metadata/tagging directives are
    /// applied at the gateway. Returns `(etag, size)`. A missing source maps to
    /// `NoSuchKey`.
    pub async fn copy_object(
        &self,
        dest_bucket: &str,
        dest_key: &str,
        src_bucket: &str,
        src_key: &str,
        principal: &str,
        directives: CopyDirectives,
    ) -> Result<(String, u64), GatewayError> {
        // Deep copy: read the source object and write FRESH EC blocks for the
        // destination, so the copy is fully INDEPENDENT of the source (AWS S3
        // semantics). A metadata-only copy that shared the source's block ids would
        // let a later DELETE of the source destroy the copy -- silent data loss.
        // The data duplication happens here at the gateway; the OM stores only
        // metadata.
        let (body, _src_etag, src_metadata, _mod_ms) =
            self.get_object(src_bucket, src_key, principal).await?;
        let size = body.len() as u64;

        // Split the source's stored metadata into user metadata and tags (tags live
        // as TAG_META_PREFIX-prefixed entries) so the metadata- and tagging-
        // directives apply independently, exactly as S3 does.
        let mut user_meta = HashMap::new();
        let mut src_tags = Vec::new();
        for (k, v) in src_metadata {
            match k.strip_prefix(TAG_META_PREFIX) {
                Some(tag_key) => src_tags.push((tag_key.to_string(), v)),
                None => {
                    user_meta.insert(k, v);
                }
            }
        }
        let metadata = if directives.replace_metadata {
            directives.metadata
        } else {
            user_meta
        };
        let tags = if directives.replace_tags {
            directives.tags
        } else {
            src_tags
        };

        let etag = self
            .put_object(dest_bucket, dest_key, principal, body, metadata, tags)
            .await?;
        Ok((etag, size))
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
        // The OM persists the tags under its tag-metadata prefix; a missing key
        // surfaces as `OmError::NotFound` -> `NoSuchKey` via the `From` impl.
        self.om(principal)
            .put_object_tagging(S3_VOLUME, bucket, key, &tags)
            .await?;
        Ok(())
    }

    /// GET object tagging: the object's tag set, sorted by key (the OM strips the
    /// tag-metadata prefix and sorts). A missing key maps to `NoSuchKey`.
    pub async fn get_object_tagging(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
    ) -> Result<Vec<(String, String)>, GatewayError> {
        Ok(self.om(principal).get_object_tagging(S3_VOLUME, bucket, key).await?)
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
        let meta = self.om(principal).get_key_info(S3_VOLUME, bucket, key).await?;
        Ok((
            meta.etag.unwrap_or_default(),
            meta.size,
            meta.modification_time,
        ))
    }

    /// HEAD object: `(size, etag, metadata, last_modified_ms)` from OM, no data
    /// read.
    pub async fn head_object(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
    ) -> Result<(u64, String, HashMap<String, String>, u64), GatewayError> {
        let meta = self.om(principal).get_key_info(S3_VOLUME, bucket, key).await?;
        Ok((
            meta.size,
            meta.etag.unwrap_or_default(),
            meta.metadata.into_iter().collect(),
            meta.modification_time,
        ))
    }

    /// GET object: gather shards, decode, return `(bytes, etag, metadata,
    /// last_modified_ms)`.
    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
    ) -> Result<(Bytes, String, HashMap<String, String>, u64), GatewayError> {
        let meta = self.om(principal).get_key_info(S3_VOLUME, bucket, key).await?;
        let etag = meta.etag.clone().unwrap_or_default();
        let mod_ms = meta.modification_time;

        // The object is the concatenation of its blocks' groups (one group for a
        // simple PUT; one per part for a completed multipart upload). Each block
        // carries its own EC config and member endpoints.
        let mut out = Vec::with_capacity(meta.size as usize);
        for block in &meta.blocks {
            let part = self.read_block_group(block).await?;
            out.extend_from_slice(&part);
        }
        Ok((
            Bytes::from(out),
            etag,
            meta.metadata.into_iter().collect(),
            mod_ms,
        ))
    }

    /// Read and EC-decode a single `block` group. Gathers every shard it can from
    /// the block's member endpoints (W3: slot `s` from the member with
    /// `replica_index == s`); missing/failed reads stay absent and the decoder
    /// reconstructs as long as `>= k` shards survive.
    async fn read_block_group(&self, block: &BlockLocation) -> Result<Vec<u8>, GatewayError> {
        let ec = block.ec;
        let profile = ec_profile(ec);
        let container = ContainerId(block.container_id);
        let local = LocalId(block.local_id);
        let total = profile.data + profile.parity;
        let mut shard_bufs: Vec<Option<Vec<u8>>> = vec![None; total];
        for slot in 1..=total {
            let endpoint = match endpoint_for_slot(block, slot as u8) {
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
        Ok(ozone_ec::stripe::decode_object(profile, block.length as usize, &views)?)
    }

    /// DELETE object: remove OM metadata and best-effort delete shards. S3
    /// DELETE is idempotent, so a missing key still succeeds.
    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
    ) -> Result<(), GatewayError> {
        // Look up first so we can tear down the shards; absent key -> just 204.
        if let Ok(meta) = self.om(principal).get_key_info(S3_VOLUME, bucket, key).await {
            for block in &meta.blocks {
                let container = ContainerId(block.container_id);
                let local = LocalId(block.local_id);
                let total = (block.ec.data + block.ec.parity) as usize;
                for slot in 1..=total {
                    if let Ok(endpoint) = endpoint_for_slot(block, slot as u8) {
                        if let Ok(mut dnc) = self.dn(&endpoint).await {
                            let bslot =
                                BlockId::ec(container, local, ReplicaIndex::new(slot as u8));
                            let _ = dnc.delete_block(&bslot).await;
                        }
                    }
                }
            }
        }

        self.om(principal).delete_key(S3_VOLUME, bucket, key).await?;
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

    /// Initiate a multipart upload: validate the bucket and get an upload id from
    /// OM (which persists the upload). The gateway keeps no local state.
    pub async fn initiate_multipart(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
    ) -> Result<String, GatewayError> {
        Ok(self.om(principal).initiate_multipart(S3_VOLUME, bucket, key).await?)
    }

    /// Upload one part: EC-encode + store its block group(s), record the part in
    /// OM (the per-part S3 ETag rides the commit), and return the part ETag (MD5
    /// hex, unquoted). The part-number front-door check short-circuits a bad
    /// request before any datanode write; the OM also enforces it authoritatively.
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
        let (blocks, client_id) = self
            .allocate_and_write(bucket, key, principal, &body, Some((upload_id, part_number)))
            .await?;
        let etag_hex = hex(&md5_binary(&body));
        self.om(principal)
            .commit_part(
                S3_VOLUME,
                bucket,
                key,
                upload_id,
                part_number,
                client_id,
                &blocks,
                body.len() as u64,
                &etag_hex,
            )
            .await?;
        Ok(etag_hex)
    }

    /// Complete a multipart upload: forward the client's ordered part numbers to
    /// OM, which validates them against its stored parts (all present, every
    /// non-last part >= 5 MiB), stitches the key from the parts' blocks, and
    /// computes the multipart ETag from its STORED per-part ETags. Returns the
    /// final ETag (`<hash>-N`, unquoted).
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
        // A2 (InvalidPartOrder): strictly ascending, no duplicates. Only the
        // gateway sees the client's XML part order, so this front-door check is
        // authoritative; the OM re-asserts it as defense-in-depth.
        if ordered_part_numbers.windows(2).any(|w| w[0] >= w[1]) {
            return Err(GatewayError::BadRequest(
                "parts must be in ascending order with no duplicates".into(),
            ));
        }
        // The OM holds the part names and per-part ETags; the gateway forwards
        // only the part numbers (empty name/etag), and the OM rolls up the final
        // ETag from what it stored.
        let parts: Vec<(u32, String, String)> = ordered_part_numbers
            .iter()
            .map(|pn| (*pn, String::new(), String::new()))
            .collect();
        let (etag, _size) = self
            .om(principal)
            .complete_multipart(S3_VOLUME, bucket, key, upload_id, &parts)
            .await?;
        Ok(etag)
    }

    /// Abort a multipart upload: tell OM to drop the upload. Already-written part
    /// blocks become garbage reclaimed by container GC (a background reclaimer is
    /// out of scope). Idempotent at the OM (unknown ids succeed).
    pub async fn abort_multipart(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
        upload_id: &str,
    ) -> Result<(), GatewayError> {
        self.om(principal)
            .abort_multipart(S3_VOLUME, bucket, key, upload_id)
            .await?;
        Ok(())
    }

    /// List the parts uploaded so far for an upload, from OM, as
    /// `(part_number, etag_hex, size)`. A missing upload maps to `NoSuchUpload`.
    pub async fn list_parts(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
        upload_id: &str,
    ) -> Result<Vec<PartSummary>, GatewayError> {
        let parts = self
            .om(principal)
            .list_parts(S3_VOLUME, bucket, key, upload_id, 0)
            .await?;
        Ok(parts
            .into_iter()
            .map(|p| (p.part_number, p.etag.unwrap_or_default(), p.size))
            .collect())
    }

    /// List in-flight multipart uploads for a bucket, from OM, as
    /// `(upload_id, key)` (OM returns them sorted by key then upload id).
    pub async fn list_multipart_uploads(
        &self,
        bucket: &str,
        principal: &str,
    ) -> Result<Vec<UploadSummary>, GatewayError> {
        let uploads = self
            .om(principal)
            .list_multipart_uploads(S3_VOLUME, bucket, "")
            .await?;
        Ok(uploads
            .into_iter()
            .map(|u| (u.upload_id, u.key))
            .collect())
    }
}

// ---- helpers ----

fn ec_profile(ec: EcReplicationConfig) -> ozone_ec::Profile {
    ozone_ec::Profile {
        data: ec.data as usize,
        parity: ec.parity as usize,
        chunk_size: ec.ec_chunk_size as usize,
    }
}

/// Resolve the datanode endpoint holding EC replica `slot` (1-based) by finding
/// the block's member whose `replica_index == slot` (W3: slot fidelity comes
/// from the member's index, never its position). The member already carries the
/// full `http://ip:port` dial endpoint.
fn endpoint_for_slot(block: &BlockLocation, slot: u8) -> Result<String, GatewayError> {
    block
        .members
        .iter()
        .find(|m| m.replica_index == slot)
        .map(|m| m.endpoint.clone())
        .ok_or_else(|| GatewayError::Internal(format!("block has no member for slot {slot}")))
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
