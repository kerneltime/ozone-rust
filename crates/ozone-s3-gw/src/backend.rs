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

use std::collections::HashMap;

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

/// The gateway backend. Holds the OM channel and a per-datanode channel cache;
/// per-call clients are built from these cloneable channels.
pub struct Gateway {
    om_channel: Channel,
    dn_channels: Mutex<HashMap<String, Channel>>,
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

    /// PUT object: encode, fan shards to datanodes, commit metadata. Returns the
    /// object's ETag (MD5 hex, unquoted).
    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
        body: Bytes,
    ) -> Result<String, GatewayError> {
        let mut om = self.om();

        // Bucket existence + EC config.
        let hb = om
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
        let profile = ec_profile(ec);

        // Encode into k+p shards.
        let shards = ozone_ec::stripe::encode_object(profile, &body)?;

        // Allocate a block + pipeline.
        let vbk = om::VolumeBucketKey {
            volume_name: S3_VOLUME.to_string(),
            bucket_name: bucket.to_string(),
            key_name: key.to_string(),
        };
        let ck = om
            .create_key(om::CreateKeyRequest {
                vbk: Some(vbk.clone()),
                expected_size: body.len() as u64,
                ec_config: Some(ec_wire.clone()),
                metadata: HashMap::new(),
                auth: Some(auth(principal)),
            })
            .await?;
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

        // Write each shard to the datanode holding its slot.
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

        // ETag + commit.
        let etag = md5_hex(&body);
        let final_loc = om::KeyLocation {
            block_id: Some(group),
            offset: 0,
            length: body.len() as u64,
            pipeline: Some(pipeline),
            block_token: Vec::new(),
        };
        om.commit_key(om::CommitKeyRequest {
            client_id: ck.client_id,
            open_version: ck.open_version,
            vbk: Some(vbk),
            final_size: body.len() as u64,
            final_locations: vec![final_loc],
            etag: etag.clone().into_bytes(),
            auth: Some(auth(principal)),
        })
        .await?;

        Ok(etag)
    }

    /// HEAD object: `(size, etag)` from OM metadata, no data read.
    pub async fn head_object(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
    ) -> Result<(u64, String), GatewayError> {
        let info = self.lookup(bucket, key, principal).await?;
        Ok((info.data_size, etag_of(&info)))
    }

    /// GET object: gather shards, decode, return `(bytes, etag)`.
    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        principal: &str,
    ) -> Result<(Bytes, String), GatewayError> {
        let info = self.lookup(bucket, key, principal).await?;
        let size = info.data_size as usize;
        let etag = etag_of(&info);
        let ec = to_domain_ec(
            info.ec_config
                .as_ref()
                .ok_or_else(|| GatewayError::Internal("key has no EC config".into()))?,
        )?;
        let profile = ec_profile(ec);
        let loc = info
            .locations
            .into_iter()
            .next()
            .ok_or_else(|| GatewayError::Internal("key has no locations".into()))?;
        let pipeline = loc
            .pipeline
            .ok_or_else(|| GatewayError::Internal("location has no pipeline".into()))?;
        let group = loc
            .block_id
            .ok_or_else(|| GatewayError::Internal("location has no block id".into()))?;
        let container = ContainerId(group.container_id);
        let local = LocalId(group.local_id);
        let total = (ec.data + ec.parity) as usize;

        // Gather every shard we can; missing/failed reads stay None and EC
        // reconstructs as long as >= k survive.
        let mut shard_bufs: Vec<Option<Vec<u8>>> = vec![None; total];
        for slot in 1..=total {
            let endpoint = match endpoint_for_slot(&pipeline, slot as u32) {
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
            if let Ok(b) = dnc.read_chunk(&block_slot, &chunk, false).await {
                shard_bufs[slot - 1] = Some(b.to_vec());
            }
        }

        let views: Vec<Option<&[u8]>> = shard_bufs.iter().map(|o| o.as_deref()).collect();
        let data = ozone_ec::stripe::decode_object(profile, size, &views)?;
        Ok((Bytes::from(data), etag))
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

fn md5_hex(data: &[u8]) -> String {
    let mut h = Md5::new();
    h.update(data);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}
