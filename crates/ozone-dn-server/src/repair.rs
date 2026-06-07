//! EC repair-at-rest: rebuild a block group's missing/corrupt shard(s) by reading
//! the survivors from peer datanodes, EC-decoding the object, re-encoding to
//! recover the exact stored shards (data AND parity are reproduced byte-for-byte
//! — proven in `ozone-ec/tests/repair_identity.rs`), and persisting the rebuilt
//! shard(s) plus a fresh checksum locally.
//!
//! This is the one datanode operation that is both a *server* (it writes locally
//! through the [`MetaStore`]/[`ChunkStore`] traits the service already holds) and
//! a *client of peers* (it reads survivors via [`DnClient`]). It never makes a
//! loopback RPC to itself.

use std::sync::Arc;

use bytes::Bytes;
use thiserror::Error;

use ozone_dn_client::DnClient;
use ozone_storage::{checksum, ChunkStore, MetaStore, StorageError};
use ozone_types::{
    BlockData, BlockId, ChecksumType, ChunkInfo, ContainerId, ContainerState, EcReplicationConfig,
    LocalId, ReplicaIndex,
};

/// Everything needed to rebuild one block group's missing shard(s) on this DN.
/// All fields are derivable from either trigger (an SCM `ReconstructEC` command
/// or a scrubber finding plus the container's pipeline).
pub struct RepairInput {
    /// The block group's container and block id.
    pub container: ContainerId,
    /// The block group's local id.
    pub local: LocalId,
    /// EC config; carries the cell size used for both the decoder and the
    /// per-shard checksum window.
    pub ec: EcReplicationConfig,
    /// User bytes in this block group — feeds the EC decoder's length so the
    /// trailing partial stripe is sized exactly.
    pub block_group_len: u64,
    /// 1-indexed EC slots this datanode must rebuild.
    pub missing_slots: Vec<u8>,
    /// Surviving peers as `(EC slot 1..=k+p, "http://ip:port")`.
    pub sources: Vec<(u8, String)>,
}

/// Failure of an EC repair.
#[derive(Debug, Error)]
pub enum RepairError {
    /// Fewer than `k` shards could be read, so reconstruction is impossible.
    #[error("not enough surviving shards to reconstruct (have {have}, need {need})")]
    NotEnoughShards { have: usize, need: usize },
    /// The container does not exist locally. Repair refuses rather than create it:
    /// a stale command must not resurrect a just-deleted container.
    #[error("container {0} is absent locally; refusing to create it for repair")]
    ContainerAbsent(ContainerId),
    /// The container exists but is not writable (e.g. Closed). Repair refuses
    /// rather than write chunk bytes that `put_block` would then reject, orphaning
    /// them with no metadata pointer.
    #[error("container {0} is not writable ({1:?}); refusing repair")]
    ContainerNotWritable(ContainerId, ContainerState),
    /// The erasure decoder/encoder failed.
    #[error(transparent)]
    Ec(#[from] ozone_ec::EcError),
    /// A local store write failed.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// Recomputing the rebuilt shard's checksum failed.
    #[error("checksum: {0}")]
    Checksum(String),
}

/// Reconstruct `input.missing_slots` for one block group and persist them locally
/// (bytes + recomputed checksum + block metadata). Returns the slots actually
/// repaired. Idempotent: a re-run re-derives identical shards and overwrites.
pub async fn reconstruct_and_persist(
    meta: &Arc<dyn MetaStore>,
    chunks: &Arc<dyn ChunkStore>,
    input: RepairInput,
) -> Result<Vec<u8>, RepairError> {
    let profile = ozone_ec::Profile {
        data: input.ec.data as usize,
        parity: input.ec.parity as usize,
        chunk_size: input.ec.ec_chunk_size as usize,
    };
    let total = profile.data + profile.parity;
    let k = profile.data;

    // The container must already exist locally AND be writable. We do NOT create
    // it: blindly creating would resurrect a concurrently-deleted container, and
    // writing a shard whose put_block then fails (non-Open) would orphan the chunk
    // bytes with no metadata pointer. A real SCM only reconstructs into a live,
    // provisioned replica; a brand-new whole-replica target is provisioned
    // separately, not here.
    match meta.get_container(input.container).await? {
        Some(ci) if ci.state.is_writable() => {}
        Some(ci) => return Err(RepairError::ContainerNotWritable(input.container, ci.state)),
        None => return Err(RepairError::ContainerAbsent(input.container)),
    }

    // Gather surviving shards from peers; never read a slot we are rebuilding.
    let mut shard_bufs: Vec<Option<Vec<u8>>> = vec![None; total];
    for (slot, endpoint) in &input.sources {
        let slot = *slot;
        if slot == 0 || slot as usize > total || input.missing_slots.contains(&slot) {
            continue;
        }
        let mut peer = match DnClient::connect(endpoint.clone()).await {
            Ok(p) => p,
            Err(_) => continue,
        };
        let bslot = BlockId::ec(input.container, input.local, ReplicaIndex::new(slot));
        let chunk = ChunkInfo {
            chunk_name: "0".to_string(),
            offset: 0,
            len: 0,
            checksum_data: None,
            stripe_checksum: None,
        };
        // verify=true: each peer validates its own shard against its stored
        // checksum, so a peer whose data is itself rotten is dropped here rather
        // than feeding the decoder corrupt bytes.
        if let Ok(b) = peer.read_chunk(&bslot, &chunk, true).await {
            shard_bufs[slot as usize - 1] = Some(b.to_vec());
        }
    }

    let present = shard_bufs.iter().filter(|o| o.is_some()).count();
    if present < k {
        return Err(RepairError::NotEnoughShards { have: present, need: k });
    }

    // Recover the object, then re-encode to recover the exact stored shards.
    let views: Vec<Option<&[u8]>> = shard_bufs.iter().map(|o| o.as_deref()).collect();
    let object = ozone_ec::stripe::decode_object(profile, input.block_group_len as usize, &views)?;
    let shards = ozone_ec::stripe::encode_object(profile, &object)?;

    // Persist each rebuilt slot: bytes + recomputed checksum + block metadata.
    // Writing metadata (not just bytes) is required: a later read_chunk(verify=true)
    // with no caller-supplied checksum loads the expected digest from this block
    // metadata, so a stale digest would fail the next verify.
    let mut repaired = Vec::new();
    for &slot in &input.missing_slots {
        let s = slot as usize;
        if s == 0 || s > total {
            continue;
        }
        let shard: &[u8] = if s <= k {
            &shards.data[s - 1]
        } else {
            &shards.parity[s - 1 - k]
        };
        let cd = checksum::compute(shard, input.ec.ec_chunk_size, ChecksumType::Crc32c)
            .map_err(|e| RepairError::Checksum(e.to_string()))?;
        let chunk = ChunkInfo {
            chunk_name: "0".to_string(),
            offset: 0,
            len: shard.len() as u64,
            checksum_data: Some(cd),
            stripe_checksum: None,
        };
        let bslot = BlockId::ec(input.container, input.local, ReplicaIndex::new(slot));
        chunks
            .write_chunk(&bslot, &chunk, Bytes::copy_from_slice(shard))
            .await?;
        let mut bd = BlockData::new(bslot);
        bd.chunks.push(chunk);
        bd.set_block_group_len(input.block_group_len);
        meta.put_block(&bd).await?;
        repaired.push(slot);
    }
    Ok(repaired)
}
