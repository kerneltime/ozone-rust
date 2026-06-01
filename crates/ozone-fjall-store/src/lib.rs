//! fjall-backed [`MetaStore`]: container and block metadata persistence.
//!
//! Two partitions in one fjall keyspace:
//! - `containers`: `container_id` (8-byte big-endian) -> postcard([`StoredContainer`]).
//! - `blocks`: `container_id || local_id || replica_index` (8 + 8 + 1 bytes,
//!   big-endian) -> postcard([`ozone_types::BlockData`]). The key ordering means
//!   a prefix scan over `container_id` yields a container's blocks in ascending
//!   `(local_id, replica_index)` order, which is exactly what `list_blocks`
//!   pagination needs.
//!
//! # Transactional invariants
//! `put_block` and `delete_block` perform a read-modify-write of the owning
//! container's `bcsi`, `block_count`, and `used_bytes`. To keep those updates
//! atomic AND serializable, every *mutating* operation takes a single process
//! write lock and commits its block + container changes through one fjall
//! [`Batch`] (cross-partition atomic). The lock is coarse (one per store, not per
//! container); metadata mutations are far lighter than the chunk-data path that
//! does NOT take it, so this is a deliberate simplicity-over-throughput choice.
//! Reads take no lock and may observe a slightly stale container counter, which
//! is acceptable for reporting.
//!
//! Values are encoded with `postcard` (compact binary). The domain types own
//! their `serde` derives; this crate never converts to the gRPC wire types.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use ozone_storage::{BlockPage, MetaStore, StorageError};
use ozone_types::{BlockData, BlockId, ContainerId, ContainerInfo, ContainerState};

/// On-disk container record: the public [`ContainerInfo`] plus the datanode's
/// private per-container `bcsi` (Block Commit Sequence Id) counter.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredContainer {
    info: ContainerInfo,
    bcsi: u64,
}

/// fjall-backed metadata store. Cheap to clone is NOT implied; share via `Arc`.
pub struct FjallMetaStore {
    keyspace: Keyspace,
    containers: PartitionHandle,
    blocks: PartitionHandle,
    /// Serializes mutating operations so container counter read-modify-writes do
    /// not race. Held only across the (fast) metadata batch, never across chunk
    /// I/O.
    write_lock: Arc<Mutex<()>>,
}

impl FjallMetaStore {
    /// Open (or recover) a metadata store rooted at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let keyspace = Config::new(path).open().map_err(fj)?;
        let containers = keyspace
            .open_partition("containers", PartitionCreateOptions::default())
            .map_err(fj)?;
        let blocks = keyspace
            .open_partition("blocks", PartitionCreateOptions::default())
            .map_err(fj)?;
        Ok(Self {
            keyspace,
            containers,
            blocks,
            write_lock: Arc::new(Mutex::new(())),
        })
    }
}

// ---- key + value codec helpers ----

fn container_key(id: ContainerId) -> Vec<u8> {
    id.get().to_be_bytes().to_vec()
}

fn block_key(id: &BlockId) -> Vec<u8> {
    let mut k = Vec::with_capacity(17);
    k.extend_from_slice(&id.container.get().to_be_bytes());
    k.extend_from_slice(&id.local_id.get().to_be_bytes());
    k.push(id.replica_index.get());
    k
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, StorageError> {
    postcard::to_allocvec(value).map_err(|e| StorageError::Corrupt(format!("encode: {e}")))
}

fn decode<T: DeserializeOwned>(bytes: impl AsRef<[u8]>) -> Result<T, StorageError> {
    postcard::from_bytes(bytes.as_ref()).map_err(|e| StorageError::Corrupt(format!("decode: {e}")))
}

/// Map a fjall backend error into [`StorageError::Meta`].
fn fj(e: fjall::Error) -> StorageError {
    StorageError::Meta(e.to_string())
}

/// Run a blocking fjall closure on the blocking pool and flatten the join error.
async fn blocking<F, T>(f: F) -> Result<T, StorageError>
where
    F: FnOnce() -> Result<T, StorageError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| StorageError::Meta(format!("metadata task join failed: {e}")))?
}

#[async_trait]
impl MetaStore for FjallMetaStore {
    async fn create_container(&self, info: ContainerInfo) -> Result<(), StorageError> {
        let containers = self.containers.clone();
        let lock = self.write_lock.clone();
        blocking(move || {
            let _guard = lock.lock().expect("metadata write lock poisoned");
            let key = container_key(info.container_id);
            if containers.contains_key(&key).map_err(fj)? {
                return Err(StorageError::ContainerExists(info.container_id));
            }
            let record = StoredContainer { info, bcsi: 0 };
            containers.insert(&key, encode(&record)?).map_err(fj)?;
            Ok(())
        })
        .await
    }

    async fn get_container(
        &self,
        id: ContainerId,
    ) -> Result<Option<ContainerInfo>, StorageError> {
        let containers = self.containers.clone();
        blocking(move || match containers.get(container_key(id)).map_err(fj)? {
            Some(v) => Ok(Some(decode::<StoredContainer>(&v)?.info)),
            None => Ok(None),
        })
        .await
    }

    async fn set_container_state(
        &self,
        id: ContainerId,
        state: ContainerState,
    ) -> Result<(), StorageError> {
        let containers = self.containers.clone();
        let lock = self.write_lock.clone();
        blocking(move || {
            let _guard = lock.lock().expect("metadata write lock poisoned");
            let key = container_key(id);
            let mut record: StoredContainer = match containers.get(&key).map_err(fj)? {
                Some(v) => decode(&v)?,
                None => return Err(StorageError::ContainerNotFound(id)),
            };
            record.info.state = state;
            containers.insert(&key, encode(&record)?).map_err(fj)?;
            Ok(())
        })
        .await
    }

    async fn list_containers(&self) -> Result<Vec<ContainerInfo>, StorageError> {
        let containers = self.containers.clone();
        blocking(move || {
            let mut out = Vec::new();
            for kv in containers.iter() {
                let (_, v) = kv.map_err(fj)?;
                out.push(decode::<StoredContainer>(&v)?.info);
            }
            Ok(out)
        })
        .await
    }

    async fn delete_container(&self, id: ContainerId) -> Result<(), StorageError> {
        let keyspace = self.keyspace.clone();
        let containers = self.containers.clone();
        let blocks = self.blocks.clone();
        let lock = self.write_lock.clone();
        blocking(move || {
            let _guard = lock.lock().expect("metadata write lock poisoned");
            let ckey = container_key(id);
            // Collect this container's block keys, then remove container record
            // and every block in one atomic batch.
            let prefix = ckey.clone();
            let mut batch = keyspace.batch();
            for kv in blocks.prefix(&prefix) {
                let (k, _) = kv.map_err(fj)?;
                batch.remove(&blocks, k);
            }
            batch.remove(&containers, ckey);
            batch.commit().map_err(fj)?;
            Ok(())
        })
        .await
    }

    async fn put_block(&self, block: &BlockData) -> Result<u64, StorageError> {
        let keyspace = self.keyspace.clone();
        let containers = self.containers.clone();
        let blocks = self.blocks.clone();
        let lock = self.write_lock.clone();
        let block = block.clone();
        blocking(move || {
            let _guard = lock.lock().expect("metadata write lock poisoned");
            let cid = block.block_id.container;
            let ckey = container_key(cid);
            let mut record: StoredContainer = match containers.get(&ckey).map_err(fj)? {
                Some(v) => decode(&v)?,
                None => return Err(StorageError::ContainerNotFound(cid)),
            };
            if !record.info.state.is_writable() {
                return Err(StorageError::ContainerNotOpen(cid, record.info.state));
            }

            let bkey = block_key(&block.block_id);
            let new_len = block.len();
            match blocks.get(&bkey).map_err(fj)? {
                Some(prev) => {
                    // Overwrite: adjust used_bytes by the delta, count unchanged.
                    let prev_len = decode::<BlockData>(&prev)?.len();
                    record.info.used_bytes =
                        record.info.used_bytes.saturating_sub(prev_len) + new_len;
                }
                None => {
                    record.info.block_count += 1;
                    record.info.used_bytes += new_len;
                }
            }
            record.bcsi += 1;
            let new_bcsi = record.bcsi;

            let mut batch = keyspace.batch();
            batch.insert(&blocks, bkey, encode(&block)?);
            batch.insert(&containers, ckey, encode(&record)?);
            batch.commit().map_err(fj)?;
            Ok(new_bcsi)
        })
        .await
    }

    async fn get_block(&self, id: &BlockId) -> Result<Option<BlockData>, StorageError> {
        let blocks = self.blocks.clone();
        let key = block_key(id);
        blocking(move || match blocks.get(key).map_err(fj)? {
            Some(v) => Ok(Some(decode::<BlockData>(&v)?)),
            None => Ok(None),
        })
        .await
    }

    async fn delete_block(&self, id: &BlockId) -> Result<(), StorageError> {
        let keyspace = self.keyspace.clone();
        let containers = self.containers.clone();
        let blocks = self.blocks.clone();
        let lock = self.write_lock.clone();
        let id = *id;
        blocking(move || {
            let _guard = lock.lock().expect("metadata write lock poisoned");
            let bkey = block_key(&id);
            let existing = match blocks.get(&bkey).map_err(fj)? {
                Some(v) => decode::<BlockData>(&v)?,
                None => return Ok(()), // idempotent: nothing to delete
            };
            let ckey = container_key(id.container);
            let mut batch = keyspace.batch();
            batch.remove(&blocks, bkey);
            // Keep container counters consistent if the container still exists.
            if let Some(v) = containers.get(&ckey).map_err(fj)? {
                let mut record: StoredContainer = decode(&v)?;
                record.info.block_count = record.info.block_count.saturating_sub(1);
                record.info.used_bytes = record.info.used_bytes.saturating_sub(existing.len());
                batch.insert(&containers, ckey, encode(&record)?);
            }
            batch.commit().map_err(fj)?;
            Ok(())
        })
        .await
    }

    async fn list_blocks(
        &self,
        container: ContainerId,
        start_local_id: u64,
        limit: usize,
    ) -> Result<BlockPage, StorageError> {
        let blocks = self.blocks.clone();
        blocking(move || {
            let prefix = container_key(container);
            let mut out = Vec::new();
            let mut next_local_id = None;
            for kv in blocks.prefix(&prefix) {
                let (_, v) = kv.map_err(fj)?;
                let bd: BlockData = decode(&v)?;
                let lid = bd.block_id.local_id.get();
                if lid < start_local_id {
                    continue;
                }
                if out.len() >= limit {
                    next_local_id = Some(lid);
                    break;
                }
                out.push(bd);
            }
            Ok(BlockPage {
                blocks: out,
                next_local_id,
            })
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ozone_types::{EcReplicationConfig, LocalId, ReplicaIndex};
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    fn scratch() -> std::path::PathBuf {
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("ozone-fjall-{}-{}", std::process::id(), n))
    }

    async fn store() -> (FjallMetaStore, std::path::PathBuf) {
        let dir = scratch();
        let s = FjallMetaStore::open(&dir).unwrap();
        (s, dir)
    }

    fn open_container(id: u64) -> ContainerInfo {
        ContainerInfo::new_open(ContainerId(id), EcReplicationConfig::RS_6_3_1MIB)
    }

    fn block(c: u64, local: u64, slot: u8, chunk_len: u64) -> BlockData {
        let mut b = BlockData::new(BlockId::ec(
            ContainerId(c),
            LocalId(local),
            ReplicaIndex::new(slot),
        ));
        b.chunks.push(ozone_types::ChunkInfo {
            chunk_name: "c0".into(),
            offset: 0,
            len: chunk_len,
            checksum_data: None,
            stripe_checksum: None,
        });
        b
    }

    #[tokio::test]
    async fn create_get_and_duplicate_container() {
        let (s, dir) = store().await;
        s.create_container(open_container(1)).await.unwrap();
        let got = s.get_container(ContainerId(1)).await.unwrap().unwrap();
        assert_eq!(got.state, ContainerState::Open);
        assert_eq!(got.block_count, 0);
        assert!(matches!(
            s.create_container(open_container(1)).await,
            Err(StorageError::ContainerExists(ContainerId(1)))
        ));
        assert!(s.get_container(ContainerId(2)).await.unwrap().is_none());
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn put_block_bumps_bcsi_and_container_stats() {
        let (s, dir) = store().await;
        s.create_container(open_container(1)).await.unwrap();
        let b1 = s.put_block(&block(1, 1, 1, 100)).await.unwrap();
        let b2 = s.put_block(&block(1, 2, 1, 50)).await.unwrap();
        assert_eq!((b1, b2), (1, 2));
        let c = s.get_container(ContainerId(1)).await.unwrap().unwrap();
        assert_eq!(c.block_count, 2);
        assert_eq!(c.used_bytes, 150);
        // Overwrite block 1 with a larger payload: count stays, bytes adjust.
        let b3 = s.put_block(&block(1, 1, 1, 200)).await.unwrap();
        assert_eq!(b3, 3);
        let c = s.get_container(ContainerId(1)).await.unwrap().unwrap();
        assert_eq!(c.block_count, 2);
        assert_eq!(c.used_bytes, 250); // 150 - 100 + 200
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn put_block_requires_open_container() {
        let (s, dir) = store().await;
        assert!(matches!(
            s.put_block(&block(7, 1, 1, 10)).await,
            Err(StorageError::ContainerNotFound(ContainerId(7)))
        ));
        s.create_container(open_container(7)).await.unwrap();
        s.set_container_state(ContainerId(7), ContainerState::Closed)
            .await
            .unwrap();
        assert!(matches!(
            s.put_block(&block(7, 1, 1, 10)).await,
            Err(StorageError::ContainerNotOpen(ContainerId(7), ContainerState::Closed))
        ));
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn get_and_delete_block() {
        let (s, dir) = store().await;
        s.create_container(open_container(1)).await.unwrap();
        s.put_block(&block(1, 5, 1, 64)).await.unwrap();
        let id = BlockId::ec(ContainerId(1), LocalId(5), ReplicaIndex::new(1));
        assert!(s.get_block(&id).await.unwrap().is_some());
        s.delete_block(&id).await.unwrap();
        assert!(s.get_block(&id).await.unwrap().is_none());
        // Idempotent second delete.
        s.delete_block(&id).await.unwrap();
        let c = s.get_container(ContainerId(1)).await.unwrap().unwrap();
        assert_eq!(c.block_count, 0);
        assert_eq!(c.used_bytes, 0);
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn list_blocks_paginates() {
        let (s, dir) = store().await;
        s.create_container(open_container(1)).await.unwrap();
        for local in 1..=5u64 {
            s.put_block(&block(1, local, 1, 10)).await.unwrap();
        }
        let page = s.list_blocks(ContainerId(1), 0, 2).await.unwrap();
        assert_eq!(page.blocks.len(), 2);
        assert_eq!(page.blocks[0].block_id.local_id, LocalId(1));
        assert_eq!(page.next_local_id, Some(3));
        // Resume.
        let page2 = s
            .list_blocks(ContainerId(1), page.next_local_id.unwrap(), 2)
            .await
            .unwrap();
        assert_eq!(page2.blocks[0].block_id.local_id, LocalId(3));
        assert_eq!(page2.next_local_id, Some(5));
        let page3 = s.list_blocks(ContainerId(1), 5, 10).await.unwrap();
        assert_eq!(page3.blocks.len(), 1);
        assert_eq!(page3.next_local_id, None);
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn delete_container_removes_blocks_and_record() {
        let (s, dir) = store().await;
        s.create_container(open_container(3)).await.unwrap();
        s.put_block(&block(3, 1, 1, 10)).await.unwrap();
        s.put_block(&block(3, 2, 1, 10)).await.unwrap();
        s.delete_container(ContainerId(3)).await.unwrap();
        assert!(s.get_container(ContainerId(3)).await.unwrap().is_none());
        let id = BlockId::ec(ContainerId(3), LocalId(1), ReplicaIndex::new(1));
        assert!(s.get_block(&id).await.unwrap().is_none());
        // Idempotent.
        s.delete_container(ContainerId(3)).await.unwrap();
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn list_containers_enumerates() {
        let (s, dir) = store().await;
        s.create_container(open_container(1)).await.unwrap();
        s.create_container(open_container(2)).await.unwrap();
        let mut ids: Vec<u64> = s
            .list_containers()
            .await
            .unwrap()
            .into_iter()
            .map(|c| c.container_id.get())
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2]);
        tokio::fs::remove_dir_all(&dir).await.ok();
    }
}
