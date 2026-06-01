//! Chunk byte storage: the datanode data plane.
//!
//! A chunk is the unit of on-disk data. For EC, the bytes a chunk holds are an
//! already-encoded shard cell — the datanode is dumb storage and performs no EC
//! itself; the gateway (or the reconstruction coordinator) does the GF math and
//! hands finished bytes to [`ChunkStore::write_chunk`].
//!
//! The store is pure byte I/O: it does NOT compute or verify checksums. Callers
//! that need integrity verification combine it with [`crate::checksum`].

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use ozone_types::{BlockId, ChunkInfo, ContainerId};

use crate::error::StorageError;

/// Abstract chunk byte storage.
///
/// Implementations must be safe to share across tasks (`Send + Sync`) and to
/// call concurrently for *distinct* chunks. Concurrent writes to the *same*
/// `(block, chunk)` are not defined — the write protocol has a single writer
/// per chunk.
#[async_trait]
pub trait ChunkStore: Send + Sync {
    /// Durably store `data` as the bytes of `(block, chunk)`, replacing any
    /// existing chunk file. Writes are atomic: a reader sees either the old
    /// bytes or the complete new bytes, never a partial file.
    async fn write_chunk(
        &self,
        block: &BlockId,
        chunk: &ChunkInfo,
        data: Bytes,
    ) -> Result<(), StorageError>;

    /// Read the full bytes of `(block, chunk)`. A missing chunk file maps to
    /// [`StorageError::BlockNotFound`].
    async fn read_chunk(&self, block: &BlockId, chunk: &ChunkInfo)
        -> Result<Bytes, StorageError>;

    /// Delete a single chunk file. Deleting a non-existent chunk is a success
    /// (idempotent), so block/container teardown can be retried safely.
    async fn delete_chunk(&self, block: &BlockId, chunk: &ChunkInfo)
        -> Result<(), StorageError>;

    /// Recursively delete all chunk data for a container. Idempotent.
    async fn delete_container(&self, container: ContainerId) -> Result<(), StorageError>;
}

/// Filesystem-backed [`ChunkStore`] rooted at a single data directory.
///
/// # On-disk layout
/// ```text
/// <root>/<container_id>/chunks/<local_id>_<replica_index>_<chunk_name>
/// ```
/// One file per chunk. `replica_index` is in the path so a datanode that holds
/// more than one EC slot of the same logical block (unusual, but legal) does not
/// collide. Multi-disk placement across several data directories is a higher
/// layer's concern — this store owns exactly one `root`.
pub struct FileChunkStore {
    root: PathBuf,
}

/// Process-local counter making temp-file names unique without a clock/RNG.
static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

impl FileChunkStore {
    /// Create a store rooted at `root`. The directory is created lazily on the
    /// first write; constructing the store does no I/O.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The container's top-level directory.
    fn container_dir(&self, container: ContainerId) -> PathBuf {
        self.root.join(container.get().to_string())
    }

    /// Absolute path of a chunk file.
    fn chunk_path(&self, block: &BlockId, chunk: &ChunkInfo) -> PathBuf {
        self.container_dir(block.container).join("chunks").join(format!(
            "{}_{}_{}",
            block.local_id.get(),
            block.replica_index.get(),
            chunk.chunk_name
        ))
    }

    /// A sibling temp path for atomic create-then-rename. Unique per call within
    /// the process (pid + monotonic counter), so concurrent writers to distinct
    /// chunks never share a temp name.
    fn temp_path(final_path: &Path) -> PathBuf {
        let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let mut name = final_path
            .file_name()
            .map(|n| n.to_os_string())
            .unwrap_or_default();
        name.push(format!(".tmp.{pid}.{seq}"));
        final_path.with_file_name(name)
    }
}

#[async_trait]
impl ChunkStore for FileChunkStore {
    async fn write_chunk(
        &self,
        block: &BlockId,
        chunk: &ChunkInfo,
        data: Bytes,
    ) -> Result<(), StorageError> {
        let path = self.chunk_path(block, chunk);
        // Safe: chunk_path always has a parent (root/<c>/chunks/<file>).
        let dir = path.parent().expect("chunk path always has a parent");
        tokio::fs::create_dir_all(dir).await?;

        let tmp = Self::temp_path(&path);
        // Write the whole payload to the temp file, then atomically rename into
        // place. rename(2) within a directory is atomic on POSIX filesystems.
        tokio::fs::write(&tmp, &data).await?;
        match tokio::fs::rename(&tmp, &path).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // Best-effort cleanup of the orphaned temp file.
                let _ = tokio::fs::remove_file(&tmp).await;
                Err(e.into())
            }
        }
    }

    async fn read_chunk(
        &self,
        block: &BlockId,
        chunk: &ChunkInfo,
    ) -> Result<Bytes, StorageError> {
        let path = self.chunk_path(block, chunk);
        match tokio::fs::read(&path).await {
            Ok(bytes) => Ok(Bytes::from(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(StorageError::BlockNotFound(*block))
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn delete_chunk(
        &self,
        block: &BlockId,
        chunk: &ChunkInfo,
    ) -> Result<(), StorageError> {
        let path = self.chunk_path(block, chunk);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn delete_container(&self, container: ContainerId) -> Result<(), StorageError> {
        let dir = self.container_dir(container);
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ozone_types::{LocalId, ReplicaIndex};

    /// A unique temp dir for a test, without external crates.
    fn scratch_dir(tag: &str) -> PathBuf {
        let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "ozone-chunkstore-{}-{}-{}",
            tag,
            std::process::id(),
            seq
        ));
        dir
    }

    fn chunk(name: &str, len: u64) -> ChunkInfo {
        ChunkInfo {
            chunk_name: name.to_string(),
            offset: 0,
            len,
            checksum_data: None,
            stripe_checksum: None,
        }
    }

    #[tokio::test]
    async fn write_read_round_trip() {
        let root = scratch_dir("rw");
        let store = FileChunkStore::new(&root);
        let block = BlockId::ec(ContainerId(1), LocalId(2), ReplicaIndex::new(1));
        let ci = chunk("c0", 5);
        store
            .write_chunk(&block, &ci, Bytes::from_static(b"hello"))
            .await
            .unwrap();
        let got = store.read_chunk(&block, &ci).await.unwrap();
        assert_eq!(&got[..], b"hello");
        tokio::fs::remove_dir_all(&root).await.ok();
    }

    #[tokio::test]
    async fn overwrite_replaces_bytes() {
        let root = scratch_dir("overwrite");
        let store = FileChunkStore::new(&root);
        let block = BlockId::ec(ContainerId(9), LocalId(1), ReplicaIndex::new(2));
        let ci = chunk("c", 3);
        store
            .write_chunk(&block, &ci, Bytes::from_static(b"aaa"))
            .await
            .unwrap();
        store
            .write_chunk(&block, &ci, Bytes::from_static(b"bbb"))
            .await
            .unwrap();
        let got = store.read_chunk(&block, &ci).await.unwrap();
        assert_eq!(&got[..], b"bbb");
        tokio::fs::remove_dir_all(&root).await.ok();
    }

    #[tokio::test]
    async fn missing_chunk_is_block_not_found() {
        let root = scratch_dir("missing");
        let store = FileChunkStore::new(&root);
        let block = BlockId::ec(ContainerId(1), LocalId(2), ReplicaIndex::new(1));
        let err = store.read_chunk(&block, &chunk("nope", 0)).await.unwrap_err();
        assert!(matches!(err, StorageError::BlockNotFound(_)));
        tokio::fs::remove_dir_all(&root).await.ok();
    }

    #[tokio::test]
    async fn delete_chunk_is_idempotent() {
        let root = scratch_dir("del");
        let store = FileChunkStore::new(&root);
        let block = BlockId::ec(ContainerId(1), LocalId(2), ReplicaIndex::new(1));
        let ci = chunk("c0", 1);
        store
            .write_chunk(&block, &ci, Bytes::from_static(b"x"))
            .await
            .unwrap();
        store.delete_chunk(&block, &ci).await.unwrap();
        // Second delete still succeeds.
        store.delete_chunk(&block, &ci).await.unwrap();
        assert!(matches!(
            store.read_chunk(&block, &ci).await,
            Err(StorageError::BlockNotFound(_))
        ));
        tokio::fs::remove_dir_all(&root).await.ok();
    }

    #[tokio::test]
    async fn delete_container_removes_everything() {
        let root = scratch_dir("delc");
        let store = FileChunkStore::new(&root);
        let block = BlockId::ec(ContainerId(42), LocalId(1), ReplicaIndex::new(1));
        store
            .write_chunk(&block, &chunk("a", 1), Bytes::from_static(b"x"))
            .await
            .unwrap();
        store
            .write_chunk(&block, &chunk("b", 1), Bytes::from_static(b"y"))
            .await
            .unwrap();
        store.delete_container(ContainerId(42)).await.unwrap();
        assert!(matches!(
            store.read_chunk(&block, &chunk("a", 1)).await,
            Err(StorageError::BlockNotFound(_))
        ));
        // Idempotent.
        store.delete_container(ContainerId(42)).await.unwrap();
        tokio::fs::remove_dir_all(&root).await.ok();
    }
}
