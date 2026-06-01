//! Block and container metadata persistence contract.
//!
//! The chunk *bytes* live in a [`crate::ChunkStore`]; their *metadata* (which
//! chunks make up a block, container state and usage counters, the EC config)
//! lives in a [`MetaStore`]. The two are separate because they have different
//! durability and access patterns: chunk data is large and append-mostly, while
//! metadata is small, frequently mutated, and benefits from a transactional KV
//! store. `ozone-fjall-store` provides the production implementation.

use async_trait::async_trait;
use ozone_types::{BlockData, BlockId, ContainerId, ContainerInfo, ContainerState};

use crate::error::StorageError;

/// A page of blocks plus the cursor to resume after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockPage {
    /// Blocks in this page, ordered by ascending `local_id`.
    pub blocks: Vec<BlockData>,
    /// `Some(local_id)` to resume the next page from (pass as `start_local_id`),
    /// or `None` when this page is the last.
    pub next_local_id: Option<u64>,
}

/// Transactional metadata store for containers and the blocks within them.
///
/// # Invariants implementations must uphold
/// - `bcsi` (Block Commit Sequence Id) is a per-container monotonically
///   increasing counter. Every [`MetaStore::put_block`] bumps it and returns the
///   new value; the returned value never decreases for a given container.
/// - `put_block` also keeps the owning container's `block_count` and
///   `used_bytes` consistent with the set of committed blocks (a newly-committed
///   block increments `block_count` and adds its [`BlockData::len`]; overwriting
///   an existing block adjusts `used_bytes` by the delta and leaves
///   `block_count` unchanged).
/// - Writes are rejected with [`StorageError::ContainerNotOpen`] when the
///   container is not [`ContainerState::Open`].
#[async_trait]
pub trait MetaStore: Send + Sync {
    // ---- containers ----

    /// Create a new container record. Fails with
    /// [`StorageError::ContainerExists`] if the id is already present.
    async fn create_container(&self, info: ContainerInfo) -> Result<(), StorageError>;

    /// Fetch a container record, or `None` if absent.
    async fn get_container(
        &self,
        id: ContainerId,
    ) -> Result<Option<ContainerInfo>, StorageError>;

    /// Transition a container to `state`. Fails with
    /// [`StorageError::ContainerNotFound`] if absent.
    async fn set_container_state(
        &self,
        id: ContainerId,
        state: ContainerState,
    ) -> Result<(), StorageError>;

    /// List all container records (unordered). Intended for container reports
    /// to SCM; the datanode's container count is small enough to enumerate.
    async fn list_containers(&self) -> Result<Vec<ContainerInfo>, StorageError>;

    /// Delete a container record and all of its block metadata. Idempotent:
    /// deleting an absent container succeeds. Chunk *bytes* are removed
    /// separately via [`crate::ChunkStore::delete_container`].
    async fn delete_container(&self, id: ContainerId) -> Result<(), StorageError>;

    // ---- blocks ----

    /// Commit (insert or overwrite) a block's metadata, returning the owning
    /// container's new `bcsi`. Fails with [`StorageError::ContainerNotFound`] if
    /// the container is absent or [`StorageError::ContainerNotOpen`] if it is not
    /// open.
    async fn put_block(&self, block: &BlockData) -> Result<u64, StorageError>;

    /// Fetch a block's metadata, or `None` if absent.
    async fn get_block(&self, id: &BlockId) -> Result<Option<BlockData>, StorageError>;

    /// Delete a block's metadata, decrementing the container's `block_count`
    /// and `used_bytes`. Idempotent. Does NOT remove chunk bytes — the caller
    /// deletes those via the [`crate::ChunkStore`].
    async fn delete_block(&self, id: &BlockId) -> Result<(), StorageError>;

    /// List a container's blocks starting at `start_local_id` (inclusive),
    /// returning at most `limit` of them ordered by ascending `local_id`,
    /// together with a resume cursor.
    async fn list_blocks(
        &self,
        container: ContainerId,
        start_local_id: u64,
        limit: usize,
    ) -> Result<BlockPage, StorageError>;
}
