//! Datanode storage layer: the contracts and the on-disk data plane.
//!
//! Two orthogonal concerns:
//! - [`ChunkStore`] — chunk *byte* storage (the data plane). [`FileChunkStore`]
//!   is the filesystem implementation. The store performs no EC and no checksum
//!   work; it writes and reads opaque bytes atomically.
//! - [`MetaStore`] — block and container *metadata* (the control plane). This
//!   crate defines only the trait; `ozone-fjall-store` implements it.
//!
//! [`checksum`] provides CRC32C compute/verify used by callers that combine the
//! two planes (e.g. the datanode verifies chunk bytes on read against the
//! [`ozone_types::ChecksumData`] recorded in block metadata).
//!
//! # Why split byte storage from metadata
//! Chunk data is large and write-once; metadata is small and frequently
//! mutated, and needs transactional updates (a block commit must atomically bump
//! the container's `bcsi`, `block_count`, and `used_bytes`). Different stores fit
//! these different access patterns, and keeping the byte path free of any KV
//! dependency keeps it simple and fast.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod checksum;
mod chunk_store;
mod error;
mod meta;

pub use chunk_store::{ChunkStore, FileChunkStore};
pub use error::{ChecksumError, StorageError};
pub use meta::{BlockPage, MetaStore};
