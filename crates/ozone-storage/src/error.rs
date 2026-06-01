//! Storage-layer error types.

use ozone_types::{BlockId, ChecksumType, ContainerId, ContainerState};
use thiserror::Error;

/// Failure modes of the chunk store and metadata store.
///
/// `Meta` and `Corrupt` carry strings rather than a concrete backend error so
/// this crate stays free of any specific KV-store dependency (the fjall
/// implementation lives in `ozone-fjall-store` and maps its errors into these).
#[derive(Debug, Error)]
pub enum StorageError {
    /// Underlying filesystem I/O failed.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// A chunk-integrity check failed.
    #[error(transparent)]
    Checksum(#[from] ChecksumError),

    /// Operation referenced a container that does not exist.
    #[error("container {0} not found")]
    ContainerNotFound(ContainerId),

    /// Tried to create a container id that already exists.
    #[error("container {0} already exists")]
    ContainerExists(ContainerId),

    /// Write attempted against a container not in the `Open` state.
    #[error("container {0} is not open (state {1:?})")]
    ContainerNotOpen(ContainerId, ContainerState),

    /// Operation referenced a block that does not exist.
    #[error("block {0} not found")]
    BlockNotFound(BlockId),

    /// The metadata backend (e.g. fjall) returned an error.
    #[error("metadata store error: {0}")]
    Meta(String),

    /// Stored bytes could not be decoded into the expected shape (e.g. a
    /// corrupt block-metadata record).
    #[error("corrupt stored data: {0}")]
    Corrupt(String),
}

/// Checksum computation / verification failures.
#[derive(Debug, Error)]
pub enum ChecksumError {
    /// A per-window digest did not match the recomputed value.
    #[error("checksum mismatch at window {window}")]
    Mismatch {
        /// Zero-based window index whose digest disagreed.
        window: usize,
    },

    /// The number of stored digests does not match the number of windows the
    /// data divides into.
    #[error("checksum count mismatch: data spans {windows} windows but {provided} digests were provided")]
    CountMismatch {
        /// Windows the data divides into for the declared `bytes_per_checksum`.
        windows: usize,
        /// Digests actually present.
        provided: usize,
    },

    /// The algorithm is recognized but not implemented for compute/verify here.
    /// Chunk-data integrity in this datanode is CRC32C (Ozone's default);
    /// SHA256/MD5/CRC32 are reserved.
    #[error("unsupported checksum type for chunk verification: {0:?}")]
    Unsupported(ChecksumType),
}
