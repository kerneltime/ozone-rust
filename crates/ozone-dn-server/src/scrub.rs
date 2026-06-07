//! Bit-rot scrubber: scan this datanode's stored chunks and verify each against
//! its recorded checksum, surfacing any shard that fails (silent corruption) or
//! is missing. Detection is fully LOCAL (no peers) — that is what catches bit-rot
//! on a present-but-rotten shard, which SCM cannot see from container reports.
//! The repair itself reuses the shared [`crate::repair::reconstruct_and_persist`]
//! primitive, driven with the peer pipeline that SCM (or a coordinator) supplies.

use std::sync::Arc;
use std::time::Duration;

use ozone_storage::{checksum, ChunkStore, MetaStore, StorageError};
use ozone_types::{ContainerId, EcReplicationConfig, LocalId};

/// One corrupt/missing block-group shard a scrub pass found on this datanode.
/// Carries everything the repair primitive needs except the peer `sources`,
/// which the scrubber cannot know locally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairRequest {
    /// The shard's container and block group.
    pub container: ContainerId,
    /// The block group's local id.
    pub local: LocalId,
    /// The EC slot (replica index) of the corrupt/missing shard held here.
    pub slot: u8,
    /// The container's EC config.
    pub ec: EcReplicationConfig,
    /// The block group's length in user bytes.
    pub block_group_len: u64,
}

/// Result of one scrub pass.
#[derive(Debug, Default)]
pub struct ScrubReport {
    /// Chunks read and verified clean.
    pub clean: usize,
    /// Corrupt or missing shards found (each needs repair).
    pub corrupt: Vec<RepairRequest>,
}

/// Scans local container/block metadata and verifies each stored chunk.
pub struct Scrubber {
    meta: Arc<dyn MetaStore>,
    chunks: Arc<dyn ChunkStore>,
    /// Pagination page size for `list_blocks`.
    page: usize,
}

impl Scrubber {
    /// Build a scrubber over a datanode's stores.
    pub fn new(meta: Arc<dyn MetaStore>, chunks: Arc<dyn ChunkStore>) -> Self {
        Self {
            meta,
            chunks,
            page: 256,
        }
    }

    /// Run one full scan pass over every container's blocks and return what was
    /// found. Deterministic and timer-free — the production loop just calls this
    /// on an interval. A chunk is "corrupt" if its bytes fail their recorded
    /// checksum, or "missing" if its file is gone/truncated (`BlockNotFound`).
    /// Driven by container/block metadata (not client reads), so it reaches cold
    /// data.
    pub async fn scrub_once(&self) -> Result<ScrubReport, StorageError> {
        let mut report = ScrubReport::default();
        for c in self.meta.list_containers().await? {
            let Some(ec) = c.ec_config else { continue };
            let mut start = 0u64;
            loop {
                let pager = self
                    .meta
                    .list_blocks(c.container_id, start, self.page)
                    .await?;
                for bd in &pager.blocks {
                    let slot = bd.block_id.replica_index.get();
                    let len = bd.block_group_len().unwrap_or(0);
                    for chunk in &bd.chunks {
                        let Some(cd) = chunk.checksum_data.as_ref() else {
                            continue;
                        };
                        let corrupt = match self.chunks.read_chunk(&bd.block_id, chunk).await {
                            Ok(bytes) => checksum::verify(&bytes, cd).is_err(),
                            Err(StorageError::BlockNotFound(_)) => true,
                            Err(e) => return Err(e),
                        };
                        if corrupt {
                            report.corrupt.push(RepairRequest {
                                container: c.container_id,
                                local: bd.block_id.local_id,
                                slot,
                                ec,
                                block_group_len: len,
                            });
                        } else {
                            report.clean += 1;
                        }
                    }
                }
                match pager.next_local_id {
                    Some(next) => start = next,
                    None => break,
                }
            }
        }
        Ok(report)
    }

    /// Run [`Scrubber::scrub_once`] forever on `interval`, sending each finding to
    /// `repairs` for the SCM loop to act on. Stops when the receiver is dropped.
    pub async fn run(&self, interval: Duration, repairs: tokio::sync::mpsc::Sender<RepairRequest>) {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            match self.scrub_once().await {
                Ok(report) => {
                    for req in report.corrupt {
                        if repairs.send(req).await.is_err() {
                            return; // consumer gone
                        }
                    }
                }
                Err(e) => tracing::warn!("scrub pass failed: {e}"),
            }
        }
    }
}
