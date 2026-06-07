//! Durability: a datanode's on-disk state must survive a process restart. Write EC
//! shard bytes + block metadata + container metadata, DROP the stores (simulating
//! shutdown), reopen the SAME directory, and prove everything is intact and still
//! verifies against its checksum. This is the foundational "we don't lose data"
//! property; it is asserted empirically here, not assumed.

use std::sync::Arc;

use bytes::Bytes;
use ozone_fjall_store::FjallMetaStore;
use ozone_storage::{checksum, ChunkStore, FileChunkStore, MetaStore};
use ozone_types::{
    BlockData, BlockId, ChecksumType, ChunkInfo, ContainerId, ContainerInfo, EcReplicationConfig,
    LocalId, ReplicaIndex,
};

#[tokio::test]
async fn datanode_stores_survive_restart() {
    let dir = std::env::temp_dir().join(format!("ozone-durability-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    let ec = EcReplicationConfig::rs(3, 2, 8);
    let container = ContainerId(42);
    let bslot = BlockId::ec(container, LocalId(7), ReplicaIndex::new(1));
    let payload: &[u8] = b"durable bytes that must survive a datanode restart";
    let cd = checksum::compute(payload, 8, ChecksumType::Crc32c).unwrap();
    let chunk = ChunkInfo {
        chunk_name: "0".to_string(),
        offset: 0,
        len: payload.len() as u64,
        checksum_data: Some(cd),
        stripe_checksum: None,
    };

    // --- session 1: write, then drop the stores (simulate a clean shutdown) ---
    {
        let meta: Arc<dyn MetaStore> = Arc::new(FjallMetaStore::open(dir.join("meta")).unwrap());
        let chunks: Arc<dyn ChunkStore> = Arc::new(FileChunkStore::new(dir.join("data")));
        meta.create_container(ContainerInfo::new_open(container, ec))
            .await
            .unwrap();
        chunks
            .write_chunk(&bslot, &chunk, Bytes::from_static(payload))
            .await
            .unwrap();
        let mut bd = BlockData::new(bslot);
        bd.chunks.push(chunk.clone());
        bd.set_block_group_len(payload.len() as u64);
        meta.put_block(&bd).await.unwrap();
    } // meta + chunks dropped here

    // --- session 2: reopen the SAME directory; every piece must be intact ---
    {
        let meta: Arc<dyn MetaStore> = Arc::new(FjallMetaStore::open(dir.join("meta")).unwrap());
        let chunks: Arc<dyn ChunkStore> = Arc::new(FileChunkStore::new(dir.join("data")));

        let ci = meta
            .get_container(container)
            .await
            .unwrap()
            .expect("container metadata must survive restart");
        assert_eq!(ci.ec_config, Some(ec), "EC config survives restart");

        let bd = meta
            .get_block(&bslot)
            .await
            .unwrap()
            .expect("block metadata must survive restart");
        assert_eq!(
            bd.block_group_len(),
            Some(payload.len() as u64),
            "block-group length survives restart"
        );
        let stored_cd = bd.chunks[0]
            .checksum_data
            .as_ref()
            .expect("stored checksum survives restart");

        let got = chunks
            .read_chunk(&bslot, &bd.chunks[0])
            .await
            .expect("chunk bytes must survive restart");
        assert_eq!(got.as_ref(), payload, "chunk bytes byte-identical after restart");
        checksum::verify(&got, stored_cd).expect("restored chunk verifies clean against stored checksum");

        // The scrubber's listing view must also see the restored state.
        let containers = meta.list_containers().await.unwrap();
        assert!(
            containers.iter().any(|c| c.container_id == container),
            "list_containers sees the restored container"
        );
        let page = meta.list_blocks(container, 0, 256).await.unwrap();
        assert!(
            page.blocks.iter().any(|b| b.block_id == bslot),
            "list_blocks sees the restored block"
        );
    }

    std::fs::remove_dir_all(&dir).ok();
}

fn ec() -> EcReplicationConfig {
    EcReplicationConfig::rs(3, 2, 8)
}

/// Write one block group's shard (chunk bytes + checksum + block metadata).
async fn put_block(
    meta: &Arc<dyn MetaStore>,
    chunks: &Arc<dyn ChunkStore>,
    container: ContainerId,
    local: u64,
    bytes: &[u8],
) {
    let _ = meta
        .create_container(ContainerInfo::new_open(container, ec()))
        .await;
    let cd = checksum::compute(bytes, 8, ChecksumType::Crc32c).unwrap();
    let chunk = ChunkInfo {
        chunk_name: "0".to_string(),
        offset: 0,
        len: bytes.len() as u64,
        checksum_data: Some(cd),
        stripe_checksum: None,
    };
    let bslot = BlockId::ec(container, LocalId(local), ReplicaIndex::new(1));
    chunks
        .write_chunk(&bslot, &chunk, Bytes::copy_from_slice(bytes))
        .await
        .unwrap();
    let mut bd = BlockData::new(bslot);
    bd.chunks.push(chunk);
    bd.set_block_group_len(bytes.len() as u64);
    meta.put_block(&bd).await.unwrap();
}

fn open(dir: &std::path::Path) -> (Arc<dyn MetaStore>, Arc<dyn ChunkStore>) {
    (
        Arc::new(FjallMetaStore::open(dir.join("meta")).unwrap()),
        Arc::new(FileChunkStore::new(dir.join("data"))),
    )
}

/// Many blocks across a pagination boundary must all survive a restart and still
/// paginate (the scrubber's view).
#[tokio::test]
async fn many_blocks_survive_restart_with_pagination() {
    let dir = std::env::temp_dir().join(format!("ozone-dur-many-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let container = ContainerId(1);
    const N: u64 = 300; // > one 256-block page

    {
        let (meta, chunks) = open(&dir);
        for local in 1..=N {
            put_block(&meta, &chunks, container, local, format!("block-{local}").as_bytes()).await;
        }
    }
    {
        let (meta, chunks) = open(&dir);
        let mut seen = std::collections::BTreeSet::new();
        let mut start = 0u64;
        loop {
            let page = meta.list_blocks(container, start, 256).await.unwrap();
            for b in &page.blocks {
                seen.insert(b.block_id.local_id.0);
            }
            match page.next_local_id {
                Some(next) => start = next,
                None => break,
            }
        }
        assert_eq!(seen.len() as u64, N, "all {N} blocks survive restart and paginate");
        for local in [1u64, 150, N] {
            let bslot = BlockId::ec(container, LocalId(local), ReplicaIndex::new(1));
            let bd = meta.get_block(&bslot).await.unwrap().unwrap();
            let got = chunks.read_chunk(&bslot, &bd.chunks[0]).await.unwrap();
            assert_eq!(got.as_ref(), format!("block-{local}").as_bytes());
            checksum::verify(&got, bd.chunks[0].checksum_data.as_ref().unwrap()).unwrap();
        }
    }
    std::fs::remove_dir_all(&dir).ok();
}

/// Blocks written CONCURRENTLY must all be durable across a restart.
#[tokio::test]
async fn concurrent_writes_survive_restart() {
    let dir = std::env::temp_dir().join(format!("ozone-dur-conc-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let container = ContainerId(2);
    const N: u64 = 32;

    {
        let (meta, chunks) = open(&dir);
        let mut handles = Vec::new();
        for local in 1..=N {
            let meta = meta.clone();
            let chunks = chunks.clone();
            handles.push(tokio::spawn(async move {
                put_block(&meta, &chunks, container, local, format!("c-{local}").as_bytes()).await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }
    {
        let (meta, chunks) = open(&dir);
        for local in 1..=N {
            let bslot = BlockId::ec(container, LocalId(local), ReplicaIndex::new(1));
            let bd = meta
                .get_block(&bslot)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("block {local} must survive restart"));
            let got = chunks.read_chunk(&bslot, &bd.chunks[0]).await.unwrap();
            assert_eq!(got.as_ref(), format!("c-{local}").as_bytes());
        }
    }
    std::fs::remove_dir_all(&dir).ok();
}

/// Overwriting a block then restarting must keep the LATEST bytes, with a stored
/// checksum that matches them (no stale-digest/stale-bytes mismatch).
#[tokio::test]
async fn overwrite_then_restart_keeps_latest() {
    let dir = std::env::temp_dir().join(format!("ozone-dur-ow-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let container = ContainerId(3);

    {
        let (meta, chunks) = open(&dir);
        put_block(&meta, &chunks, container, 1, b"OLD-VALUE").await;
        put_block(&meta, &chunks, container, 1, b"NEW-VALUE-that-replaces-the-old").await;
    }
    {
        let (meta, chunks) = open(&dir);
        let bslot = BlockId::ec(container, LocalId(1), ReplicaIndex::new(1));
        let bd = meta.get_block(&bslot).await.unwrap().unwrap();
        let got = chunks.read_chunk(&bslot, &bd.chunks[0]).await.unwrap();
        assert_eq!(
            got.as_ref(),
            b"NEW-VALUE-that-replaces-the-old",
            "latest write survives restart"
        );
        checksum::verify(&got, bd.chunks[0].checksum_data.as_ref().unwrap())
            .expect("latest checksum matches latest bytes after restart");
    }
    std::fs::remove_dir_all(&dir).ok();
}
