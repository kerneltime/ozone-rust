//! End-to-end data-path test: a real `DatanodeService` (fjall metadata +
//! filesystem chunk store) served over an ephemeral TCP port, exercised through
//! the domain-typed [`DnClient`]. This is the proof that the wire protocol,
//! conversions, streaming, and storage compose into a working datanode.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ozone_dn_client::DnClient;
use ozone_dn_server::DatanodeService;
use ozone_fjall_store::FjallMetaStore;
use ozone_storage::{checksum, FileChunkStore};
use ozone_types::{
    BlockData, BlockId, ChecksumData, ChecksumType, ChunkInfo, ContainerId, ContainerState,
    EcReplicationConfig, LocalId, ReplicaIndex,
};
use tokio_stream::wrappers::TcpListenerStream;

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch() -> PathBuf {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("ozone-dn-e2e-{}-{}", std::process::id(), n))
}

/// Spawn a datanode server bound to an ephemeral port; return its endpoint URL
/// and the scratch dir to clean up.
async fn spawn_datanode() -> (String, PathBuf) {
    let root = scratch();
    let meta = Arc::new(FjallMetaStore::open(root.join("meta")).unwrap());
    let chunks = Arc::new(FileChunkStore::new(root.join("data")));
    let service = DatanodeService::new(meta, chunks).into_server();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (format!("http://{addr}"), root)
}

/// Connect with a short retry while the spawned server finishes binding.
async fn connect(endpoint: &str) -> DnClient {
    for _ in 0..50 {
        if let Ok(c) = DnClient::connect(endpoint.to_string()).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("datanode did not come up at {endpoint}");
}

fn ci(name: &str, len: u64, checksum_data: Option<ChecksumData>) -> ChunkInfo {
    ChunkInfo {
        chunk_name: name.to_string(),
        offset: 0,
        len,
        checksum_data,
        stripe_checksum: None,
    }
}

#[tokio::test]
async fn data_path_round_trip() {
    let (endpoint, root) = spawn_datanode().await;
    let mut client = connect(&endpoint).await;

    // Version handshake.
    let (maj, _min, _build) = client.get_version().await.unwrap();
    assert_eq!(maj, 1);

    // Create an EC container.
    let cid = ContainerId(1);
    let state = client
        .create_container(cid, EcReplicationConfig::RS_6_3_1MIB)
        .await
        .unwrap();
    assert_eq!(state, ContainerState::Open);

    // Write a chunk (replica slot 1 = first data shard) and read it back.
    let block = BlockId::ec(cid, LocalId(1), ReplicaIndex::new(1));
    let payload = Bytes::from_static(b"hello erasure coded world");
    let chunk = ci("c0", payload.len() as u64, None);
    let n = client
        .write_chunk(&block, &chunk, payload.clone())
        .await
        .unwrap();
    assert_eq!(n, payload.len() as u64);
    let got = client.read_chunk(&block, &chunk, false).await.unwrap();
    assert_eq!(got, payload);

    // Commit block metadata, then read it back.
    let mut bd = BlockData::new(block);
    bd.chunks.push(chunk.clone());
    bd.set_block_group_len(payload.len() as u64);
    let bcsi = client.put_block(&bd, true).await.unwrap();
    assert_eq!(bcsi, 1);
    let fetched = client.get_block(&block).await.unwrap().unwrap();
    assert_eq!(fetched.chunks.len(), 1);
    assert_eq!(fetched.block_group_len(), Some(payload.len() as u64));

    // List and container info.
    let blocks = client.list_blocks(cid, 0, 10).await.unwrap();
    assert_eq!(blocks.len(), 1);
    let info = client.get_container_info(cid).await.unwrap().unwrap();
    assert_eq!(info.block_count, 1);
    assert_eq!(info.used_bytes, payload.len() as u64);

    // Absent lookups fold to None.
    assert!(client
        .get_block(&BlockId::ec(cid, LocalId(99), ReplicaIndex::new(1)))
        .await
        .unwrap()
        .is_none());
    assert!(client
        .get_container_info(ContainerId(404))
        .await
        .unwrap()
        .is_none());

    tokio::fs::remove_dir_all(&root).await.ok();
}

#[tokio::test]
async fn checksum_verified_read_and_ingress_rejection() {
    let (endpoint, root) = spawn_datanode().await;
    let mut client = connect(&endpoint).await;
    let cid = ContainerId(2);
    client
        .create_container(cid, EcReplicationConfig::RS_3_2_1MIB)
        .await
        .unwrap();

    let block = BlockId::ec(cid, LocalId(1), ReplicaIndex::new(1));
    let payload = Bytes::from((0..4096u32).map(|i| i as u8).collect::<Vec<u8>>());

    // Correct CRC32C: write succeeds, verified read returns the bytes.
    let good = checksum::compute(&payload, 1024, ChecksumType::Crc32c).unwrap();
    let chunk = ci("c0", payload.len() as u64, Some(good));
    client
        .write_chunk(&block, &chunk, payload.clone())
        .await
        .unwrap();
    let got = client.read_chunk(&block, &chunk, true).await.unwrap();
    assert_eq!(got, payload);

    // Wrong checksum for the data: the datanode rejects at ingress (DataLoss).
    let mut bogus = checksum::compute(&payload, 1024, ChecksumType::Crc32c).unwrap();
    bogus.checksums[0] = vec![0, 0, 0, 0];
    let bad_chunk = ci("c1", payload.len() as u64, Some(bogus));
    let err = client
        .write_chunk(&block, &bad_chunk, payload.clone())
        .await
        .unwrap_err();
    match err {
        ozone_dn_client::DnClientError::Rpc(s) => {
            assert_eq!(s.code(), tonic::Code::DataLoss)
        }
        other => panic!("expected DataLoss rpc error, got {other:?}"),
    }

    tokio::fs::remove_dir_all(&root).await.ok();
}
