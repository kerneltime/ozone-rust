//! Track-1 end-to-end: the COMPLIANT OM client + real datanodes store and retrieve
//! an EC object. This is the Track-1 core thesis proven end to end, the analog of
//! Track 2's self-heal proof: `create_key`/`commit_key`/`get_key_info` over the REAL
//! `OzoneManagerService.submitRequest(OMRequest)` envelope hand back a block whose
//! pipeline `member_replica_indexes` place each EC shard on the right datanode, and a
//! later verified read returns the exact bytes — including a DEGRADED read with the
//! datanode holding a data shard shut down (EC reconstruction across the bridge).
//!
//! It deliberately drives the OM client + datanode data plane DIRECTLY (not through
//! the S3 HTTP backend, which is still on the bespoke OM until B3), so it isolates
//! the OM↔datanode bridge: the W3 slot mapping (`BlockLocation.members`) and the
//! commit→read round-trip.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use ozone_dn_client::DnClient;
use ozone_dn_server::DatanodeService;
use ozone_fjall_store::FjallMetaStore;
use ozone_grpc_types::hadoop::hdds;
use ozone_om_client::compliant::{BlockLocation, OzoneOmClient};
use ozone_storage::{checksum, ChunkStore, FileChunkStore, MetaStore};
use ozone_types::{
    BlockData, BlockId, ChecksumType, ChunkInfo, ContainerId, EcReplicationConfig, LocalId,
    ReplicaIndex,
};
use test_fixtures::compliant_om::{CompliantOm, CompliantOmPipeline};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

static SEQ: AtomicU64 = AtomicU64::new(0);

const K: usize = 3;
const P: usize = 5 - 3; // EC-3-2
const CHUNK: u32 = 1024;

fn ec() -> EcReplicationConfig {
    EcReplicationConfig::rs(K as u8, P as u8, CHUNK)
}

fn profile() -> ozone_ec::Profile {
    ozone_ec::Profile { data: K, parity: P, chunk_size: CHUNK as usize }
}

fn ec_hdds() -> hdds::EcReplicationConfig {
    hdds::EcReplicationConfig {
        data: K as i32,
        parity: P as i32,
        codec: "rs".to_string(),
        ec_chunk_size: CHUNK as i32,
    }
}

struct Dn {
    uuid: String,
    addr: SocketAddr,
    dir: PathBuf,
    handle: JoinHandle<()>,
}

async fn spawn_dn(idx: usize) -> Dn {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("ozone-om-e2e-{}-{n}", std::process::id()));
    let meta: Arc<dyn MetaStore> = Arc::new(FjallMetaStore::open(dir.join("meta")).unwrap());
    let chunks: Arc<dyn ChunkStore> = Arc::new(FileChunkStore::new(dir.join("data")));
    let service = DatanodeService::new(meta, chunks).into_server();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .ok();
    });
    Dn { uuid: format!("dn-{idx}"), addr, dir, handle }
}

/// A datanode-details message for the OM pipeline: the datanode's data endpoint is
/// advertised under the REPLICATION port (what the Rust datanode registers to SCM in
/// Track 2, so the OM pipeline carries it).
fn dd(uuid: &str, port: u16) -> hdds::DatanodeDetailsProto {
    hdds::DatanodeDetailsProto {
        uuid: Some(uuid.to_string()),
        ip_address: "127.0.0.1".to_string(),
        host_name: "h".to_string(),
        ports: vec![hdds::Port { name: "REPLICATION".to_string(), value: port as u32 }],
        ..Default::default()
    }
}

/// Write one EC shard to the datanode at `endpoint` (mirrors the gateway's
/// write_block_group per-slot write: create container, write chunk + checksum, put
/// block so a later verify=true read finds the digest).
async fn write_shard(endpoint: &str, container: u64, local: u64, slot: u8, shard: &[u8], group_len: u64) {
    let mut dnc = DnClient::connect(endpoint.to_string()).await.unwrap();
    let cid = ContainerId(container);
    let _ = dnc.create_container(cid, ec()).await; // idempotent
    let cd = checksum::compute(shard, CHUNK, ChecksumType::Crc32c).unwrap();
    let chunk = ChunkInfo {
        chunk_name: "0".to_string(),
        offset: 0,
        len: shard.len() as u64,
        checksum_data: Some(cd),
        stripe_checksum: None,
    };
    let bslot = BlockId::ec(cid, LocalId(local), ReplicaIndex::new(slot));
    dnc.write_chunk(&bslot, &chunk, Bytes::copy_from_slice(shard)).await.unwrap();
    let mut bd = BlockData::new(bslot);
    bd.chunks.push(chunk);
    bd.set_block_group_len(group_len);
    dnc.put_block(&bd, true).await.unwrap();
}

/// Read every available shard of `block` from its member datanodes (verify=true), EC
/// decode to the original object, and return it. A member whose datanode is down or
/// whose shard fails to verify contributes `None`; decoding succeeds while >= k
/// remain.
async fn read_and_decode(block: &BlockLocation) -> Vec<u8> {
    let total = K + P;
    let mut views: Vec<Option<Vec<u8>>> = vec![None; total];
    for m in &block.members {
        let slot = m.replica_index as usize;
        let mut dnc = match DnClient::connect(m.endpoint.clone()).await {
            Ok(c) => c,
            Err(_) => continue,
        };
        let bslot = BlockId::ec(
            ContainerId(block.container_id),
            LocalId(block.local_id),
            ReplicaIndex::new(slot as u8),
        );
        let probe = ChunkInfo {
            chunk_name: "0".to_string(),
            offset: 0,
            len: 0,
            checksum_data: None,
            stripe_checksum: None,
        };
        if let Ok(b) = dnc.read_chunk(&bslot, &probe, true).await {
            views[slot - 1] = Some(b.to_vec());
        }
    }
    let refs: Vec<Option<&[u8]>> = views.iter().map(|o| o.as_deref()).collect();
    ozone_ec::stripe::decode_object(profile(), block.length as usize, &refs).unwrap()
}

#[tokio::test]
async fn compliant_om_and_datanodes_put_get_roundtrip() {
    // 5 real datanodes; dns[i] is advertised as EC slot i+1 in the OM pipeline.
    let mut dns = Vec::new();
    for i in 0..5 {
        dns.push(spawn_dn(i).await);
    }
    let pipeline = CompliantOmPipeline {
        datanodes: dns.iter().map(|d| dd(&d.uuid, d.addr.port())).collect(),
        ec: ec_hdds(),
    };

    // Serve the compliant OM.
    let om_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let om_addr = om_listener.local_addr().unwrap();
    let om_fixture = CompliantOm::new(pipeline);
    tokio::spawn(async move {
        Server::builder()
            .add_service(om_fixture.into_server())
            .serve_with_incoming(TcpListenerStream::new(om_listener))
            .await
            .ok();
    });

    let mut om = OzoneOmClient::connect(format!("http://{om_addr}"), "client-1", "the-principal")
        .await
        .unwrap();
    om.create_bucket("s3v", "bkt").await.unwrap();

    // PUT: open the key, EC-encode, write each shard to the datanode the OM pipeline
    // names for its slot, then commit with the real length + an ETag.
    let payload: Vec<u8> = (0..2000u32).map(|i| (i * 7 + 1) as u8).collect();
    let open = om
        .create_key("s3v", "bkt", "obj", Some(ec()), payload.len() as u64)
        .await
        .unwrap();
    assert_eq!(open.blocks.len(), 1, "fixture pre-allocates one block");
    let block = open.blocks[0].clone();
    assert_eq!(block.members.len(), 5, "the block names all k+p datanodes");

    let shards = ozone_ec::stripe::encode_object(profile(), &payload).unwrap();
    for m in &block.members {
        let slot = m.replica_index as usize;
        let shard: &[u8] = if slot <= K {
            &shards.data[slot - 1]
        } else {
            &shards.parity[slot - 1 - K]
        };
        write_shard(&m.endpoint, block.container_id, block.local_id, slot as u8, shard, payload.len() as u64).await;
    }

    let committed = BlockLocation { length: payload.len() as u64, ..block.clone() };
    om.commit_key("s3v", "bkt", "obj", open.client_id, &[committed], payload.len() as u64, Some("etag-abc"), &[])
        .await
        .unwrap();

    // GET: resolve the committed layout and read the object back, byte-identical.
    let meta = om.get_key_info("s3v", "bkt", "obj").await.unwrap();
    assert_eq!(meta.size, payload.len() as u64);
    assert_eq!(meta.etag.as_deref(), Some("etag-abc"));
    assert_eq!(meta.blocks.len(), 1);
    let got = read_and_decode(&meta.blocks[0]).await;
    assert_eq!(got, payload, "object read back byte-identical through the compliant OM");

    // DEGRADED GET: shut down the datanode holding EC slot 1 (a DATA shard). The read
    // gathers the surviving k+ shards and reconstructs -- EC across the bridge.
    let slot1_endpoint = meta.blocks[0]
        .members
        .iter()
        .find(|m| m.replica_index == 1)
        .map(|m| m.endpoint.clone())
        .unwrap();
    let slot1_dn = dns
        .iter()
        .find(|d| slot1_endpoint.ends_with(&d.addr.port().to_string()))
        .unwrap();
    slot1_dn.handle.abort();
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let degraded = read_and_decode(&meta.blocks[0]).await;
    assert_eq!(degraded, payload, "degraded read reconstructs the object with slot 1's datanode down");

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}
