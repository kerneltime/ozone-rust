//! EC repair-at-rest, end to end through real datanodes.
//!
//! Seeds an EC-3-2 block group (with a partial trailing stripe) across 5 real
//! `DatanodeService` instances, corrupts one shard ON DISK, drives repair via an
//! SCM `ReconstructEC` command, and proves the shard is healed AT REST: a plain
//! per-slot `read_chunk(verify=true)` of the target succeeds afterwards — the
//! datanode read path never calls the EC decoder, so a clean verified read means
//! the bytes on disk are correct, not reconstructed on the fly.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ozone_dn_client::DnClient;
use ozone_dn_server::scrub::Scrubber;
use ozone_dn_server::{repair, CompliantScmRegistration, DatanodeService};
use ozone_ec::stripe::{encode_object, EncodedShards};
use ozone_ec::Profile;
use ozone_fjall_store::FjallMetaStore;
use ozone_grpc_types::hadoop::hdds as oz;
use ozone_storage::{checksum, ChunkStore, FileChunkStore, MetaStore, StorageError};
use ozone_types::{
    BlockData, BlockId, ChecksumType, ChunkInfo, ContainerId, ContainerInfo, ContainerState,
    EcReplicationConfig, LocalId, ReplicaIndex,
};
use test_fixtures::compliant_scm::{CompliantPipelineFixture, CompliantScm};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

static SEQ: AtomicU64 = AtomicU64::new(0);

const CONTAINER: u64 = 1;
const LOCAL: u64 = 1;
const PAYLOAD_LEN: usize = 3 * 8 + 5; // EC-3-2, 8-byte cells: 2 full data cells + a partial

fn profile() -> Profile {
    Profile { data: 3, parity: 2, chunk_size: 8 }
}

fn ec() -> EcReplicationConfig {
    EcReplicationConfig::rs(3, 2, 8)
}

struct Dn {
    uuid: String,
    addr: SocketAddr,
    meta: Arc<dyn MetaStore>,
    chunks: Arc<dyn ChunkStore>,
    dir: PathBuf,
    handle: JoinHandle<()>,
}

async fn spawn_dn(idx: usize) -> Dn {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("ozone-ec-repair-{}-{n}", std::process::id()));
    let meta: Arc<dyn MetaStore> = Arc::new(FjallMetaStore::open(dir.join("meta")).unwrap());
    let chunks: Arc<dyn ChunkStore> = Arc::new(FileChunkStore::new(dir.join("data")));
    let service = DatanodeService::new(meta.clone(), chunks.clone()).into_server();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .ok();
    });
    Dn {
        uuid: format!("dn-{idx}"),
        addr,
        meta,
        chunks,
        dir,
        handle,
    }
}

fn shard_bytes(shards: &EncodedShards, slot: u8) -> Vec<u8> {
    let s = slot as usize;
    if s <= 3 {
        shards.data[s - 1].clone()
    } else {
        shards.parity[s - 1 - 3].clone()
    }
}

/// Write shard `slot` of block group `local` into datanode `dn`'s stores (mirrors
/// the gateway's write_block_group: create container, write chunk + checksum, put
/// block).
async fn seed_shard_at(dn: &Dn, local: u64, slot: u8, shard: &[u8]) {
    let _ = dn
        .meta
        .create_container(ContainerInfo::new_open(ContainerId(CONTAINER), ec()))
        .await;
    let cd = checksum::compute(shard, 8, ChecksumType::Crc32c).unwrap();
    let chunk = ChunkInfo {
        chunk_name: "0".to_string(),
        offset: 0,
        len: shard.len() as u64,
        checksum_data: Some(cd),
        stripe_checksum: None,
    };
    let bslot = BlockId::ec(ContainerId(CONTAINER), LocalId(local), ReplicaIndex::new(slot));
    dn.chunks
        .write_chunk(&bslot, &chunk, Bytes::copy_from_slice(shard))
        .await
        .unwrap();
    let mut bd = BlockData::new(bslot);
    bd.chunks.push(chunk);
    bd.set_block_group_len(PAYLOAD_LEN as u64);
    dn.meta.put_block(&bd).await.unwrap();
}

async fn seed_shard(dn: &Dn, slot: u8, shard: &[u8]) {
    seed_shard_at(dn, LOCAL, slot, shard).await
}

/// Encode `payload` and seed each of the 5 shards (one per datanode) for block
/// group `local`. Returns the original shard bytes indexed by slot-1.
async fn seed_block_group(dns: &[Dn], local: u64, payload: &[u8]) -> Vec<Vec<u8>> {
    let shards = encode_object(profile(), payload).unwrap();
    for slot in 1..=5u8 {
        seed_shard_at(&dns[(slot - 1) as usize], local, slot, &shard_bytes(&shards, slot)).await;
    }
    (1..=5u8).map(|s| shard_bytes(&shards, s)).collect()
}

/// Corrupt one byte of (block group `local`, slot) on `dn`'s disk, leaving the
/// stored checksum intact so a verified read detects the rot.
fn corrupt_chunk(dn: &Dn, local: u64, slot: u8) {
    let p = dn
        .dir
        .join("data")
        .join(CONTAINER.to_string())
        .join("chunks")
        .join(format!("{local}_{slot}_0"));
    let mut bytes = std::fs::read(&p).unwrap();
    bytes[0] ^= 0xFF;
    std::fs::write(&p, &bytes).unwrap();
}

/// A read-chunk request for (block group `local`, slot) with no caller checksum,
/// so the datanode verifies against its OWN stored checksum (verify=true).
fn verify_read_at(local: u64, slot: u8) -> (BlockId, ChunkInfo) {
    (
        BlockId::ec(ContainerId(CONTAINER), LocalId(local), ReplicaIndex::new(slot)),
        ChunkInfo {
            chunk_name: "0".to_string(),
            offset: 0,
            len: 0,
            checksum_data: None,
            stripe_checksum: None,
        },
    )
}

fn verify_read(slot: u8) -> (BlockId, ChunkInfo) {
    verify_read_at(LOCAL, slot)
}

#[tokio::test]
async fn scrub_then_repair_heals_shard_at_rest() {
    let mut dns = Vec::new();
    for i in 0..5 {
        dns.push(spawn_dn(i).await);
    }

    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 5 + 9) as u8).collect();
    let shards = encode_object(profile(), &payload).unwrap();
    for slot in 1..=5u8 {
        seed_shard(&dns[(slot - 1) as usize], slot, &shard_bytes(&shards, slot)).await;
    }
    let original_slot1 = shard_bytes(&shards, 1);

    // Corrupt slot 1 on disk (dns[0]).
    let chunk_path = dns[0]
        .dir
        .join("data")
        .join(CONTAINER.to_string())
        .join("chunks")
        .join(format!("{LOCAL}_1_0"));
    let mut bytes = std::fs::read(&chunk_path).unwrap();
    bytes[0] ^= 0xFF;
    std::fs::write(&chunk_path, &bytes).unwrap();

    // The SCRUBBER detects the rot locally (no peers), scanning dns[0]'s own store.
    let scrubber = Scrubber::new(dns[0].meta.clone(), dns[0].chunks.clone());
    let report = scrubber.scrub_once().await.unwrap();
    assert_eq!(report.clean, 0, "the only local shard is corrupt");
    assert_eq!(report.corrupt.len(), 1, "scrubber must find one corrupt shard");
    let req = &report.corrupt[0];
    assert_eq!(req.slot, 1);
    assert_eq!(req.container, ContainerId(CONTAINER));
    assert_eq!(req.local, LocalId(LOCAL));
    assert_eq!(req.block_group_len, PAYLOAD_LEN as u64);

    // Repair via the shared primitive, with the peer sources the SCM/coordinator
    // would supply (slots 2..5 on dns[1..5]).
    let sources: Vec<(u8, String)> = (1..5)
        .map(|i| ((i + 1) as u8, format!("http://{}", dns[i].addr)))
        .collect();
    let input = repair::RepairInput {
        container: req.container,
        local: req.local,
        ec: req.ec,
        block_group_len: req.block_group_len,
        missing_slots: vec![req.slot],
        sources,
    };
    let repaired = repair::reconstruct_and_persist(&dns[0].meta, &dns[0].chunks, input)
        .await
        .expect("repair must succeed");
    assert_eq!(repaired, vec![1]);

    // PROOF of at-rest heal: a verified read of slot 1 on dns[0] succeeds AND keeps
    // succeeding after every peer is shut down — the healed bytes are on dns[0]'s
    // disk, never reconstructed from peers on the read path.
    for d in &dns[1..5] {
        d.handle.abort();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut target = DnClient::connect(format!("http://{}", dns[0].addr)).await.unwrap();
    let (b, c) = verify_read(1);
    let healed = target
        .read_chunk(&b, &c, true)
        .await
        .expect("healed shard must read clean from disk alone, with all peers down");
    assert_eq!(healed.as_ref(), &original_slot1[..], "repaired shard is byte-identical");

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

async fn poll_verified_read(target: &mut DnClient, local: u64, slot: u8, tries: usize) -> Option<Vec<u8>> {
    for _ in 0..tries {
        let (b, c) = verify_read_at(local, slot);
        if let Ok(bytes) = target.read_chunk(&b, &c, true).await {
            return Some(bytes.to_vec());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    None
}

// ---- compliant self-heal harness (real Apache Ozone wire protocol) ----

/// The compliant survivor pipeline: all 5 datanodes, each carrying its EC slot inline
/// and a REPLICATION port, plus the EC config -- the cluster knowledge a real SCM
/// supplies in a ReconstructEC. `handle_reconstruct` excludes the slot(s) being
/// rebuilt, so this is correct for any target slot.
fn compliant_pipeline_for(dns: &[Dn]) -> CompliantPipelineFixture {
    CompliantPipelineFixture {
        sources: (0..5)
            .map(|i| oz::DatanodeDetailsAndReplicaIndexProto {
                datanode_details: oz::DatanodeDetailsProto {
                    uuid: Some(dns[i].uuid.clone()),
                    ip_address: "127.0.0.1".to_string(),
                    host_name: "h".to_string(),
                    ports: vec![oz::Port {
                        name: "REPLICATION".to_string(),
                        value: dns[i].addr.port() as u32,
                    }],
                    ..Default::default()
                },
                replica_index: (i + 1) as i32,
            })
            .collect(),
        ec_config: oz::EcReplicationConfig {
            data: 3,
            parity: 2,
            codec: "rs".to_string(),
            ec_chunk_size: 8,
        },
    }
}

/// Serve `scm` (the compliant fake) and return its endpoint.
async fn serve_compliant_scm(scm: CompliantScm) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(scm.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .ok();
    });
    format!("http://{addr}")
}

/// Run a scrubber (short interval) + the COMPLIANT SCM loop on `dn`, wired via the
/// repair channel, against the SCM at `endpoint`. No command is injected -- repair is
/// driven entirely by the closed loop.
fn spawn_compliant_heal_stack(dn: &Dn, endpoint: String) {
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    let scrubber = Scrubber::new(dn.meta.clone(), dn.chunks.clone());
    tokio::spawn(async move {
        scrubber.run(Duration::from_millis(25), tx).await;
    });
    let reg = CompliantScmRegistration {
        uuid: dn.uuid.clone(),
        ip_address: "127.0.0.1".to_string(),
        host_name: "h".to_string(),
        data_port: dn.addr.port() as u32,
        meta: dn.meta.clone(),
        chunks: dn.chunks.clone(),
        heartbeat_interval: Duration::from_millis(50),
        repairs: Some(rx),
    };
    tokio::spawn(async move {
        reg.run(endpoint).await.ok();
    });
}

#[tokio::test]
async fn concurrent_repairs_of_same_shard_are_idempotent() {
    let mut dns = Vec::new();
    for i in 0..5 {
        dns.push(spawn_dn(i).await);
    }
    let originals = seed_block_group(&dns, LOCAL, &(0..PAYLOAD_LEN).map(|i| (i * 3 + 1) as u8).collect::<Vec<_>>()).await;
    corrupt_chunk(&dns[0], LOCAL, 1);

    let sources: Vec<(u8, String)> = (1..5)
        .map(|i| ((i + 1) as u8, format!("http://{}", dns[i].addr)))
        .collect();
    let mk_input = || repair::RepairInput {
        container: ContainerId(CONTAINER),
        local: LocalId(LOCAL),
        ec: ec(),
        block_group_len: PAYLOAD_LEN as u64,
        missing_slots: vec![1],
        sources: sources.clone(),
    };
    // Two repairs of the SAME shard, concurrently. decode->re-encode is
    // deterministic and write_chunk is atomic (create-then-rename to a per-call
    // temp), so the final on-disk shard is exactly one complete, correct write --
    // never an interleaved/torn file.
    let (r1, r2) = tokio::join!(
        repair::reconstruct_and_persist(&dns[0].meta, &dns[0].chunks, mk_input()),
        repair::reconstruct_and_persist(&dns[0].meta, &dns[0].chunks, mk_input()),
    );
    assert!(r1.is_ok() && r2.is_ok(), "both concurrent repairs succeed: {r1:?} {r2:?}");

    let mut t0 = DnClient::connect(format!("http://{}", dns[0].addr)).await.unwrap();
    let (b, c) = verify_read(1);
    let healed = t0
        .read_chunk(&b, &c, true)
        .await
        .expect("shard verifies clean after two concurrent repairs");
    assert_eq!(healed.as_ref(), &originals[0][..], "no torn/interleaved write");

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

#[tokio::test]
async fn repair_does_not_resurrect_a_deleted_container() {
    let mut dns = Vec::new();
    for i in 0..5 {
        dns.push(spawn_dn(i).await);
    }
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 3 + 1) as u8).collect();
    seed_block_group(&dns, LOCAL, &payload).await;
    corrupt_chunk(&dns[0], LOCAL, 1);
    // Delete the container on dns[0] (meta + chunks), as a DeleteContainer would.
    dns[0].meta.delete_container(ContainerId(CONTAINER)).await.unwrap();
    dns[0].chunks.delete_container(ContainerId(CONTAINER)).await.ok();

    let sources: Vec<(u8, String)> = (1..5)
        .map(|i| ((i + 1) as u8, format!("http://{}", dns[i].addr)))
        .collect();
    let input = repair::RepairInput {
        container: ContainerId(CONTAINER),
        local: LocalId(LOCAL),
        ec: ec(),
        block_group_len: PAYLOAD_LEN as u64,
        missing_slots: vec![1],
        sources,
    };
    let r = repair::reconstruct_and_persist(&dns[0].meta, &dns[0].chunks, input).await;
    assert!(r.is_err(), "repair must refuse a deleted container, got {r:?}");
    assert!(
        dns[0].meta.get_container(ContainerId(CONTAINER)).await.unwrap().is_none(),
        "repair must NOT recreate a deleted container"
    );
    let bslot = BlockId::ec(ContainerId(CONTAINER), LocalId(LOCAL), ReplicaIndex::new(1));
    assert!(
        dns[0].meta.get_block(&bslot).await.unwrap().is_none(),
        "repair must write no block metadata into a deleted container"
    );

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

#[tokio::test]
async fn repair_refuses_a_non_open_container_without_orphan_write() {
    let mut dns = Vec::new();
    for i in 0..5 {
        dns.push(spawn_dn(i).await);
    }
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 3 + 1) as u8).collect();
    seed_block_group(&dns, LOCAL, &payload).await;
    corrupt_chunk(&dns[0], LOCAL, 1);
    // Close the container: it must no longer accept writes (incl. repair writes).
    dns[0]
        .meta
        .set_container_state(ContainerId(CONTAINER), ContainerState::Closed)
        .await
        .unwrap();

    let sources: Vec<(u8, String)> = (1..5)
        .map(|i| ((i + 1) as u8, format!("http://{}", dns[i].addr)))
        .collect();
    let input = repair::RepairInput {
        container: ContainerId(CONTAINER),
        local: LocalId(LOCAL),
        ec: ec(),
        block_group_len: PAYLOAD_LEN as u64,
        missing_slots: vec![1],
        sources,
    };
    let r = repair::reconstruct_and_persist(&dns[0].meta, &dns[0].chunks, input).await;
    assert!(r.is_err(), "repair must refuse a closed container, got {r:?}");
    // Repair wrote nothing -> the slot stays corrupt (no orphan/partial write).
    let mut target = DnClient::connect(format!("http://{}", dns[0].addr)).await.unwrap();
    let (b, c) = verify_read(1);
    assert!(
        target.read_chunk(&b, &c, true).await.is_err(),
        "a closed container must not be written by repair; slot stays corrupt"
    );

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

// ---- B4: compliant survivor-enumeration reconstruction ----

#[tokio::test]
async fn reconstruct_wholly_lost_replica_from_survivors() {
    // 6 datanodes; dns[0..5] hold EC slots 1..5, dns[5] is a FRESH target with no
    // local data, rebuilding slot 1 from the survivors (slots 2..5) -- the case the
    // in-place self-heal code literally cannot do.
    let mut dns = Vec::new();
    for i in 0..6 {
        dns.push(spawn_dn(i).await);
    }
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 3 + 1) as u8).collect();
    let originals = seed_block_group(&dns, LOCAL, &payload).await;

    let sources: Vec<(u8, String)> = (1..5)
        .map(|i| ((i + 1) as u8, format!("http://{}", dns[i].addr)))
        .collect(); // slots 2..5
    let input = repair::ReconstructInput {
        container: ContainerId(CONTAINER),
        ec: ec(),
        missing_slots: vec![1],
        sources,
    };
    let rebuilt = repair::reconstruct_from_survivors(&dns[5].meta, &dns[5].chunks, input)
        .await
        .expect("reconstruct from survivors");
    assert_eq!(rebuilt, vec![LOCAL]);

    // The fresh target now holds slot 1, byte-identical, and verifies clean.
    let mut t = DnClient::connect(format!("http://{}", dns[5].addr)).await.unwrap();
    let (b, c) = verify_read(1);
    let got = t
        .read_chunk(&b, &c, true)
        .await
        .expect("rebuilt slot reads clean on the fresh target");
    assert_eq!(
        got.as_ref(),
        &originals[0][..],
        "wholly-lost replica rebuilt byte-identical from survivors"
    );

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

#[tokio::test]
async fn reconstruct_uses_min_block_group_len() {
    // One survivor over-claims a LARGER block-group length (as if a torn trailing
    // write recorded a wrong length). Reconstruction MUST use the MIN across
    // survivors (the correct length), not the inflated value -- else the trailing
    // partial stripe is silently corrupted.
    let mut dns = Vec::new();
    for i in 0..6 {
        dns.push(spawn_dn(i).await);
    }
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 5 + 2) as u8).collect();
    let originals = seed_block_group(&dns, LOCAL, &payload).await;

    // Inflate the recorded length on slot 2's survivor (dns[1]).
    {
        let bslot = BlockId::ec(ContainerId(CONTAINER), LocalId(LOCAL), ReplicaIndex::new(2));
        let mut bd = dns[1].meta.get_block(&bslot).await.unwrap().unwrap();
        bd.set_block_group_len(PAYLOAD_LEN as u64 + 100);
        dns[1].meta.put_block(&bd).await.unwrap();
    }

    let sources: Vec<(u8, String)> = (1..5)
        .map(|i| ((i + 1) as u8, format!("http://{}", dns[i].addr)))
        .collect();
    let input = repair::ReconstructInput {
        container: ContainerId(CONTAINER),
        ec: ec(),
        missing_slots: vec![1],
        sources,
    };
    repair::reconstruct_from_survivors(&dns[5].meta, &dns[5].chunks, input)
        .await
        .expect("reconstruct");

    let mut t = DnClient::connect(format!("http://{}", dns[5].addr)).await.unwrap();
    let (b, c) = verify_read(1);
    let got = t.read_chunk(&b, &c, true).await.expect("rebuilt at min length reads clean");
    assert_eq!(
        got.as_ref(),
        &originals[0][..],
        "rebuilt using min(blockGroupLen) is byte-identical to the original slot 1"
    );

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

/// END-TO-END through the COMPLIANT command path: a real ReconstructECContainers
/// command (byte-per-index missingContainerIndexes, positional targets, sources
/// with inline replica_index + REPLICATION port) drives handle_reconstruct ->
/// reconstruct_from_survivors on a FRESH target. Closes the verification's coverage
/// gap on the safety-critical command-INTERPRETATION wiring (the algorithm is tested
/// directly elsewhere; this proves the dispatch decodes the wire correctly).
#[tokio::test]
async fn compliant_reconstruct_command_rebuilds_target_slot() {
    let mut dns = Vec::new();
    for i in 0..6 {
        dns.push(spawn_dn(i).await);
    }
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 3 + 1) as u8).collect();
    let originals = seed_block_group(&dns, LOCAL, &payload).await; // slots 1..5 on dns[0..4]

    fn oz_dd(uuid: &str, port: u16) -> oz::DatanodeDetailsProto {
        oz::DatanodeDetailsProto {
            uuid: Some(uuid.to_string()),
            ip_address: "127.0.0.1".to_string(),
            host_name: "h".to_string(),
            ports: vec![oz::Port {
                name: "REPLICATION".to_string(),
                value: port as u32,
            }],
            ..Default::default()
        }
    }
    // Survivors slots 2..5 on dns[1..4], with their REPLICATION (data) ports.
    let sources: Vec<oz::DatanodeDetailsAndReplicaIndexProto> = (1..5)
        .map(|i| oz::DatanodeDetailsAndReplicaIndexProto {
            datanode_details: oz_dd(&format!("dn-{i}"), dns[i].addr.port()),
            replica_index: (i + 1) as i32,
        })
        .collect();
    let cmd = oz::ScmCommandProto {
        command_type: oz::scm_command_proto::Type::ReconstructEcContainersCommand as i32,
        reconstruct_ec_containers_command_proto: Some(oz::ReconstructEcContainersCommandProto {
            container_id: CONTAINER as i64,
            sources,
            targets: vec![oz_dd("dn-5", dns[5].addr.port())],
            missing_container_indexes: vec![1u8], // rebuild slot 1 on dns[5]
            ec_replication_config: oz::EcReplicationConfig {
                data: 3,
                parity: 2,
                codec: "rs".to_string(),
                ec_chunk_size: 8,
            },
            cmd_id: 1,
        }),
        ..Default::default()
    };

    let scm = CompliantScm::with_commands(vec![cmd]);
    let scm_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let scm_addr = scm_listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(scm.into_server())
            .serve_with_incoming(TcpListenerStream::new(scm_listener))
            .await
            .ok();
    });
    let reg = CompliantScmRegistration {
        uuid: "dn-5".to_string(),
        ip_address: "127.0.0.1".to_string(),
        host_name: "h".to_string(),
        data_port: dns[5].addr.port() as u32,
        meta: dns[5].meta.clone(),
        chunks: dns[5].chunks.clone(),
        heartbeat_interval: Duration::from_millis(50),
        repairs: None,
    };
    tokio::spawn(async move {
        reg.run(format!("http://{scm_addr}")).await.ok();
    });

    let mut t = DnClient::connect(format!("http://{}", dns[5].addr)).await.unwrap();
    let mut healed = None;
    for _ in 0..150 {
        let (b, c) = verify_read(1);
        if let Ok(bytes) = t.read_chunk(&b, &c, true).await {
            healed = Some(bytes);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let healed =
        healed.expect("a compliant ReconstructEC command must drive handle_reconstruct to rebuild slot 1");
    assert_eq!(
        healed.as_ref(),
        &originals[0][..],
        "the compliant command path rebuilt slot 1 byte-identical on the fresh target"
    );

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

// ---- B5: RECOVERING-then-CLOSED lifecycle + group-atomic rollback ----

/// A [`ChunkStore`] whose `write_chunk` always fails, to deterministically abort a
/// rebuild AFTER the target container has been provisioned. Reads/deletes delegate
/// to a real inner store so survivor enumeration and rollback cleanup still work.
struct FailWritesChunkStore {
    inner: FileChunkStore,
}

#[async_trait::async_trait]
impl ChunkStore for FailWritesChunkStore {
    async fn write_chunk(
        &self,
        _b: &BlockId,
        _c: &ChunkInfo,
        _d: Bytes,
    ) -> Result<(), StorageError> {
        Err(StorageError::Meta("injected write failure".to_string()))
    }
    async fn read_chunk(&self, b: &BlockId, c: &ChunkInfo) -> Result<Bytes, StorageError> {
        self.inner.read_chunk(b, c).await
    }
    async fn delete_chunk(&self, b: &BlockId, c: &ChunkInfo) -> Result<(), StorageError> {
        self.inner.delete_chunk(b, c).await
    }
    async fn delete_container(&self, container: ContainerId) -> Result<(), StorageError> {
        self.inner.delete_container(container).await
    }
}

#[tokio::test]
async fn reconstruct_closes_fresh_target_and_noops_on_redelivery() {
    // dns[0..4] hold slots 1..5; dns[5] is a fresh target rebuilding slot 1.
    let mut dns = Vec::new();
    for i in 0..6 {
        dns.push(spawn_dn(i).await);
    }
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 3 + 1) as u8).collect();
    let originals = seed_block_group(&dns, LOCAL, &payload).await;

    let sources: Vec<(u8, String)> = (1..5)
        .map(|i| ((i + 1) as u8, format!("http://{}", dns[i].addr)))
        .collect();
    let mk = || repair::ReconstructInput {
        container: ContainerId(CONTAINER),
        ec: ec(),
        missing_slots: vec![1],
        sources: sources.clone(),
    };

    // First delivery: the fresh target rebuilds slot 1, then CLOSES the container
    // (RECOVERING -> CLOSED) -- a complete replica and a valid future EC source.
    let r1 = repair::reconstruct_from_survivors(&dns[5].meta, &dns[5].chunks, mk())
        .await
        .expect("first reconstruct succeeds");
    assert_eq!(r1, vec![LOCAL]);
    let ci = dns[5]
        .meta
        .get_container(ContainerId(CONTAINER))
        .await
        .unwrap()
        .expect("target container present after rebuild");
    assert_eq!(
        ci.state,
        ContainerState::Closed,
        "a rebuilt whole replica is completed by closing it"
    );

    // The rebuilt slot reads clean even though the container is CLOSED (reads do not
    // depend on container state).
    let mut t = DnClient::connect(format!("http://{}", dns[5].addr)).await.unwrap();
    let (b, c) = verify_read(1);
    let got = t
        .read_chunk(&b, &c, true)
        .await
        .expect("rebuilt slot reads clean on the closed target");
    assert_eq!(got.as_ref(), &originals[0][..]);

    // Re-delivery of the SAME command is a clean NO-OP (CLOSED target): never an
    // error and never a re-write.
    let r2 = repair::reconstruct_from_survivors(&dns[5].meta, &dns[5].chunks, mk())
        .await
        .expect("re-delivery to a CLOSED target is a clean no-op");
    assert!(r2.is_empty(), "re-delivery to a CLOSED target rebuilds nothing");
    let ci2 = dns[5]
        .meta
        .get_container(ContainerId(CONTAINER))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ci2.state, ContainerState::Closed, "re-delivery leaves it CLOSED");

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

#[tokio::test]
async fn reconstruct_rolls_back_created_container_on_failure() {
    // Survivors slots 1..5 on dns[0..4] (real servers).
    let mut dns = Vec::new();
    for i in 0..5 {
        dns.push(spawn_dn(i).await);
    }
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 3 + 1) as u8).collect();
    seed_block_group(&dns, LOCAL, &payload).await;

    // Fresh target whose chunk store FAILS every write: the rebuild aborts AFTER the
    // container is created, exercising the group-atomic rollback.
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let tdir = std::env::temp_dir().join(format!("ozone-ec-rollback-{}-{n}", std::process::id()));
    let tmeta: Arc<dyn MetaStore> = Arc::new(FjallMetaStore::open(tdir.join("meta")).unwrap());
    let tchunks: Arc<dyn ChunkStore> = Arc::new(FailWritesChunkStore {
        inner: FileChunkStore::new(tdir.join("data")),
    });

    let sources: Vec<(u8, String)> = (1..5)
        .map(|i| ((i + 1) as u8, format!("http://{}", dns[i].addr)))
        .collect();
    let input = repair::ReconstructInput {
        container: ContainerId(CONTAINER),
        ec: ec(),
        missing_slots: vec![1],
        sources,
    };
    let r = repair::reconstruct_from_survivors(&tmeta, &tchunks, input).await;
    assert!(r.is_err(), "a write failure mid-rebuild must surface as an error, got {r:?}");
    assert!(
        tmeta.get_container(ContainerId(CONTAINER)).await.unwrap().is_none(),
        "a container created for the rebuild must be rolled back (deleted) on failure -- never left half-built"
    );
    let bslot = BlockId::ec(ContainerId(CONTAINER), LocalId(LOCAL), ReplicaIndex::new(1));
    assert!(
        tmeta.get_block(&bslot).await.unwrap().is_none(),
        "rollback leaves no orphan block metadata"
    );

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
    tokio::fs::remove_dir_all(&tdir).await.ok();
}

#[tokio::test]
async fn reconstruct_keeps_preexisting_container_on_failure() {
    // Survivors slots 1..5 on dns[0..4].
    let mut dns = Vec::new();
    for i in 0..5 {
        dns.push(spawn_dn(i).await);
    }
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 3 + 1) as u8).collect();
    seed_block_group(&dns, LOCAL, &payload).await;

    // Target with a PRE-EXISTING Open container (the in-place heal path) and a chunk
    // store that fails writes. The rebuild fails -- but a container we did NOT create
    // must SURVIVE: deleting a live replica on a transient error would be data loss.
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let tdir =
        std::env::temp_dir().join(format!("ozone-ec-keepexisting-{}-{n}", std::process::id()));
    let tmeta: Arc<dyn MetaStore> = Arc::new(FjallMetaStore::open(tdir.join("meta")).unwrap());
    let tchunks: Arc<dyn ChunkStore> = Arc::new(FailWritesChunkStore {
        inner: FileChunkStore::new(tdir.join("data")),
    });
    tmeta
        .create_container(ContainerInfo::new_open(ContainerId(CONTAINER), ec()))
        .await
        .unwrap();

    let sources: Vec<(u8, String)> = (1..5)
        .map(|i| ((i + 1) as u8, format!("http://{}", dns[i].addr)))
        .collect();
    let input = repair::ReconstructInput {
        container: ContainerId(CONTAINER),
        ec: ec(),
        missing_slots: vec![1],
        sources,
    };
    let r = repair::reconstruct_from_survivors(&tmeta, &tchunks, input).await;
    assert!(r.is_err(), "the write failure must surface, got {r:?}");
    let ci = tmeta
        .get_container(ContainerId(CONTAINER))
        .await
        .unwrap()
        .expect("a pre-existing container must survive a rebuild failure -- never deleted");
    assert_eq!(
        ci.state,
        ContainerState::Open,
        "the pre-existing container is left Open (untouched), not closed or deleted"
    );

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
    tokio::fs::remove_dir_all(&tdir).await.ok();
}

#[tokio::test]
async fn reconstruct_rolls_back_empty_rebuild_no_spurious_replica() {
    // Fresh target, but only 2 survivors offered (slots 2,3) -- below k=3, so every
    // block group is unrecoverable and NOTHING is rebuilt. The provisioned container
    // must be rolled back, not left/closed as an empty "healthy" replica (which the
    // post-rebuild ICR would otherwise announce to SCM).
    let mut dns = Vec::new();
    for i in 0..6 {
        dns.push(spawn_dn(i).await);
    }
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 3 + 1) as u8).collect();
    seed_block_group(&dns, LOCAL, &payload).await;

    let sources: Vec<(u8, String)> = vec![
        (2, format!("http://{}", dns[1].addr)),
        (3, format!("http://{}", dns[2].addr)),
    ];
    let input = repair::ReconstructInput {
        container: ContainerId(CONTAINER),
        ec: ec(),
        missing_slots: vec![1],
        sources,
    };
    let rebuilt = repair::reconstruct_from_survivors(&dns[5].meta, &dns[5].chunks, input)
        .await
        .expect("an unrecoverable rebuild is a clean no-op, not an error");
    assert!(rebuilt.is_empty(), "nothing is rebuilt when survivors < k");
    assert!(
        dns[5]
            .meta
            .get_container(ContainerId(CONTAINER))
            .await
            .unwrap()
            .is_none(),
        "a fresh container that rebuilt nothing must be rolled back -- no empty replica"
    );

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

/// After a compliant ReconstructEC rebuilds a fresh whole replica, the datanode must
/// ANNOUNCE the new replica to SCM via an incremental container report marking it
/// CLOSED (real Ozone's sendICR-on-close) -- the convergence signal that overwrites
/// SCM's prior UNHEALTHY entry. Without it SCM never learns the replica was restored.
#[tokio::test]
async fn reconstruct_announces_closed_replica_to_scm() {
    let mut dns = Vec::new();
    for i in 0..6 {
        dns.push(spawn_dn(i).await);
    }
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 3 + 1) as u8).collect();
    seed_block_group(&dns, LOCAL, &payload).await; // slots 1..5 on dns[0..4]

    fn oz_dd(uuid: &str, port: u16) -> oz::DatanodeDetailsProto {
        oz::DatanodeDetailsProto {
            uuid: Some(uuid.to_string()),
            ip_address: "127.0.0.1".to_string(),
            host_name: "h".to_string(),
            ports: vec![oz::Port {
                name: "REPLICATION".to_string(),
                value: port as u32,
            }],
            ..Default::default()
        }
    }
    let sources: Vec<oz::DatanodeDetailsAndReplicaIndexProto> = (1..5)
        .map(|i| oz::DatanodeDetailsAndReplicaIndexProto {
            datanode_details: oz_dd(&format!("dn-{i}"), dns[i].addr.port()),
            replica_index: (i + 1) as i32,
        })
        .collect();
    let cmd = oz::ScmCommandProto {
        command_type: oz::scm_command_proto::Type::ReconstructEcContainersCommand as i32,
        reconstruct_ec_containers_command_proto: Some(oz::ReconstructEcContainersCommandProto {
            container_id: CONTAINER as i64,
            sources,
            targets: vec![oz_dd("dn-5", dns[5].addr.port())],
            missing_container_indexes: vec![1u8],
            ec_replication_config: oz::EcReplicationConfig {
                data: 3,
                parity: 2,
                codec: "rs".to_string(),
                ec_chunk_size: 8,
            },
            cmd_id: 1,
        }),
        ..Default::default()
    };

    let scm = CompliantScm::with_commands(vec![cmd]);
    let record = scm.record();
    let scm_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let scm_addr = scm_listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(scm.into_server())
            .serve_with_incoming(TcpListenerStream::new(scm_listener))
            .await
            .ok();
    });
    let reg = CompliantScmRegistration {
        uuid: "dn-5".to_string(),
        ip_address: "127.0.0.1".to_string(),
        host_name: "h".to_string(),
        data_port: dns[5].addr.port() as u32,
        meta: dns[5].meta.clone(),
        chunks: dns[5].chunks.clone(),
        heartbeat_interval: Duration::from_millis(50),
        repairs: None,
    };
    tokio::spawn(async move {
        reg.run(format!("http://{scm_addr}")).await.ok();
    });

    // PROOF: poll the recorded heartbeats until one carries an INCREMENTAL report
    // marking container CONTAINER, slot 1, CLOSED. The ICR rides the heartbeat AFTER
    // the one that delivered the command, so a few ticks may pass first.
    let mut announced = false;
    for _ in 0..200 {
        let found = record.lock().heartbeats.iter().any(|hb| {
            hb.incremental_container_report.iter().any(|icr| {
                icr.report.iter().any(|r| {
                    r.container_id == CONTAINER as i64
                        && r.state == oz::container_replica_proto::State::Closed as i32
                        && r.replica_index == Some(1)
                })
            })
        });
        if found {
            announced = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        announced,
        "the datanode must announce the rebuilt replica to SCM as CLOSED (incremental report)"
    );

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

/// END-TO-END compliant self-heal: a scrubber finds on-disk rot, the COMPLIANT loop
/// reports it UNHEALTHY (incremental), the compliant fake SCM (pipeline mode) turns
/// that report into a real ReconstructEC, and the same loop heals the shard AT REST
/// -- with NO externally-injected command. The compliant analog of
/// `scrub_to_self_heal_closes_the_loop`, proving the whole loop closes over the real
/// Apache Ozone wire protocol.
#[tokio::test]
async fn compliant_scrub_to_self_heal_closes_the_loop() {
    let mut dns = Vec::new();
    for i in 0..5 {
        dns.push(spawn_dn(i).await);
    }
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 3 + 1) as u8).collect();
    let originals = seed_block_group(&dns, LOCAL, &payload).await;
    let original_slot1 = originals[0].clone();
    corrupt_chunk(&dns[0], LOCAL, 1);

    let mut target = DnClient::connect(format!("http://{}", dns[0].addr)).await.unwrap();
    let (b, c) = verify_read(1);
    assert!(
        target.read_chunk(&b, &c, true).await.is_err(),
        "shard must be corrupt before the self-heal loop runs"
    );

    let scm = CompliantScm::with_pipeline(compliant_pipeline_for(&dns));
    let record = scm.record();
    spawn_compliant_heal_stack(&dns[0], serve_compliant_scm(scm).await);

    // PROOF: slot 1 heals at rest with NO injected command -- the loop closed itself.
    let healed = poll_verified_read(&mut target, LOCAL, 1, 250)
        .await
        .expect("shard must self-heal via the COMPLIANT scrubber -> SCM -> reconstruct loop");
    assert_eq!(healed, original_slot1, "self-healed bytes are the original");

    // The DN's signal was an INCREMENTAL UNHEALTHY report naming slot 1.
    let saw_unhealthy = record.lock().heartbeats.iter().any(|hb| {
        hb.incremental_container_report.iter().any(|icr| {
            icr.report.iter().any(|r| {
                r.state == oz::container_replica_proto::State::Unhealthy as i32
                    && r.replica_index == Some(1)
                    && r.container_id == CONTAINER as i64
            })
        })
    });
    assert!(saw_unhealthy, "expected an INCREMENTAL UNHEALTHY report for slot 1");

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

#[tokio::test]
async fn compliant_self_heal_covers_all_block_groups_in_replica() {
    let mut dns = Vec::new();
    for i in 0..5 {
        dns.push(spawn_dn(i).await);
    }
    let o1 = seed_block_group(&dns, 1, &(0..PAYLOAD_LEN).map(|i| (i * 3 + 1) as u8).collect::<Vec<_>>()).await;
    let o2 = seed_block_group(&dns, 2, &(0..PAYLOAD_LEN).map(|i| (i * 5 + 2) as u8).collect::<Vec<_>>()).await;
    // Corrupt slot 1 in BOTH of this DN's block groups.
    corrupt_chunk(&dns[0], 1, 1);
    corrupt_chunk(&dns[0], 2, 1);

    let scm = CompliantScm::with_pipeline(compliant_pipeline_for(&dns));
    let record = scm.record();
    spawn_compliant_heal_stack(&dns[0], serve_compliant_scm(scm).await);

    let mut target = DnClient::connect(format!("http://{}", dns[0].addr)).await.unwrap();
    // ONE UNHEALTHY report (latch) -> one ReconstructEC, whose survivor-enumeration
    // rebuilds slot 1 across BOTH block groups of the replica.
    let h1 = poll_verified_read(&mut target, 1, 1, 250).await.expect("block 1 heals");
    let h2 = poll_verified_read(&mut target, 2, 1, 250).await.expect("block 2 heals");
    assert_eq!(h1, o1[0]);
    assert_eq!(h2, o2[0]);

    // The rising-edge latch collapses both findings into a SINGLE UNHEALTHY report.
    let unhealthy = record
        .lock()
        .heartbeats
        .iter()
        .flat_map(|hb| hb.incremental_container_report.clone())
        .flat_map(|icr| icr.report)
        .filter(|r| {
            r.state == oz::container_replica_proto::State::Unhealthy as i32
                && r.replica_index == Some(1)
        })
        .count();
    assert_eq!(
        unhealthy, 1,
        "the rising-edge latch must collapse both block-group findings into one report"
    );

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

#[tokio::test]
async fn compliant_self_heal_gives_up_cleanly_when_unrecoverable() {
    let mut dns = Vec::new();
    for i in 0..5 {
        dns.push(spawn_dn(i).await);
    }
    // Block group 1: corrupt slot 1 (target) AND the two parity peers (slots 4,5),
    // leaving only slots 2,3 valid = 2 < k=3 -> unrecoverable.
    let _o1 = seed_block_group(&dns, 1, &(0..PAYLOAD_LEN).map(|i| (i * 3 + 1) as u8).collect::<Vec<_>>()).await;
    corrupt_chunk(&dns[0], 1, 1);
    corrupt_chunk(&dns[3], 1, 4);
    corrupt_chunk(&dns[4], 1, 5);
    // Block group 2: fully intact -- the liveness control.
    let o2 = seed_block_group(&dns, 2, &(0..PAYLOAD_LEN).map(|i| (i * 7 + 9) as u8).collect::<Vec<_>>()).await;

    let scm = CompliantScm::with_pipeline(compliant_pipeline_for(&dns));
    spawn_compliant_heal_stack(&dns[0], serve_compliant_scm(scm).await);
    let mut target = DnClient::connect(format!("http://{}", dns[0].addr)).await.unwrap();

    // Give the loop ample time to attempt + fail the unrecoverable repair.
    tokio::time::sleep(Duration::from_millis(800)).await;

    // The unrecoverable shard stays corrupt (no panic, no garbage written).
    let (b, c) = verify_read_at(1, 1);
    assert!(
        target.read_chunk(&b, &c, true).await.is_err(),
        "an unrecoverable shard must remain corrupt, not be half-written"
    );
    // The datanode stays live: the intact block group (slot 1 of group 2) still serves.
    let live = poll_verified_read(&mut target, 2, 1, 50)
        .await
        .expect("intact block group must still be served after a failed repair");
    assert_eq!(live, o2[0]);

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

#[tokio::test]
async fn compliant_concurrent_self_heal_of_two_slots() {
    let mut dns = Vec::new();
    for i in 0..5 {
        dns.push(spawn_dn(i).await);
    }
    let originals = seed_block_group(&dns, LOCAL, &(0..PAYLOAD_LEN).map(|i| (i * 3 + 1) as u8).collect::<Vec<_>>()).await;
    // Corrupt two DATA shards at once -- slot 1 (dns[0]) and slot 2 (dns[1]). Both must
    // self-heal even though each command targets a DIFFERENT DN: the fake SCM routes
    // each synthesized ReconstructEC only to the DN its targets name (one CompliantScm,
    // two heartbeating DNs). Survivors among slots 3,4,5 (= k=3) always suffice.
    corrupt_chunk(&dns[0], LOCAL, 1);
    corrupt_chunk(&dns[1], LOCAL, 2);

    let endpoint = serve_compliant_scm(CompliantScm::with_pipeline(compliant_pipeline_for(&dns))).await;
    spawn_compliant_heal_stack(&dns[0], endpoint.clone());
    spawn_compliant_heal_stack(&dns[1], endpoint);

    let mut t0 = DnClient::connect(format!("http://{}", dns[0].addr)).await.unwrap();
    let mut t1 = DnClient::connect(format!("http://{}", dns[1].addr)).await.unwrap();
    let h0 = poll_verified_read(&mut t0, LOCAL, 1, 300).await.expect("slot 1 self-heals");
    let h1 = poll_verified_read(&mut t1, LOCAL, 2, 300).await.expect("slot 2 self-heals");
    assert_eq!(h0, originals[0], "slot 1 healed to the original bytes");
    assert_eq!(h1, originals[1], "slot 2 healed to the original bytes");

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

#[tokio::test]
async fn compliant_malformed_reconstruct_does_not_stall_the_loop() {
    let mut dns = Vec::new();
    for i in 0..5 {
        dns.push(spawn_dn(i).await);
    }
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 3 + 1) as u8).collect();
    seed_block_group(&dns, LOCAL, &payload).await;

    // A ReconstructEC with an UNPARSEABLE codec, FOLLOWED by a CloseContainer in the
    // same batch. The malformed command must be handled gracefully (logged, skipped)
    // without stalling the loop, so the close still takes effect.
    let bad = oz::ScmCommandProto {
        command_type: oz::scm_command_proto::Type::ReconstructEcContainersCommand as i32,
        reconstruct_ec_containers_command_proto: Some(oz::ReconstructEcContainersCommandProto {
            container_id: CONTAINER as i64,
            sources: Vec::new(),
            targets: vec![oz::DatanodeDetailsProto {
                uuid: Some("dn-0".to_string()),
                ..Default::default()
            }],
            missing_container_indexes: vec![1u8],
            ec_replication_config: oz::EcReplicationConfig {
                data: 3,
                parity: 2,
                codec: "not-a-codec".to_string(),
                ec_chunk_size: 8,
            },
            cmd_id: 1,
        }),
        ..Default::default()
    };
    let close = oz::ScmCommandProto {
        command_type: oz::scm_command_proto::Type::CloseContainerCommand as i32,
        close_container_command_proto: Some(oz::CloseContainerCommandProto {
            container_id: CONTAINER as i64,
            ..Default::default()
        }),
        ..Default::default()
    };

    let endpoint = serve_compliant_scm(CompliantScm::with_commands(vec![bad, close])).await;
    let reg = CompliantScmRegistration {
        uuid: "dn-0".to_string(),
        ip_address: "127.0.0.1".to_string(),
        host_name: "h".to_string(),
        data_port: dns[0].addr.port() as u32,
        meta: dns[0].meta.clone(),
        chunks: dns[0].chunks.clone(),
        heartbeat_interval: Duration::from_millis(50),
        repairs: None,
    };
    tokio::spawn(async move {
        reg.run(endpoint).await.ok();
    });

    // The CloseContainer must take effect -> the loop survived the malformed command.
    let mut closed = false;
    for _ in 0..150 {
        if let Some(ci) = dns[0].meta.get_container(ContainerId(CONTAINER)).await.unwrap() {
            if ci.state == ContainerState::Closed {
                closed = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        closed,
        "the loop must process CloseContainer after a malformed ReconstructEC"
    );

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}
