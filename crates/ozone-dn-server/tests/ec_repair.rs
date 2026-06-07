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
use ozone_dn_server::{repair, DatanodeService, ScmRegistration};
use ozone_ec::stripe::{encode_object, EncodedShards};
use ozone_ec::Profile;
use ozone_fjall_store::FjallMetaStore;
use ozone_grpc_types::dn::v1 as dn;
use ozone_grpc_types::scm::dn::v1 as scm;
use ozone_storage::{checksum, ChunkStore, FileChunkStore, MetaStore};
use ozone_types::{
    BlockData, BlockId, ChecksumType, ChunkInfo, ContainerId, ContainerInfo, EcReplicationConfig,
    LocalId, ReplicaIndex,
};
use test_fixtures::fake_scm::{FakeScm, PipelineFixture};
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

fn datanode_id(dn: &Dn) -> scm::DatanodeId {
    scm::DatanodeId {
        uuid: dn.uuid.clone(),
        ip_address: "127.0.0.1".to_string(),
        host_name: "host".to_string(),
        version: "1".to_string(),
        setup_time_ms: 0,
        gateway_port: dn.addr.port() as u32,
    }
}

#[tokio::test]
async fn reconstruct_ec_repairs_corrupt_shard_at_rest() {
    // 5 datanodes; dns[i] holds EC slot i+1 (slots 1-3 data, 4-5 parity).
    let mut dns = Vec::new();
    for i in 0..5 {
        dns.push(spawn_dn(i).await);
    }

    // Seed the block group: encode a partial-stripe object and place one shard
    // per datanode.
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 7 + 1) as u8).collect();
    let shards = encode_object(profile(), &payload).unwrap();
    for slot in 1..=5u8 {
        seed_shard(&dns[(slot - 1) as usize], slot, &shard_bytes(&shards, slot)).await;
    }
    let original_slot1 = shard_bytes(&shards, 1);

    // Corrupt slot 1's chunk file on disk (dns[0]), leaving its stored checksum
    // intact so a verified read detects the rot.
    let chunk_path = dns[0]
        .dir
        .join("data")
        .join(CONTAINER.to_string())
        .join("chunks")
        .join(format!("{LOCAL}_1_0"));
    let mut bytes = std::fs::read(&chunk_path).unwrap();
    bytes[0] ^= 0xFF;
    std::fs::write(&chunk_path, &bytes).unwrap();

    // Pre-assert (fail-without-fix): a verified read of the corrupt shard fails.
    let mut target = DnClient::connect(format!("http://{}", dns[0].addr)).await.unwrap();
    let (b, c) = verify_read(1);
    assert!(
        target.read_chunk(&b, &c, true).await.is_err(),
        "corrupt shard must fail verification before repair"
    );

    // Build the SCM ReconstructEC command: target dns[0] (slot 1), sources the
    // four survivors, with the slot map and the block-group length.
    let sources: Vec<scm::DatanodeId> = (1..5).map(|i| datanode_id(&dns[i])).collect();
    let source_replica_indexes = (1..5u32)
        .map(|i| (dns[i as usize].uuid.clone(), i + 1))
        .collect();
    let cmd = scm::ScmCommand {
        cmd_id: 1,
        term: 0,
        encoded_token: Vec::new(),
        deadline_ms: 0,
        payload: Some(scm::scm_command::Payload::ReconstructEcContainers(
            scm::ReconstructEcContainersCommand {
                container_id: Some(dn::ContainerId { id: CONTAINER }),
                sources,
                targets: vec![datanode_id(&dns[0])],
                missing_indexes: vec![1],
                ec_config: Some(ec().into()),
                source_replica_indexes,
                blocks: vec![scm::ReconstructBlock {
                    local_id: LOCAL,
                    block_group_len: PAYLOAD_LEN as u64,
                }],
            },
        )),
    };

    // Deliver the command to dns[0]'s SCM loop via a fake SCM.
    let fake = FakeScm::with_commands(vec![cmd]);
    let scm_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let scm_addr = scm_listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(fake.into_server())
            .serve_with_incoming(TcpListenerStream::new(scm_listener))
            .await
            .ok();
    });
    let reg = ScmRegistration {
        datanode_id: datanode_id(&dns[0]),
        meta: dns[0].meta.clone(),
        chunks: dns[0].chunks.clone(),
        heartbeat_interval: Duration::from_millis(50),
        repairs: None,
    };
    tokio::spawn(async move {
        reg.run(format!("http://{scm_addr}")).await.ok();
    });

    // THE PROOF: poll until a verified per-slot read of slot 1 succeeds. The
    // datanode read path never invokes the EC decoder, so a clean verified read
    // means the on-disk bytes are correct — healed at rest, not reconstructed.
    let mut healed = None;
    for _ in 0..150 {
        let (b, c) = verify_read(1);
        if let Ok(bytes) = target.read_chunk(&b, &c, true).await {
            healed = Some(bytes);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let healed = healed.expect("slot 1 was not healed at rest by the reconstruct command");
    assert_eq!(
        healed.as_ref(),
        &original_slot1[..],
        "repaired shard must be byte-identical to the original"
    );

    // The block metadata (checksum + block-group length) was persisted too.
    let bd = dns[0]
        .meta
        .get_block(&BlockId::ec(
            ContainerId(CONTAINER),
            LocalId(LOCAL),
            ReplicaIndex::new(1),
        ))
        .await
        .unwrap()
        .expect("repaired block metadata present");
    assert!(bd.chunks[0].checksum_data.is_some(), "checksum persisted");
    assert_eq!(bd.block_group_len(), Some(PAYLOAD_LEN as u64));

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
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

// ---- self-heal loop (scrubber -> SCM report -> ReconstructEC -> repair) ----

/// The full 5-slot pipeline (all datanodes). `handle_reconstruct` excludes the
/// slot(s) being rebuilt, so this is correct for any target slot.
fn pipeline_fixture(dns: &[Dn]) -> PipelineFixture {
    PipelineFixture {
        sources: (0..5).map(|i| datanode_id(&dns[i])).collect(),
        source_replica_indexes: (0..5u32)
            .map(|i| (dns[i as usize].uuid.clone(), i + 1))
            .collect(),
        ec_config: ec().into(),
    }
}

/// Serve `fake` as the SCM; return its endpoint.
async fn serve_fake_scm(fake: FakeScm) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(fake.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .ok();
    });
    format!("http://{addr}")
}

/// Run a scrubber (short interval) + SCM loop on `dn`, wired via the repair
/// channel, against the SCM at `scm_endpoint`. NO ScmCommand is built — repair is
/// driven entirely by the closed loop.
fn spawn_heal_stack(dn: &Dn, scm_endpoint: String) {
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    let scrubber = Scrubber::new(dn.meta.clone(), dn.chunks.clone());
    tokio::spawn(async move {
        scrubber.run(Duration::from_millis(25), tx).await;
    });
    let reg = ScmRegistration {
        datanode_id: datanode_id(dn),
        meta: dn.meta.clone(),
        chunks: dn.chunks.clone(),
        heartbeat_interval: Duration::from_millis(50),
        repairs: Some(rx),
    };
    tokio::spawn(async move {
        reg.run(scm_endpoint).await.ok();
    });
}

/// Serve `fake` and run the self-heal stack on the single target dns[0].
async fn spawn_self_heal(dns: &[Dn], fake: FakeScm) {
    let endpoint = serve_fake_scm(fake).await;
    spawn_heal_stack(&dns[0], endpoint);
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

#[tokio::test]
async fn scrub_to_self_heal_closes_the_loop() {
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
        "shard must be corrupt before the loop runs"
    );

    let fake = FakeScm::with_pipeline(pipeline_fixture(&dns));
    let reports = fake.received_reports();
    spawn_self_heal(&dns, fake).await;

    // PROOF: the shard self-heals with NO externally-injected command.
    let healed = poll_verified_read(&mut target, LOCAL, 1, 150)
        .await
        .expect("shard must self-heal via scrubber -> SCM -> reconstruct");
    assert_eq!(healed, original_slot1, "self-healed bytes are the original");

    // The DN's signal was an INCREMENTAL UNHEALTHY report naming slot 1.
    let snap = reports.lock().clone();
    assert!(
        snap.iter().any(|r| {
            r.kind == scm::container_report_request::Kind::Incremental as i32
                && r.reports.iter().any(|cr| {
                    cr.state == dn::container_state::State::Unhealthy as i32 && cr.replica_index == 1
                })
        }),
        "expected an INCREMENTAL UNHEALTHY report for slot 1"
    );

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

#[tokio::test]
async fn self_heal_covers_all_block_groups_in_replica() {
    let mut dns = Vec::new();
    for i in 0..5 {
        dns.push(spawn_dn(i).await);
    }
    let o1 = seed_block_group(&dns, 1, &(0..PAYLOAD_LEN).map(|i| (i * 3 + 1) as u8).collect::<Vec<_>>()).await;
    let o2 = seed_block_group(&dns, 2, &(0..PAYLOAD_LEN).map(|i| (i * 5 + 2) as u8).collect::<Vec<_>>()).await;
    // Corrupt slot 1 in BOTH of this DN's block groups.
    corrupt_chunk(&dns[0], 1, 1);
    corrupt_chunk(&dns[0], 2, 1);

    let fake = FakeScm::with_pipeline(pipeline_fixture(&dns));
    let reports = fake.received_reports();
    spawn_self_heal(&dns, fake).await;

    let mut target = DnClient::connect(format!("http://{}", dns[0].addr)).await.unwrap();
    // ONE UNHEALTHY report (latch) + empty-blocks command heals BOTH block groups.
    let h1 = poll_verified_read(&mut target, 1, 1, 200).await.expect("block 1 heals");
    let h2 = poll_verified_read(&mut target, 2, 1, 200).await.expect("block 2 heals");
    assert_eq!(h1, o1[0]);
    assert_eq!(h2, o2[0]);

    let snap = reports.lock().clone();
    let unhealthy = snap
        .iter()
        .flat_map(|r| r.reports.iter())
        .filter(|cr| cr.state == dn::container_state::State::Unhealthy as i32 && cr.replica_index == 1)
        .count();
    assert_eq!(
        unhealthy, 1,
        "the rising-edge latch must collapse both findings into a single report"
    );

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

#[tokio::test]
async fn self_heal_gives_up_cleanly_when_unrecoverable() {
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

    let fake = FakeScm::with_pipeline(pipeline_fixture(&dns));
    spawn_self_heal(&dns, fake).await;
    let mut target = DnClient::connect(format!("http://{}", dns[0].addr)).await.unwrap();

    // Give the loop ample time to attempt + fail the repair.
    tokio::time::sleep(Duration::from_millis(800)).await;

    // The unrecoverable shard stays corrupt (no panic, no garbage written).
    let (b, c) = verify_read_at(1, 1);
    assert!(
        target.read_chunk(&b, &c, true).await.is_err(),
        "an unrecoverable shard must remain corrupt, not be half-written"
    );
    // The datanode stays live: the intact block group still serves.
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
async fn concurrent_self_heal_of_two_slots() {
    let mut dns = Vec::new();
    for i in 0..5 {
        dns.push(spawn_dn(i).await);
    }
    let originals = seed_block_group(&dns, LOCAL, &(0..PAYLOAD_LEN).map(|i| (i * 3 + 1) as u8).collect::<Vec<_>>()).await;
    // Corrupt two DATA shards at once -- slot 1 (dns[0]) and slot 2 (dns[1]). Both
    // must self-heal even though each repair may read the OTHER (still-corrupt)
    // shard as a candidate source. Survivors among slots 3,4,5 (= k=3) always
    // suffice, so the loop converges regardless of interleaving.
    corrupt_chunk(&dns[0], LOCAL, 1);
    corrupt_chunk(&dns[1], LOCAL, 2);

    let fake = FakeScm::with_pipeline(pipeline_fixture(&dns));
    let endpoint = serve_fake_scm(fake).await;
    spawn_heal_stack(&dns[0], endpoint.clone());
    spawn_heal_stack(&dns[1], endpoint);

    let mut t0 = DnClient::connect(format!("http://{}", dns[0].addr)).await.unwrap();
    let mut t1 = DnClient::connect(format!("http://{}", dns[1].addr)).await.unwrap();
    let h0 = poll_verified_read(&mut t0, LOCAL, 1, 250).await.expect("slot 1 self-heals");
    let h1 = poll_verified_read(&mut t1, LOCAL, 2, 250).await.expect("slot 2 self-heals");
    assert_eq!(h0, originals[0], "slot 1 healed to the original bytes");
    assert_eq!(h1, originals[1], "slot 2 healed to the original bytes");

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
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
