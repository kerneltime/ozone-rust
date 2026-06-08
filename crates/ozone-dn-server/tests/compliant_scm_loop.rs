//! Compliance tests for the datanode's real-protocol SCM loop
//! ([`CompliantScmRegistration`]) against the [`CompliantScm`] fixture: the
//! register must carry all four reports, a REPLICATION named port, real volume
//! capacity, the DN's OWN uuid, and EC replicas with a VALID 1-based replica index
//! (never 0 — which would crash a real SCM's replica-count math); and the poll loop
//! must execute the commands SCM returns on the heartbeat.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ozone_dn_server::CompliantScmRegistration;
use ozone_fjall_store::FjallMetaStore;
use ozone_grpc_types::hadoop::hdds as oz;
use ozone_storage::{checksum, ChunkStore, FileChunkStore, MetaStore};
use ozone_types::{
    BlockData, BlockId, ChecksumType, ChunkInfo, ContainerId, ContainerInfo, ContainerState,
    EcReplicationConfig, LocalId, ReplicaIndex,
};
use test_fixtures::compliant_scm::CompliantScm;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

async fn serve(scm: CompliantScm) -> String {
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

/// Seed an EC container with one block at `slot`, so this DN's container report
/// derives that replica index.
async fn seed(meta: &Arc<dyn MetaStore>, chunks: &Arc<dyn ChunkStore>, container: ContainerId, slot: u8) {
    meta.create_container(ContainerInfo::new_open(
        container,
        EcReplicationConfig::rs(3, 2, 1024),
    ))
    .await
    .unwrap();
    let bytes = b"hello";
    let cd = checksum::compute(bytes, 1024, ChecksumType::Crc32c).unwrap();
    let chunk = ChunkInfo {
        chunk_name: "0".to_string(),
        offset: 0,
        len: bytes.len() as u64,
        checksum_data: Some(cd),
        stripe_checksum: None,
    };
    let bslot = BlockId::ec(container, LocalId(1), ReplicaIndex::new(slot));
    chunks
        .write_chunk(&bslot, &chunk, Bytes::from_static(bytes))
        .await
        .unwrap();
    let mut bd = BlockData::new(bslot);
    bd.chunks.push(chunk);
    bd.set_block_group_len(bytes.len() as u64);
    meta.put_block(&bd).await.unwrap();
}

#[tokio::test]
async fn registers_compliantly_and_executes_close_container() {
    let dir = std::env::temp_dir().join(format!("ozone-compliant-loop-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let meta: Arc<dyn MetaStore> = Arc::new(FjallMetaStore::open(dir.join("meta")).unwrap());
    let chunks: Arc<dyn ChunkStore> = Arc::new(FileChunkStore::new(dir.join("data")));
    let container = ContainerId(7);
    seed(&meta, &chunks, container, 3).await; // this DN holds EC slot 3

    // Queue a CloseContainer command, delivered on a heartbeat.
    let close = oz::ScmCommandProto {
        command_type: oz::scm_command_proto::Type::CloseContainerCommand as i32,
        close_container_command_proto: Some(oz::CloseContainerCommandProto {
            container_id: container.0 as i64,
            pipeline_id: oz::PipelineId::default(),
            cmd_id: 1,
            force: None,
        }),
        ..Default::default()
    };
    let scm = CompliantScm::with_commands(vec![close]);
    let record = scm.record();
    let endpoint = serve(scm).await;

    let reg = CompliantScmRegistration {
        uuid: "dn-1".to_string(),
        ip_address: "127.0.0.1".to_string(),
        host_name: "host".to_string(),
        data_port: 19864,
        meta: meta.clone(),
        chunks: chunks.clone(),
        heartbeat_interval: Duration::from_millis(50),
        repairs: None,
    };
    tokio::spawn(async move {
        reg.run(endpoint).await.ok();
    });

    // The command takes effect -> proves register + poll + dispatch end to end.
    let mut closed = false;
    for _ in 0..150 {
        if let Some(ci) = meta.get_container(container).await.unwrap() {
            if ci.state == ContainerState::Closed {
                closed = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(closed, "CloseContainer command must close the container");

    // Register compliance assertions (the register snapshot was taken while OPEN).
    let rec = record.lock();
    assert_eq!(rec.registers.len(), 1, "registered exactly once");
    let r = &rec.registers[0];
    let dd = &r.extended_datanode_details.datanode_details;
    assert_eq!(dd.uuid.as_deref(), Some("dn-1"), "datanode keeps its OWN uuid (no rename)");
    assert!(
        dd.ports.iter().any(|p| p.name == "REPLICATION" && p.value == 19864),
        "register advertises a REPLICATION named port"
    );
    assert!(
        r.node_report
            .storage_report
            .iter()
            .any(|s| s.capacity.unwrap_or(0) > 0 && !s.storage_uuid.is_empty()),
        "register carries a non-zero-capacity volume with a storage uuid"
    );
    let replica = r
        .container_report
        .reports
        .iter()
        .find(|x| x.container_id == container.0 as i64)
        .expect("the held container is reported");
    assert_eq!(
        replica.replica_index,
        Some(3),
        "EC replica_index is the 1-based slot, NOT 0 (a 0 crashes the real SCM)"
    );
    assert_eq!(
        replica.state,
        oz::container_replica_proto::State::Open as i32,
        "container state maps to the real enum"
    );

    drop(rec);
    std::fs::remove_dir_all(&dir).ok();
}
