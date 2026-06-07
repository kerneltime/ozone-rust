//! Datanode <-> SCM control-plane test: the datanode registers with a fake SCM,
//! heartbeats, and executes a `CloseContainer` command pushed back on the
//! heartbeat stream.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ozone_dn_server::ScmRegistration;
use ozone_fjall_store::FjallMetaStore;
use ozone_grpc_types::dn::v1 as dn;
use ozone_grpc_types::scm::dn::v1 as scm;
use ozone_storage::{ChunkStore, FileChunkStore, MetaStore};
use ozone_types::{
    BlockId, ChunkInfo, ContainerId, ContainerInfo, ContainerState, EcReplicationConfig, LocalId,
    ReplicaIndex,
};
use test_fixtures::fake_scm::FakeScm;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

async fn poll_until<F: Fn() -> bool>(cond: F) -> bool {
    for _ in 0..150 {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    cond()
}

#[tokio::test]
async fn datanode_registers_heartbeats_and_closes_container() {
    let dir = std::env::temp_dir().join(format!("ozone-scm-loop-{}", std::process::id()));
    let meta = Arc::new(FjallMetaStore::open(dir.join("meta")).unwrap());
    meta.create_container(ContainerInfo::new_open(
        ContainerId(1),
        EcReplicationConfig::RS_3_2_1MIB,
    ))
    .await
    .unwrap();
    let chunks = Arc::new(FileChunkStore::new(dir.join("data")));

    // Fake SCM that delivers a CloseContainer(1) on the first heartbeat reply.
    let close_cmd = scm::ScmCommand {
        cmd_id: 1,
        term: 0,
        encoded_token: Vec::new(),
        deadline_ms: 0,
        payload: Some(scm::scm_command::Payload::CloseContainer(
            scm::CloseContainerCommand {
                container_id: Some(dn::ContainerId { id: 1 }),
                force: false,
            },
        )),
    };
    let fake = FakeScm::with_commands(vec![close_cmd]);
    let heartbeats = fake.heartbeat_counter();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(fake.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .ok();
    });

    // Run the datanode's registration + heartbeat loop.
    let reg = ScmRegistration {
        datanode_id: scm::DatanodeId {
            uuid: "dn-test".to_string(),
            ip_address: "127.0.0.1".to_string(),
            host_name: "host".to_string(),
            version: "1".to_string(),
            setup_time_ms: 0,
            gateway_port: 0,
        },
        meta: meta.clone(),
        chunks,
        heartbeat_interval: Duration::from_millis(50),
        repairs: None,
    };
    let endpoint = format!("http://{addr}");
    tokio::spawn(async move {
        reg.run(endpoint).await.ok();
    });

    // The datanode should heartbeat...
    assert!(
        poll_until(|| heartbeats.load(Ordering::Relaxed) > 0).await,
        "datanode never heartbeated"
    );

    // ...and act on the CloseContainer command (re-checked with awaits, which a
    // sync predicate can't do).
    let mut became_closed = false;
    for _ in 0..150 {
        if let Some(info) = meta.get_container(ContainerId(1)).await.unwrap() {
            if info.state == ContainerState::Closed {
                became_closed = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(became_closed, "container 1 was not closed by the SCM command");

    tokio::fs::remove_dir_all(&dir).await.ok();
}

#[tokio::test]
async fn datanode_sends_full_container_report_on_registration() {
    let dir = std::env::temp_dir().join(format!("ozone-scm-report-{}", std::process::id()));
    let meta = Arc::new(FjallMetaStore::open(dir.join("meta")).unwrap());
    for id in [1u64, 2] {
        meta.create_container(ContainerInfo::new_open(
            ContainerId(id),
            EcReplicationConfig::RS_3_2_1MIB,
        ))
        .await
        .unwrap();
    }
    let chunks = Arc::new(FileChunkStore::new(dir.join("data")));

    let fake = FakeScm::new();
    let reports = fake.received_reports();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(fake.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .ok();
    });

    let reg = ScmRegistration {
        datanode_id: scm::DatanodeId {
            uuid: "dn-report".to_string(),
            ip_address: "127.0.0.1".to_string(),
            host_name: "host".to_string(),
            version: "1".to_string(),
            setup_time_ms: 0,
            gateway_port: 0,
        },
        meta: meta.clone(),
        chunks,
        heartbeat_interval: Duration::from_millis(50),
        repairs: None,
    };
    let endpoint = format!("http://{addr}");
    tokio::spawn(async move {
        reg.run(endpoint).await.ok();
    });

    assert!(
        poll_until(|| !reports.lock().is_empty()).await,
        "SCM received no container report"
    );
    let snapshot = reports.lock().clone();
    let full = snapshot
        .iter()
        .find(|r| r.kind == scm::container_report_request::Kind::Full as i32)
        .expect("a FULL container report");
    let mut ids: Vec<u64> = full
        .reports
        .iter()
        .filter_map(|c| c.container_id.as_ref().map(|id| id.id))
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2], "FULL report must list every held container");

    tokio::fs::remove_dir_all(&dir).await.ok();
}

#[tokio::test]
async fn datanode_reclaims_chunks_on_delete_container_command() {
    let dir = std::env::temp_dir().join(format!("ozone-scm-delete-{}", std::process::id()));
    let meta = Arc::new(FjallMetaStore::open(dir.join("meta")).unwrap());
    meta.create_container(ContainerInfo::new_open(
        ContainerId(7),
        EcReplicationConfig::RS_3_2_1MIB,
    ))
    .await
    .unwrap();
    let chunks = Arc::new(FileChunkStore::new(dir.join("data")));
    // Seed a chunk in container 7.
    let block = BlockId::ec(ContainerId(7), LocalId(1), ReplicaIndex::new(1));
    let chunk = ChunkInfo {
        chunk_name: "0".to_string(),
        offset: 0,
        len: 3,
        checksum_data: None,
        stripe_checksum: None,
    };
    chunks
        .write_chunk(&block, &chunk, Bytes::from_static(b"abc"))
        .await
        .unwrap();
    assert!(
        chunks.read_chunk(&block, &chunk).await.is_ok(),
        "chunk present before delete"
    );

    // Fake SCM delivers DeleteContainer(7) on the first heartbeat reply.
    let del_cmd = scm::ScmCommand {
        cmd_id: 1,
        term: 0,
        encoded_token: Vec::new(),
        deadline_ms: 0,
        payload: Some(scm::scm_command::Payload::DeleteContainer(
            scm::DeleteContainerCommand {
                container_id: Some(dn::ContainerId { id: 7 }),
                force: false,
            },
        )),
    };
    let fake = FakeScm::with_commands(vec![del_cmd]);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(fake.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .ok();
    });

    let reg = ScmRegistration {
        datanode_id: scm::DatanodeId {
            uuid: "dn-delete".to_string(),
            ip_address: "127.0.0.1".to_string(),
            host_name: "host".to_string(),
            version: "1".to_string(),
            setup_time_ms: 0,
            gateway_port: 0,
        },
        meta: meta.clone(),
        chunks: chunks.clone(),
        heartbeat_interval: Duration::from_millis(50),
        repairs: None,
    };
    let endpoint = format!("http://{addr}");
    tokio::spawn(async move {
        reg.run(endpoint).await.ok();
    });

    // The command must reclaim BOTH the metadata and the chunk bytes (deleting
    // only metadata would leak the chunk files).
    let mut reclaimed = false;
    for _ in 0..150 {
        let meta_gone = meta.get_container(ContainerId(7)).await.unwrap().is_none();
        let chunk_gone = chunks.read_chunk(&block, &chunk).await.is_err();
        if meta_gone && chunk_gone {
            reclaimed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(reclaimed, "DeleteContainer must reclaim metadata AND chunk bytes");

    tokio::fs::remove_dir_all(&dir).await.ok();
}
