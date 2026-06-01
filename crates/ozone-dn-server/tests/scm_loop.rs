//! Datanode <-> SCM control-plane test: the datanode registers with a fake SCM,
//! heartbeats, and executes a `CloseContainer` command pushed back on the
//! heartbeat stream.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use ozone_dn_server::ScmRegistration;
use ozone_fjall_store::FjallMetaStore;
use ozone_grpc_types::dn::v1 as dn;
use ozone_grpc_types::scm::dn::v1 as scm;
use ozone_storage::MetaStore;
use ozone_types::{ContainerId, ContainerInfo, ContainerState, EcReplicationConfig};
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
        },
        meta: meta.clone(),
        heartbeat_interval: Duration::from_millis(50),
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
