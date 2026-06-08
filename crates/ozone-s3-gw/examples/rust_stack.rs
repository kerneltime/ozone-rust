//! Standalone launcher for the Rust Ozone S3 stack, so EXTERNAL acceptance tests
//! (the `aws` CLI, Apache Ozone's Robot Framework S3 smoketests) can run against
//! the real gateway + datanode binaries over HTTP/gRPC.
//!
//! It stands up, in one process: 5 datanodes (`DatanodeService` over temporary
//! fjall + filesystem stores), the compliant OM fixture (`CompliantOm`, which is
//! wire-compliant with the real `OzoneManagerService` but is an in-memory fixture,
//! NOT a real Java OM), and the S3 gateway on a fixed port. Then it blocks.
//!
//! This isolates exactly what the Rust reimplementation owns end to end — the S3
//! HTTP surface, the OM client envelope, the EC math, and the datanode data path —
//! and exercises it with the same tools a real cluster's acceptance suite uses.
//! A real Java OM/SCM cluster is a separate integration (the Rust datanode also
//! needs an SCM gRPC adapter; the data plane is Rust-native).
//!
//! Run: `cargo run --release --example rust_stack`
//! Env: `RUST_STACK_S3_PORT` (default 9878).

use std::sync::Arc;

use ozone_dn_server::DatanodeService;
use ozone_fjall_store::FjallMetaStore;
use ozone_grpc_types::hadoop::hdds;
use ozone_s3_gw::{serve, Gateway};
use ozone_storage::{ChunkStore, FileChunkStore, MetaStore};
use test_fixtures::compliant_om::{CompliantOm, CompliantOmPipeline};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

// EC-3-2: exactly 5 datanodes, one EC slot each.
const DATA: i32 = 3;
const PARITY: i32 = 2;
const N_DN: usize = (DATA + PARITY) as usize;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let s3_port: u16 = std::env::var("RUST_STACK_S3_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9878);

    let base = std::env::temp_dir().join(format!("rust-ozone-stack-{}", std::process::id()));
    let mut members = Vec::with_capacity(N_DN);
    for i in 0..N_DN {
        let dir = base.join(format!("dn-{i}"));
        let meta: Arc<dyn MetaStore> = Arc::new(FjallMetaStore::open(dir.join("meta"))?);
        let chunks: Arc<dyn ChunkStore> = Arc::new(FileChunkStore::new(dir.join("data")));
        let service = DatanodeService::new(meta, chunks).into_server();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .ok();
        });
        members.push(hdds::DatanodeDetailsProto {
            uuid: Some(format!("dn-{i}")),
            ip_address: "127.0.0.1".to_string(),
            host_name: "h".to_string(),
            ports: vec![hdds::Port {
                name: "REPLICATION".to_string(),
                value: addr.port() as u32,
            }],
            ..Default::default()
        });
        eprintln!("datanode {i} (EC slot {}) on {addr}", i + 1);
    }

    let pipeline = CompliantOmPipeline {
        datanodes: members,
        ec: hdds::EcReplicationConfig {
            data: DATA,
            parity: PARITY,
            codec: "rs".to_string(),
            ec_chunk_size: 1024,
        },
    };
    let om = CompliantOm::new(pipeline).into_server();
    let om_listener = TcpListener::bind("127.0.0.1:0").await?;
    let om_addr = om_listener.local_addr()?;
    tokio::spawn(async move {
        Server::builder()
            .add_service(om)
            .serve_with_incoming(TcpListenerStream::new(om_listener))
            .await
            .ok();
    });
    eprintln!("compliant OM on http://{om_addr}");

    let gateway = Arc::new(Gateway::connect(format!("http://{om_addr}"), "us-east-1").await?);
    let s3_listener = TcpListener::bind(("127.0.0.1", s3_port)).await?;
    eprintln!("S3 gateway listening on http://127.0.0.1:{s3_port}");
    eprintln!("READY");
    serve(gateway, s3_listener).await?;
    Ok(())
}
