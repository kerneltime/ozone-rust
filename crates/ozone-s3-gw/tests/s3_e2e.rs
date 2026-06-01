//! Full-stack S3 end-to-end test.
//!
//! Stands up the complete vertical: `k+p` real Rust datanodes (fjall metadata +
//! filesystem chunks), the in-memory fake OM bound to a pipeline over those
//! datanodes, and the S3 gateway — then drives **real HTTP S3 requests** through
//! it. The headline assertion is a degraded read: after a data datanode is
//! killed, a GET still returns the exact object bytes, reconstructed via EC
//! through the whole stack.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{header, HeaderMap, Method, Request, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use md5::{Digest, Md5};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use ozone_dn_server::DatanodeService;
use ozone_fjall_store::FjallMetaStore;
use ozone_s3_gw::{serve, Gateway};
use ozone_storage::FileChunkStore;
use test_fixtures::fake_om::{datanode_details, FakeOm, PipelineConfig};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch() -> PathBuf {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("ozone-s3-e2e-{}-{}", std::process::id(), n))
}

struct Datanode {
    uuid: String,
    addr: SocketAddr,
    handle: JoinHandle<()>,
    dir: PathBuf,
}

async fn spawn_datanode(idx: usize) -> Datanode {
    let dir = scratch();
    let meta = Arc::new(FjallMetaStore::open(dir.join("meta")).unwrap());
    let chunks = Arc::new(FileChunkStore::new(dir.join("data")));
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
    Datanode {
        uuid: format!("dn-{idx}"),
        addr,
        handle,
        dir,
    }
}

async fn spawn_om(pipeline: PipelineConfig) -> (String, JoinHandle<()>) {
    let service = FakeOm::new(pipeline).into_server();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .ok();
    });
    (format!("http://{addr}"), handle)
}

type HttpClient = Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>;

async fn http(
    client: &HttpClient,
    method: Method,
    url: String,
    principal: Option<&str>,
    body: Bytes,
) -> (StatusCode, HeaderMap, Bytes) {
    let mut builder = Request::builder().method(method).uri(url);
    if let Some(p) = principal {
        builder = builder.header("x-auth-principal", p);
    }
    let req = builder.body(Full::new(body)).unwrap();
    let resp = client.request(req).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, headers, bytes)
}

fn md5_hex(data: &[u8]) -> String {
    let mut h = Md5::new();
    h.update(data);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

/// Stand up 5 datanodes (RS-3-2, small 1 KiB cells so objects span several
/// stripes) + a FakeOm pipeline + the gateway. Returns the gateway base URL and
/// the datanode handles (for fault injection + cleanup).
async fn spawn_stack() -> (String, Vec<Datanode>) {
    let mut dns = Vec::new();
    for i in 0..5 {
        dns.push(spawn_datanode(i).await);
    }
    let ec_wire: ozone_grpc_types::dn::v1::EcReplicationConfig =
        ozone_types::EcReplicationConfig::rs(3, 2, 1024).into();
    let details: Vec<_> = dns
        .iter()
        .map(|d| datanode_details(&d.uuid, "127.0.0.1", d.addr.port() as u32))
        .collect();
    let (om_endpoint, _om) = spawn_om(PipelineConfig::new(details, ec_wire)).await;
    let gateway = Arc::new(Gateway::connect(om_endpoint, "us-east-1").await.unwrap());
    let gw_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", gw_listener.local_addr().unwrap());
    tokio::spawn(async move {
        serve(gateway, gw_listener).await.ok();
    });
    (base, dns)
}

#[tokio::test]
async fn s3_object_lifecycle_with_degraded_read() {
    let (base, dns) = spawn_stack().await;
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
    let url = format!("{base}/bucket1/dir/object.bin");

    // An object that spans multiple stripes (stripe = k*C = 3*1024 = 3072).
    let body = Bytes::from((0..8000u32).map(|i| (i % 256) as u8).collect::<Vec<u8>>());
    let want_etag = md5_hex(&body);

    // PUT.
    let (st, hdr, _) = http(
        &client,
        Method::PUT,
        url.clone(),
        Some("tester"),
        body.clone(),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "PUT status");
    assert_eq!(
        hdr.get(header::ETAG).unwrap().to_str().unwrap(),
        format!("\"{want_etag}\""),
        "PUT ETag"
    );

    // GET round-trips the exact bytes.
    let (st, hdr, got) = http(&client, Method::GET, url.clone(), Some("tester"), Bytes::new()).await;
    assert_eq!(st, StatusCode::OK, "GET status");
    assert_eq!(got, body, "GET body");
    assert_eq!(
        hdr.get(header::ETAG).unwrap().to_str().unwrap(),
        format!("\"{want_etag}\"")
    );

    // HEAD returns size + ETag, no body.
    let (st, hdr, got) =
        http(&client, Method::HEAD, url.clone(), Some("tester"), Bytes::new()).await;
    assert_eq!(st, StatusCode::OK, "HEAD status");
    assert_eq!(hdr.get(header::CONTENT_LENGTH).unwrap(), "8000");
    assert!(got.is_empty(), "HEAD has no body");

    // HEAD bucket exists.
    let (st, _, _) = http(
        &client,
        Method::HEAD,
        format!("{base}/bucket1"),
        Some("tester"),
        Bytes::new(),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "HEAD bucket");

    // GET a missing key -> 404 NoSuchKey.
    let (st, _, xml) = http(
        &client,
        Method::GET,
        format!("{base}/bucket1/missing"),
        Some("tester"),
        Bytes::new(),
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert!(
        String::from_utf8_lossy(&xml).contains("NoSuchKey"),
        "404 body should be an S3 error"
    );

    // DEGRADED READ: kill the datanode holding data shard slot 1, then GET. EC
    // must reconstruct shard 1 from the 4 survivors (2 data + 2 parity >= k=3).
    dns[0].handle.abort();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (st, _, got) = http(&client, Method::GET, url.clone(), Some("tester"), Bytes::new()).await;
    assert_eq!(st, StatusCode::OK, "degraded GET status");
    assert_eq!(got, body, "degraded GET must reconstruct the exact object");

    // DELETE, then GET -> 404.
    let (st, _, _) = http(
        &client,
        Method::DELETE,
        url.clone(),
        Some("tester"),
        Bytes::new(),
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT, "DELETE status");
    let (st, _, _) = http(&client, Method::GET, url.clone(), Some("tester"), Bytes::new()).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "GET after DELETE");

    // Cleanup.
    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

#[tokio::test]
async fn s3_list_objects_v2_prefix_and_delimiter() {
    let (base, dns) = spawn_stack().await;
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();

    for key in ["dir1/a.txt", "dir1/b.txt", "top.txt"] {
        let (st, _, _) = http(
            &client,
            Method::PUT,
            format!("{base}/bucket1/{key}"),
            Some("tester"),
            Bytes::from_static(b"x"),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "PUT {key}");
    }

    // Delimiter "/" folds dir1/ into a common prefix; top.txt stays a key.
    let (st, _, xml) = http(
        &client,
        Method::GET,
        format!("{base}/bucket1?list-type=2&delimiter=%2F"),
        Some("tester"),
        Bytes::new(),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let xml = String::from_utf8_lossy(&xml);
    assert!(xml.contains("<ListBucketResult"), "not a listing: {xml}");
    assert!(
        xml.contains("<CommonPrefixes><Prefix>dir1/</Prefix></CommonPrefixes>"),
        "expected folded dir1/: {xml}"
    );
    assert!(xml.contains("<Key>top.txt</Key>"), "expected top.txt: {xml}");
    assert!(
        !xml.contains("<Key>dir1/a.txt</Key>"),
        "delimiter should hide folded keys: {xml}"
    );

    // Prefix dir1/ (no delimiter) returns both nested keys.
    let (st, _, xml) = http(
        &client,
        Method::GET,
        format!("{base}/bucket1?list-type=2&prefix=dir1%2F"),
        Some("tester"),
        Bytes::new(),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let xml = String::from_utf8_lossy(&xml);
    assert!(xml.contains("<Key>dir1/a.txt</Key>"), "{xml}");
    assert!(xml.contains("<Key>dir1/b.txt</Key>"), "{xml}");
    assert!(!xml.contains("<Key>top.txt</Key>"), "{xml}");

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}
