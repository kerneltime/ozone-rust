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

/// Recursively collect every stored chunk file (those under a `chunks/`
/// directory) below `dir`. Used to inject on-disk shard corruption.
fn collect_chunk_files(dir: &std::path::Path, out: &mut Vec<PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                collect_chunk_files(&p, out);
            } else if p.components().any(|c| c.as_os_str() == "chunks") {
                out.push(p);
            }
        }
    }
}

/// Stand up 5 datanodes (RS-3-2, small 1 KiB cells so objects span several
/// stripes) + a FakeOm pipeline + the gateway. Returns the gateway base URL and
/// the datanode handles (for fault injection + cleanup).
async fn spawn_stack() -> (String, Vec<Datanode>) {
    spawn_stack_with_block_size(None).await
}

/// Like [`spawn_stack`] but optionally forces a small EC block-group size so a
/// modest object spans several block groups (the multi-block path).
async fn spawn_stack_with_block_size(block_group_size: Option<usize>) -> (String, Vec<Datanode>) {
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
    let mut gw = Gateway::connect(om_endpoint, "us-east-1").await.unwrap();
    if let Some(n) = block_group_size {
        gw.set_block_group_size(n);
    }
    let gateway = Arc::new(gw);
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
async fn s3_corrupted_shard_is_detected_and_reconstructed() {
    let (base, dns) = spawn_stack().await;
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
    let url = format!("{base}/bucket1/corrupt.bin");

    // Object spanning several stripes so the data shard on datanode 0 holds real
    // bytes (datanode 0 = data shard slot 1, per spawn_stack's pipeline order).
    let body = Bytes::from((0..8000u32).map(|i| (i % 256) as u8).collect::<Vec<u8>>());
    let (st, _, _) = http(&client, Method::PUT, url.clone(), Some("tester"), body.clone()).await;
    assert_eq!(st, StatusCode::OK, "PUT");

    // Corrupt one byte of that datanode's stored shard, in place on disk.
    let mut files = Vec::new();
    collect_chunk_files(&dns[0].dir, &mut files);
    assert!(!files.is_empty(), "expected a stored chunk under datanode 0");
    let mut bytes = std::fs::read(&files[0]).unwrap();
    assert!(!bytes.is_empty(), "shard file is non-empty");
    bytes[0] ^= 0xFF;
    std::fs::write(&files[0], &bytes).unwrap();

    // GET still returns the exact object: the datanode detects the bad checksum
    // (DataLoss), the gateway treats that shard as missing, and EC reconstructs
    // it from the survivors. Without read-path verification this GET would return
    // corrupt bytes (the corrupted data shard would be reassembled directly).
    let (st, _, got) = http(&client, Method::GET, url.clone(), Some("tester"), Bytes::new()).await;
    assert_eq!(st, StatusCode::OK, "GET after shard corruption");
    assert_eq!(got, body, "corrupted shard must be detected and reconstructed");

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

#[tokio::test]
async fn s3_empty_object_round_trip() {
    let (base, dns) = spawn_stack().await;
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
    let url = format!("{base}/bucket1/empty.bin");

    // A 0-byte object stores zero block groups; its ETag is the MD5 of empty.
    let (st, hdr, _) = http(&client, Method::PUT, url.clone(), Some("tester"), Bytes::new()).await;
    assert_eq!(st, StatusCode::OK, "empty PUT");
    assert_eq!(
        hdr.get(header::ETAG).unwrap().to_str().unwrap(),
        format!("\"{}\"", md5_hex(b"")),
    );

    let (st, hdr, got) = http(&client, Method::GET, url.clone(), Some("tester"), Bytes::new()).await;
    assert_eq!(st, StatusCode::OK, "empty GET");
    assert_eq!(hdr.get(header::CONTENT_LENGTH).unwrap(), "0");
    assert!(got.is_empty(), "empty object has no body");

    let (st, hdr, _) = http(&client, Method::HEAD, url.clone(), Some("tester"), Bytes::new()).await;
    assert_eq!(st, StatusCode::OK, "empty HEAD");
    assert_eq!(hdr.get(header::CONTENT_LENGTH).unwrap(), "0");

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

#[tokio::test]
async fn s3_multi_block_object_round_trip_and_degraded() {
    // Small block groups (2 KiB) so a 9 KB object spans 5 groups (4 full + 1).
    let (base, dns) = spawn_stack_with_block_size(Some(2048)).await;
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
    let url = format!("{base}/bucket1/multiblock.bin");

    let body = Bytes::from((0..9000u32).map(|i| (i % 256) as u8).collect::<Vec<u8>>());
    let want_etag = md5_hex(&body);

    let (st, hdr, _) = http(&client, Method::PUT, url.clone(), Some("tester"), body.clone()).await;
    assert_eq!(st, StatusCode::OK, "multi-block PUT");
    assert_eq!(
        hdr.get(header::ETAG).unwrap().to_str().unwrap(),
        format!("\"{want_etag}\"")
    );

    // GET concatenates every block group back to the exact object.
    let (st, _, got) = http(&client, Method::GET, url.clone(), Some("tester"), Bytes::new()).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(got, body, "multi-block GET must reassemble every block group");

    // HEAD reports the full object size.
    let (st, hdr, _) = http(&client, Method::HEAD, url.clone(), Some("tester"), Bytes::new()).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(hdr.get(header::CONTENT_LENGTH).unwrap(), "9000");

    // Prove the object was actually split, not stored as one oversized block:
    // 9000 bytes / 2048 = 5 block groups, so each datanode holds 5 shard files
    // (its one replica slot per group). The old single-block path would store 1.
    let mut files = Vec::new();
    collect_chunk_files(&dns[1].dir, &mut files);
    assert_eq!(files.len(), 5, "expected 5 block groups per datanode, got {}", files.len());

    // A range straddling block-group boundaries returns the right slice.
    let req = Request::builder()
        .method(Method::GET)
        .uri(url.clone())
        .header("x-auth-principal", "tester")
        .header(header::RANGE, "bytes=2000-4100")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = client.request(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    let ranged = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(ranged.as_ref(), &body[2000..=4100], "cross-block range slice");

    // Degraded read across ALL block groups: killing the datanode holding data
    // shard slot 1 removes that shard from every group; each must reconstruct.
    dns[0].handle.abort();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (st, _, got) = http(&client, Method::GET, url.clone(), Some("tester"), Bytes::new()).await;
    assert_eq!(st, StatusCode::OK, "degraded multi-block GET");
    assert_eq!(got, body, "every block group must reconstruct from survivors");

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

#[tokio::test]
async fn s3_multipart_part_spans_blocks() {
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};

    // 2 MiB block groups: the 5 MiB first part spans three groups.
    let (base, dns) = spawn_stack_with_block_size(Some(2 * 1024 * 1024)).await;
    let s3 = s3_client(&base);

    let create = s3
        .create_multipart_upload()
        .bucket("bucket1")
        .key("mp-mb.bin")
        .send()
        .await
        .expect("create");
    let uid = create.upload_id().unwrap().to_string();
    let part1: Vec<u8> = (0..5 * 1024 * 1024u32).map(|i| (i % 256) as u8).collect();
    let part2: Vec<u8> = (0..1000u32).map(|i| ((i + 7) % 256) as u8).collect();
    let mut completed = Vec::new();
    for (n, b) in [(1i32, &part1), (2i32, &part2)] {
        let up = s3
            .upload_part()
            .bucket("bucket1")
            .key("mp-mb.bin")
            .upload_id(&uid)
            .part_number(n)
            .body(ByteStream::from(b.clone()))
            .send()
            .await
            .unwrap_or_else(|e| panic!("upload {n}: {e:?}"));
        completed.push(
            CompletedPart::builder()
                .part_number(n)
                .e_tag(up.e_tag().unwrap_or_default())
                .build(),
        );
    }
    s3.complete_multipart_upload()
        .bucket("bucket1")
        .key("mp-mb.bin")
        .upload_id(&uid)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed)).build())
        .send()
        .await
        .expect("complete multi-block multipart");

    let got = s3.get_object().bucket("bucket1").key("mp-mb.bin").send().await.unwrap();
    let bytes = got.body.collect().await.unwrap().into_bytes();
    let mut expected = part1.clone();
    expected.extend_from_slice(&part2);
    assert_eq!(
        bytes.len(),
        expected.len(),
        "reassembled length (part1 spans 3 block groups)"
    );
    assert_eq!(bytes.as_ref(), &expected[..], "multi-block multipart reassembles exactly");

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

/// ListObjectsV2 pagination via the real SDK: max-keys caps the page, the
/// response is truncated with a continuation token, and following the token walks
/// the remaining keys with no overlap or gaps.
#[tokio::test]
async fn s3_list_objects_v2_pagination() {
    use aws_sdk_s3::primitives::ByteStream;

    let (base, dns) = spawn_stack().await;
    let s3 = s3_client(&base);
    for k in ["k1", "k2", "k3", "k4", "k5"] {
        s3.put_object()
            .bucket("bucket1")
            .key(k)
            .body(ByteStream::from(vec![1u8]))
            .send()
            .await
            .unwrap_or_else(|e| panic!("put {k}: {e:?}"));
    }

    let page_keys = |out: &aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output| {
        out.contents()
            .iter()
            .filter_map(|o| o.key().map(String::from))
            .collect::<Vec<_>>()
    };

    let p1 = s3
        .list_objects_v2()
        .bucket("bucket1")
        .max_keys(2)
        .send()
        .await
        .expect("page 1");
    assert_eq!(page_keys(&p1), vec!["k1", "k2"], "page 1 capped at max-keys");
    assert_eq!(p1.is_truncated(), Some(true), "page 1 truncated");
    let t1 = p1.next_continuation_token().expect("token 1").to_string();

    let p2 = s3
        .list_objects_v2()
        .bucket("bucket1")
        .max_keys(2)
        .continuation_token(t1)
        .send()
        .await
        .expect("page 2");
    assert_eq!(page_keys(&p2), vec!["k3", "k4"], "page 2 follows the token");
    assert_eq!(p2.is_truncated(), Some(true));
    let t2 = p2.next_continuation_token().expect("token 2").to_string();

    let p3 = s3
        .list_objects_v2()
        .bucket("bucket1")
        .max_keys(2)
        .continuation_token(t2)
        .send()
        .await
        .expect("page 3");
    assert_eq!(page_keys(&p3), vec!["k5"], "final page");
    assert_eq!(p3.is_truncated(), Some(false), "final page not truncated");
    assert!(p3.next_continuation_token().is_none(), "no token past the end");

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

/// Build a real AWS S3 SDK client pointed at the gateway. Path-style addressing
/// (no virtual-host buckets), static creds (the gateway extracts the access key
/// id from the SigV4 Authorization header as the principal — it does not verify
/// the signature).
fn s3_client(base: &str) -> aws_sdk_s3::Client {
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
    let creds = Credentials::new("AKIDCOMPLIANCE", "secretkey", None, None, "ozone-test");
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(base.to_string())
        .credentials_provider(creds)
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(conf)
}

/// Compliance check against the real AWS S3 SDK: the SDK signs with SigV4 and
/// frames the PUT body as `aws-chunked`, exercising the gateway's signature
/// principal extraction and chunk de-framing, then the full object lifecycle
/// and listing through a genuine S3 client.
#[tokio::test]
async fn s3_sdk_compliance_suite() {
    use aws_sdk_s3::primitives::ByteStream;

    let (base, dns) = spawn_stack().await;
    let s3 = s3_client(&base);

    // HEAD bucket.
    s3.head_bucket()
        .bucket("bucket1")
        .send()
        .await
        .expect("head_bucket");

    // PUT an object that spans several EC stripes (chunked + signed by the SDK).
    let body: Vec<u8> = (0..5000u32).map(|i| (i % 256) as u8).collect();
    let put = s3
        .put_object()
        .bucket("bucket1")
        .key("docs/report.bin")
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .expect("put_object");
    let etag = put.e_tag().unwrap_or_default().to_string();
    assert!(!etag.is_empty(), "PUT returned an ETag");

    // GET round-trips the exact bytes and the same ETag.
    let got = s3
        .get_object()
        .bucket("bucket1")
        .key("docs/report.bin")
        .send()
        .await
        .expect("get_object");
    assert_eq!(got.e_tag().unwrap_or_default(), etag, "GET ETag matches PUT");
    let got_bytes = got.body.collect().await.expect("collect body").into_bytes();
    assert_eq!(got_bytes.as_ref(), &body[..], "GET body matches PUT");

    // HEAD reports the size.
    let head = s3
        .head_object()
        .bucket("bucket1")
        .key("docs/report.bin")
        .send()
        .await
        .expect("head_object");
    assert_eq!(head.content_length(), Some(5000));

    // ListObjectsV2: prefix returns the keys; delimiter folds the prefix.
    s3.put_object()
        .bucket("bucket1")
        .key("docs/notes.txt")
        .body(ByteStream::from(vec![1u8, 2, 3]))
        .send()
        .await
        .expect("put notes");
    let list = s3
        .list_objects_v2()
        .bucket("bucket1")
        .prefix("docs/")
        .send()
        .await
        .expect("list_objects_v2");
    let keys: Vec<String> = list
        .contents()
        .iter()
        .filter_map(|o| o.key().map(String::from))
        .collect();
    assert!(keys.contains(&"docs/report.bin".to_string()), "keys: {keys:?}");
    assert!(keys.contains(&"docs/notes.txt".to_string()), "keys: {keys:?}");

    let dlist = s3
        .list_objects_v2()
        .bucket("bucket1")
        .delimiter("/")
        .send()
        .await
        .expect("list delimiter");
    let prefixes: Vec<String> = dlist
        .common_prefixes()
        .iter()
        .filter_map(|c| c.prefix().map(String::from))
        .collect();
    assert!(prefixes.contains(&"docs/".to_string()), "prefixes: {prefixes:?}");

    // DELETE then GET -> typed NoSuchKey error.
    s3.delete_object()
        .bucket("bucket1")
        .key("docs/report.bin")
        .send()
        .await
        .expect("delete_object");
    let err = s3
        .get_object()
        .bucket("bucket1")
        .key("docs/report.bin")
        .send()
        .await
        .expect_err("GET after DELETE must fail");
    let svc = err.into_service_error();
    assert!(svc.is_no_such_key(), "expected NoSuchKey, got: {svc:?}");

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

/// Multipart upload through the real AWS SDK: create, two upload-parts,
/// complete, then GET the reassembled object. Each part is its own EC block
/// group; the completed object is their concatenation, and the multipart ETag
/// carries the `-N` part-count suffix.
#[tokio::test]
async fn s3_sdk_multipart_upload() {
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};

    let (base, dns) = spawn_stack().await;
    let s3 = s3_client(&base);

    let create = s3
        .create_multipart_upload()
        .bucket("bucket1")
        .key("big.bin")
        .send()
        .await
        .expect("create_multipart_upload");
    let upload_id = create.upload_id().expect("upload id").to_string();

    // Part 1 is non-last, so it must meet the S3 5 MiB minimum; part 2 (the last
    // part) may be small. Both span multiple EC stripes.
    let part1: Vec<u8> = (0..5 * 1024 * 1024u32).map(|i| (i % 256) as u8).collect();
    let part2: Vec<u8> = (0..2000u32).map(|i| ((i + 99) % 256) as u8).collect();

    let mut completed_parts = Vec::new();
    for (n, body) in [(1i32, &part1), (2i32, &part2)] {
        let up = s3
            .upload_part()
            .bucket("bucket1")
            .key("big.bin")
            .upload_id(&upload_id)
            .part_number(n)
            .body(ByteStream::from(body.clone()))
            .send()
            .await
            .unwrap_or_else(|e| panic!("upload_part {n}: {e:?}"));
        completed_parts.push(
            CompletedPart::builder()
                .part_number(n)
                .e_tag(up.e_tag().unwrap_or_default())
                .build(),
        );
    }

    let completed = CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();
    let comp = s3
        .complete_multipart_upload()
        .bucket("bucket1")
        .key("big.bin")
        .upload_id(&upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect("complete_multipart_upload");
    let etag = comp.e_tag().unwrap_or_default().to_string();
    assert!(etag.contains("-2"), "multipart ETag must carry -N suffix: {etag}");

    // GET returns part1 ++ part2 exactly, with the multipart ETag.
    let got = s3
        .get_object()
        .bucket("bucket1")
        .key("big.bin")
        .send()
        .await
        .expect("get multipart object");
    assert_eq!(got.e_tag().unwrap_or_default(), etag);
    let got_bytes = got.body.collect().await.expect("collect").into_bytes();
    let mut expected = part1.clone();
    expected.extend_from_slice(&part2);
    assert_eq!(
        got_bytes.as_ref(),
        &expected[..],
        "multipart object must reassemble part1 ++ part2"
    );

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

/// Multipart limits: a non-last part below 5 MiB fails Complete (EntityTooSmall),
/// a single small part is fine (last part exempt), and part numbers outside
/// 1..=10000 are rejected at UploadPart.
#[tokio::test]
async fn s3_multipart_part_size_and_number_limits() {
    use aws_sdk_s3::error::ProvideErrorMetadata;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};

    let (base, dns) = spawn_stack().await;
    let s3 = s3_client(&base);
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();

    // Two small parts: the first (non-last) is below 5 MiB -> EntityTooSmall.
    let create = s3.create_multipart_upload().bucket("bucket1").key("small.bin").send().await.unwrap();
    let uid = create.upload_id().unwrap().to_string();
    let mut completed = Vec::new();
    for n in [1i32, 2] {
        let up = s3
            .upload_part()
            .bucket("bucket1")
            .key("small.bin")
            .upload_id(&uid)
            .part_number(n)
            .body(ByteStream::from(vec![7u8; 1000]))
            .send()
            .await
            .unwrap_or_else(|e| panic!("upload {n}: {e:?}"));
        completed.push(
            CompletedPart::builder()
                .part_number(n)
                .e_tag(up.e_tag().unwrap_or_default())
                .build(),
        );
    }
    let err = s3
        .complete_multipart_upload()
        .bucket("bucket1")
        .key("small.bin")
        .upload_id(&uid)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed)).build())
        .send()
        .await
        .expect_err("a non-last part below 5 MiB must fail");
    assert_eq!(err.code(), Some("InvalidRequest"), "{err:?}");
    s3.abort_multipart_upload().bucket("bucket1").key("small.bin").upload_id(&uid).send().await.ok();

    // A single small part is the last part, so the minimum does not apply.
    let create = s3.create_multipart_upload().bucket("bucket1").key("one.bin").send().await.unwrap();
    let uid = create.upload_id().unwrap().to_string();
    let up = s3
        .upload_part()
        .bucket("bucket1")
        .key("one.bin")
        .upload_id(&uid)
        .part_number(1)
        .body(ByteStream::from(vec![7u8; 1000]))
        .send()
        .await
        .unwrap();
    s3.complete_multipart_upload()
        .bucket("bucket1")
        .key("one.bin")
        .upload_id(&uid)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(vec![CompletedPart::builder()
                    .part_number(1)
                    .e_tag(up.e_tag().unwrap_or_default())
                    .build()]))
                .build(),
        )
        .send()
        .await
        .expect("a single small (last) part completes");

    // Part numbers outside 1..=10000 are rejected (raw HTTP; the SDK clamps).
    let create = s3.create_multipart_upload().bucket("bucket1").key("range.bin").send().await.unwrap();
    let uid = create.upload_id().unwrap().to_string();
    for pn in ["0", "10001"] {
        let req = Request::builder()
            .method(Method::PUT)
            .uri(format!("{base}/bucket1/range.bin?partNumber={pn}&uploadId={uid}"))
            .header("x-auth-principal", "t")
            .body(Full::new(Bytes::from_static(b"x")))
            .unwrap();
        let st = client.request(req).await.unwrap().status();
        assert_eq!(st, StatusCode::BAD_REQUEST, "partNumber {pn} must be rejected");
    }

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

/// Multipart Complete must reject non-ascending or duplicate part numbers
/// (S3 `InvalidPartOrder`) rather than silently re-sorting. Uses raw HTTP for
/// Complete so the part order on the wire is controlled exactly.
#[tokio::test]
async fn s3_multipart_rejects_out_of_order_and_duplicate_parts() {
    use aws_sdk_s3::primitives::ByteStream;

    let (base, dns) = spawn_stack().await;
    let s3 = s3_client(&base);
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();

    let create = s3
        .create_multipart_upload()
        .bucket("bucket1")
        .key("mp.bin")
        .send()
        .await
        .expect("create");
    let upload_id = create.upload_id().unwrap().to_string();
    for (n, body) in [(1i32, vec![1u8; 3000]), (2i32, vec![2u8; 2000])] {
        s3.upload_part()
            .bucket("bucket1")
            .key("mp.bin")
            .upload_id(&upload_id)
            .part_number(n)
            .body(ByteStream::from(body))
            .send()
            .await
            .unwrap_or_else(|e| panic!("upload {n}: {e:?}"));
    }

    let complete = |parts: &[u32]| {
        let mut b = String::from("<CompleteMultipartUpload>");
        for p in parts {
            b.push_str(&format!("<Part><PartNumber>{p}</PartNumber></Part>"));
        }
        b.push_str("</CompleteMultipartUpload>");
        b
    };
    let url = format!("{base}/bucket1/mp.bin?uploadId={upload_id}");

    // Out-of-order [2,1] -> 400 (order is checked before part size).
    let (st, _, _) = http(&client, Method::POST, url.clone(), Some("t"), Bytes::from(complete(&[2, 1]))).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "out-of-order parts must be rejected");
    // Duplicate [1,1] -> 400.
    let (st, _, _) = http(&client, Method::POST, url.clone(), Some("t"), Bytes::from(complete(&[1, 1]))).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "duplicate parts must be rejected");
    // (Successful ascending completion is covered by s3_sdk_multipart_upload,
    // which uses a 5 MiB first part; these tiny parts would hit EntityTooSmall.)
    s3.abort_multipart_upload()
        .bucket("bucket1")
        .key("mp.bin")
        .upload_id(&upload_id)
        .send()
        .await
        .expect("abort");

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

/// CopyObject and ranged GET through the real AWS SDK.
#[tokio::test]
async fn s3_sdk_copy_object_and_range_get() {
    use aws_sdk_s3::primitives::ByteStream;

    let (base, dns) = spawn_stack().await;
    let s3 = s3_client(&base);

    let body: Vec<u8> = (0..1000u32).map(|i| (i % 256) as u8).collect();
    s3.put_object()
        .bucket("bucket1")
        .key("src.bin")
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .expect("put source");

    // Server-side copy, then GET the destination -> same bytes.
    s3.copy_object()
        .bucket("bucket1")
        .key("dest.bin")
        .copy_source("bucket1/src.bin")
        .send()
        .await
        .expect("copy_object");
    let dest = s3
        .get_object()
        .bucket("bucket1")
        .key("dest.bin")
        .send()
        .await
        .expect("get dest");
    let dest_bytes = dest.body.collect().await.unwrap().into_bytes();
    assert_eq!(dest_bytes.as_ref(), &body[..], "copied object matches source");

    // Ranged GET: bytes 100-199 inclusive -> 100 bytes, 206 semantics.
    let ranged = s3
        .get_object()
        .bucket("bucket1")
        .key("src.bin")
        .range("bytes=100-199")
        .send()
        .await
        .expect("ranged get");
    assert_eq!(ranged.content_length(), Some(100));
    let ranged_bytes = ranged.body.collect().await.unwrap().into_bytes();
    assert_eq!(ranged_bytes.as_ref(), &body[100..200], "range bytes match");

    // Unsatisfiable range (past EOF) -> 416 with Content-Range: bytes */total,
    // never a silent full-body 200. Raw HTTP so we can read the exact status.
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("{base}/bucket1/src.bin"))
        .header("x-auth-principal", "tester")
        .header(header::RANGE, "bytes=2000-3000")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = client.request(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::RANGE_NOT_SATISFIABLE,
        "out-of-range GET must be 416"
    );
    assert_eq!(
        resp.headers()
            .get(header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok()),
        Some("bytes */1000"),
        "416 must carry Content-Range: bytes */total"
    );

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

/// CopyObject metadata directives via the real SDK: REPLACE swaps in new
/// metadata, COPY (default) clones the source's, self-copy with REPLACE updates
/// in place, and a pure-COPY self-copy is rejected.
#[tokio::test]
async fn s3_sdk_copy_object_metadata_directive() {
    use aws_sdk_s3::error::ProvideErrorMetadata;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::MetadataDirective;

    let (base, dns) = spawn_stack().await;
    let s3 = s3_client(&base);

    s3.put_object()
        .bucket("bucket1")
        .key("orig.bin")
        .body(ByteStream::from(vec![1u8; 10]))
        .content_type("text/plain")
        .metadata("k", "v1")
        .send()
        .await
        .expect("put orig");

    // REPLACE to a new key: new content-type + metadata, not the source's.
    s3.copy_object()
        .bucket("bucket1")
        .key("copy.bin")
        .copy_source("bucket1/orig.bin")
        .metadata_directive(MetadataDirective::Replace)
        .content_type("application/json")
        .metadata("k", "v2")
        .send()
        .await
        .expect("copy REPLACE");
    let got = s3.get_object().bucket("bucket1").key("copy.bin").send().await.unwrap();
    assert_eq!(got.content_type(), Some("application/json"), "REPLACE content-type");
    assert_eq!(
        got.metadata().and_then(|m| m.get("k")).map(String::as_str),
        Some("v2"),
        "REPLACE metadata"
    );

    // Default COPY clones the source metadata.
    s3.copy_object()
        .bucket("bucket1")
        .key("copy2.bin")
        .copy_source("bucket1/orig.bin")
        .send()
        .await
        .expect("copy COPY");
    let got2 = s3.get_object().bucket("bucket1").key("copy2.bin").send().await.unwrap();
    assert_eq!(got2.content_type(), Some("text/plain"), "COPY clones content-type");
    assert_eq!(
        got2.metadata().and_then(|m| m.get("k")).map(String::as_str),
        Some("v1"),
        "COPY clones metadata"
    );

    // Self-copy with REPLACE updates metadata in place.
    s3.copy_object()
        .bucket("bucket1")
        .key("orig.bin")
        .copy_source("bucket1/orig.bin")
        .metadata_directive(MetadataDirective::Replace)
        .content_type("text/markdown")
        .send()
        .await
        .expect("self-copy REPLACE");
    let got3 = s3.get_object().bucket("bucket1").key("orig.bin").send().await.unwrap();
    assert_eq!(got3.content_type(), Some("text/markdown"), "self-copy REPLACE updates in place");

    // Pure-COPY self-copy is rejected.
    let err = s3
        .copy_object()
        .bucket("bucket1")
        .key("orig.bin")
        .copy_source("bucket1/orig.bin")
        .send()
        .await
        .expect_err("self-copy without REPLACE must fail");
    assert_eq!(err.code(), Some("InvalidRequest"), "{err:?}");

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

/// Batch delete, GetBucketLocation, and ListMultipartUploads via the real SDK.
#[tokio::test]
async fn s3_sdk_batch_delete_location_list_uploads() {
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{Delete, ObjectIdentifier};

    let (base, dns) = spawn_stack().await;
    let s3 = s3_client(&base);

    // GetBucketLocation succeeds.
    s3.get_bucket_location()
        .bucket("bucket1")
        .send()
        .await
        .expect("get_bucket_location");

    // Two objects, then a batch delete of both.
    for k in ["d1.txt", "d2.txt"] {
        s3.put_object()
            .bucket("bucket1")
            .key(k)
            .body(ByteStream::from(vec![1u8, 2, 3]))
            .send()
            .await
            .unwrap_or_else(|e| panic!("put {k}: {e:?}"));
    }
    let del = Delete::builder()
        .objects(ObjectIdentifier::builder().key("d1.txt").build().unwrap())
        .objects(ObjectIdentifier::builder().key("d2.txt").build().unwrap())
        .build()
        .unwrap();
    let res = s3
        .delete_objects()
        .bucket("bucket1")
        .delete(del)
        .send()
        .await
        .expect("delete_objects");
    assert_eq!(res.deleted().len(), 2, "both keys reported deleted");
    assert!(
        s3.get_object()
            .bucket("bucket1")
            .key("d1.txt")
            .send()
            .await
            .is_err(),
        "deleted object is gone"
    );

    // ListMultipartUploads shows an in-flight upload.
    let cmu = s3
        .create_multipart_upload()
        .bucket("bucket1")
        .key("mpu.bin")
        .send()
        .await
        .expect("create_multipart_upload");
    let upload_id = cmu.upload_id().unwrap().to_string();
    let listed = s3
        .list_multipart_uploads()
        .bucket("bucket1")
        .send()
        .await
        .expect("list_multipart_uploads");
    assert!(
        listed
            .uploads()
            .iter()
            .any(|u| u.upload_id() == Some(upload_id.as_str()) && u.key() == Some("mpu.bin")),
        "in-flight upload should be listed"
    );
    s3.abort_multipart_upload()
        .bucket("bucket1")
        .key("mpu.bin")
        .upload_id(&upload_id)
        .send()
        .await
        .expect("abort_multipart_upload");

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

/// DeleteObjects quiet mode via the real SDK: successful keys are suppressed in
/// the response (only errors would appear), and the keys are actually deleted.
#[tokio::test]
async fn s3_sdk_delete_objects_quiet_mode() {
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{Delete, ObjectIdentifier};

    let (base, dns) = spawn_stack().await;
    let s3 = s3_client(&base);
    for k in ["q1.txt", "q2.txt"] {
        s3.put_object()
            .bucket("bucket1")
            .key(k)
            .body(ByteStream::from(vec![1u8]))
            .send()
            .await
            .unwrap_or_else(|e| panic!("put {k}: {e:?}"));
    }

    let del = Delete::builder()
        .objects(ObjectIdentifier::builder().key("q1.txt").build().unwrap())
        .objects(ObjectIdentifier::builder().key("q2.txt").build().unwrap())
        .quiet(true)
        .build()
        .unwrap();
    let res = s3
        .delete_objects()
        .bucket("bucket1")
        .delete(del)
        .send()
        .await
        .expect("delete quiet");
    assert!(res.deleted().is_empty(), "quiet mode suppresses Deleted entries");
    assert!(res.errors().is_empty(), "no per-key errors");
    // The keys are really gone.
    assert!(
        s3.get_object().bucket("bucket1").key("q1.txt").send().await.is_err(),
        "q1 deleted"
    );

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

/// Bucket lifecycle via the real SDK: CreateBucket, ListBuckets, DeleteBucket.
#[tokio::test]
async fn s3_sdk_bucket_lifecycle() {
    let (base, dns) = spawn_stack().await;
    let s3 = s3_client(&base);

    s3.create_bucket()
        .bucket("alpha")
        .send()
        .await
        .expect("create alpha");
    s3.create_bucket()
        .bucket("beta")
        .send()
        .await
        .expect("create beta");

    let names = |out: &aws_sdk_s3::operation::list_buckets::ListBucketsOutput| -> Vec<String> {
        out.buckets()
            .iter()
            .filter_map(|b| b.name().map(String::from))
            .collect()
    };

    let list = s3.list_buckets().send().await.expect("list_buckets");
    let n = names(&list);
    assert!(n.contains(&"alpha".to_string()), "buckets: {n:?}");
    assert!(n.contains(&"beta".to_string()), "buckets: {n:?}");

    s3.delete_bucket()
        .bucket("beta")
        .send()
        .await
        .expect("delete beta");
    let list2 = s3.list_buckets().send().await.expect("list_buckets 2");
    let n2 = names(&list2);
    assert!(n2.contains(&"alpha".to_string()), "buckets: {n2:?}");
    assert!(!n2.contains(&"beta".to_string()), "beta should be gone: {n2:?}");

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

/// Content-Type and x-amz-meta-* round-trip via the real SDK. The PUT is also
/// SigV4-chunked-signed by the SDK, so this additionally covers chunked signing.
#[tokio::test]
async fn s3_sdk_content_type_and_user_metadata() {
    use aws_sdk_s3::primitives::ByteStream;

    let (base, dns) = spawn_stack().await;
    let s3 = s3_client(&base);

    let body = b"some text content".to_vec();
    s3.put_object()
        .bucket("bucket1")
        .key("doc.txt")
        .body(ByteStream::from(body.clone()))
        .content_type("text/plain; charset=utf-8")
        .metadata("author", "ritesh")
        .metadata("project", "ozone-rust")
        .send()
        .await
        .expect("put with metadata");

    let got = s3
        .get_object()
        .bucket("bucket1")
        .key("doc.txt")
        .send()
        .await
        .expect("get");
    assert_eq!(got.content_type(), Some("text/plain; charset=utf-8"));
    let meta = got.metadata().expect("metadata present");
    assert_eq!(meta.get("author").map(String::as_str), Some("ritesh"));
    assert_eq!(meta.get("project").map(String::as_str), Some("ozone-rust"));
    let got_bytes = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(got_bytes.as_ref(), &body[..]);

    let head = s3
        .head_object()
        .bucket("bucket1")
        .key("doc.txt")
        .send()
        .await
        .expect("head");
    assert_eq!(head.content_type(), Some("text/plain; charset=utf-8"));
    assert_eq!(
        head.metadata().and_then(|m| m.get("author")).map(String::as_str),
        Some("ritesh")
    );

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

/// Conditional requests via the real SDK: If-Match / If-None-Match on GET, and
/// If-None-Match: * create-only on PUT.
#[tokio::test]
async fn s3_sdk_conditional_requests() {
    use aws_sdk_s3::primitives::ByteStream;

    let (base, dns) = spawn_stack().await;
    let s3 = s3_client(&base);

    let put = s3
        .put_object()
        .bucket("bucket1")
        .key("c.bin")
        .body(ByteStream::from(vec![1u8; 10]))
        .send()
        .await
        .expect("put");
    let etag = put.e_tag().unwrap().to_string();

    // If-None-Match with the current ETag -> 304 (surfaced as an error, not 200).
    assert!(
        s3.get_object()
            .bucket("bucket1")
            .key("c.bin")
            .if_none_match(&etag)
            .send()
            .await
            .is_err(),
        "If-None-Match with current ETag must not return the object"
    );

    // If-Match with the current ETag -> 200.
    let ok = s3
        .get_object()
        .bucket("bucket1")
        .key("c.bin")
        .if_match(&etag)
        .send()
        .await
        .expect("If-Match with current ETag should pass");
    let _ = ok.body.collect().await;

    // If-Match with a wrong ETag -> 412.
    assert!(
        s3.get_object()
            .bucket("bucket1")
            .key("c.bin")
            .if_match("\"deadbeef\"")
            .send()
            .await
            .is_err(),
        "If-Match with a wrong ETag must fail"
    );

    // PUT If-None-Match: * on an existing key -> 412 (create-only).
    assert!(
        s3.put_object()
            .bucket("bucket1")
            .key("c.bin")
            .if_none_match("*")
            .body(ByteStream::from(vec![9u8; 5]))
            .send()
            .await
            .is_err(),
        "create-only on an existing key must fail"
    );

    // PUT If-None-Match: * on a new key -> succeeds.
    s3.put_object()
        .bucket("bucket1")
        .key("new.bin")
        .if_none_match("*")
        .body(ByteStream::from(vec![7u8; 5]))
        .send()
        .await
        .expect("create-only on a new key should succeed");

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

/// Object tagging via the real SDK: read the (empty) initial set, put a two-tag
/// set, read it back, confirm tags do not leak into user metadata or the body,
/// overwrite (replace, not merge), delete, and finally that tagging a missing key
/// surfaces NoSuchKey.
#[tokio::test]
async fn s3_sdk_object_tagging() {
    use aws_sdk_s3::error::ProvideErrorMetadata;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{Tag, Tagging};

    let (base, dns) = spawn_stack().await;
    let s3 = s3_client(&base);

    s3.put_object()
        .bucket("bucket1")
        .key("tagged.bin")
        .body(ByteStream::from(vec![1u8, 2, 3, 4]))
        .metadata("author", "ritesh")
        .send()
        .await
        .expect("put object");

    // A fresh object has no tags.
    let empty = s3
        .get_object_tagging()
        .bucket("bucket1")
        .key("tagged.bin")
        .send()
        .await
        .expect("get_object_tagging (empty)");
    assert!(empty.tag_set().is_empty(), "new object has no tags");

    let pairs = |out: &aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput| {
        out.tag_set()
            .iter()
            .map(|t| (t.key().to_string(), t.value().to_string()))
            .collect::<Vec<_>>()
    };

    // Put a two-tag set; the gateway returns it sorted by key (env, team).
    let tagging = Tagging::builder()
        .set_tag_set(Some(vec![
            Tag::builder().key("team").value("storage").build().unwrap(),
            Tag::builder().key("env").value("prod").build().unwrap(),
        ]))
        .build()
        .unwrap();
    s3.put_object_tagging()
        .bucket("bucket1")
        .key("tagged.bin")
        .tagging(tagging)
        .send()
        .await
        .expect("put_object_tagging");
    let got = s3
        .get_object_tagging()
        .bucket("bucket1")
        .key("tagged.bin")
        .send()
        .await
        .expect("get_object_tagging");
    assert_eq!(
        pairs(&got),
        vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "storage".to_string()),
        ]
    );

    // Tags must NOT leak into the object's user metadata or change its body.
    let obj = s3
        .get_object()
        .bucket("bucket1")
        .key("tagged.bin")
        .send()
        .await
        .expect("get object");
    let meta = obj.metadata().expect("user metadata");
    assert_eq!(meta.get("author").map(String::as_str), Some("ritesh"));
    assert!(
        meta.keys().all(|k| !k.contains("tag")),
        "tags must not surface as user metadata: {meta:?}"
    );
    let body = obj.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), &[1u8, 2, 3, 4]);

    // Overwrite with a single tag -> the set is replaced, not merged.
    let replaced = Tagging::builder()
        .set_tag_set(Some(vec![Tag::builder()
            .key("env")
            .value("staging")
            .build()
            .unwrap()]))
        .build()
        .unwrap();
    s3.put_object_tagging()
        .bucket("bucket1")
        .key("tagged.bin")
        .tagging(replaced)
        .send()
        .await
        .expect("replace tags");
    let after = s3
        .get_object_tagging()
        .bucket("bucket1")
        .key("tagged.bin")
        .send()
        .await
        .expect("get after replace");
    assert_eq!(pairs(&after), vec![("env".to_string(), "staging".to_string())]);

    // DeleteObjectTagging -> empty set.
    s3.delete_object_tagging()
        .bucket("bucket1")
        .key("tagged.bin")
        .send()
        .await
        .expect("delete_object_tagging");
    let cleared = s3
        .get_object_tagging()
        .bucket("bucket1")
        .key("tagged.bin")
        .send()
        .await
        .expect("get after delete");
    assert!(cleared.tag_set().is_empty(), "tags cleared after delete");

    // Tagging a missing key surfaces NoSuchKey.
    let err = s3
        .put_object_tagging()
        .bucket("bucket1")
        .key("ghost.bin")
        .tagging(
            Tagging::builder()
                .set_tag_set(Some(vec![Tag::builder()
                    .key("a")
                    .value("b")
                    .build()
                    .unwrap()]))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect_err("tagging a missing key must fail");
    assert_eq!(err.code(), Some("NoSuchKey"), "expected NoSuchKey: {err:?}");

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

/// PUT-time tagging via the real SDK: the `x-amz-tagging` header (SDK
/// `.tagging("k=v&...")`) sets tags at object-creation time, readable back through
/// GetObjectTagging — without a separate PutObjectTagging round trip.
#[tokio::test]
async fn s3_sdk_put_object_with_tagging_header() {
    use aws_sdk_s3::primitives::ByteStream;

    let (base, dns) = spawn_stack().await;
    let s3 = s3_client(&base);

    s3.put_object()
        .bucket("bucket1")
        .key("tagged-at-put.bin")
        .body(ByteStream::from(vec![5u8; 32]))
        .tagging("env=prod&team=storage")
        .metadata("author", "ritesh")
        .send()
        .await
        .expect("put with tagging header");

    let got = s3
        .get_object_tagging()
        .bucket("bucket1")
        .key("tagged-at-put.bin")
        .send()
        .await
        .expect("get_object_tagging");
    let pairs: Vec<(String, String)> = got
        .tag_set()
        .iter()
        .map(|t| (t.key().to_string(), t.value().to_string()))
        .collect();
    assert_eq!(
        pairs,
        vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "storage".to_string()),
        ]
    );

    // PUT-time tags coexist with user metadata and never leak into it.
    let obj = s3
        .get_object()
        .bucket("bucket1")
        .key("tagged-at-put.bin")
        .send()
        .await
        .expect("get object");
    let meta = obj.metadata().expect("user metadata");
    assert_eq!(meta.get("author").map(String::as_str), Some("ritesh"));
    assert!(
        meta.keys().all(|k| !k.contains("tag")),
        "tags must not surface as user metadata: {meta:?}"
    );

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

/// GetObjectAttributes via the real SDK: request ETag + ObjectSize and confirm
/// the unquoted ETag, the byte size, and a populated Last-Modified. Also asserts
/// the `x-amz-object-attributes` selector header is required (400 without it).
#[tokio::test]
async fn s3_sdk_get_object_attributes() {
    use aws_sdk_s3::error::ProvideErrorMetadata;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::ObjectAttributes;

    let (base, dns) = spawn_stack().await;
    let s3 = s3_client(&base);

    let body: Vec<u8> = (0..4096u32).map(|i| (i % 256) as u8).collect();
    let put = s3
        .put_object()
        .bucket("bucket1")
        .key("attr.bin")
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .expect("put");
    // The ETag header is quoted; GetObjectAttributes returns it unquoted.
    let etag_unquoted = put.e_tag().unwrap().trim_matches('"').to_string();

    let attrs = s3
        .get_object_attributes()
        .bucket("bucket1")
        .key("attr.bin")
        .object_attributes(ObjectAttributes::Etag)
        .object_attributes(ObjectAttributes::ObjectSize)
        .send()
        .await
        .expect("get_object_attributes");
    assert_eq!(attrs.e_tag(), Some(etag_unquoted.as_str()), "unquoted ETag");
    assert_eq!(attrs.object_size(), Some(4096), "object size in bytes");
    assert!(attrs.last_modified().is_some(), "Last-Modified header present");

    // The attribute selector header is required (the SDK always sends it, so this
    // path is checked with a raw request).
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("{base}/bucket1/attr.bin?attributes"))
        .header("x-auth-principal", "tester")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = client.request(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "?attributes without the selector header must be rejected"
    );

    // A missing key surfaces NoSuchKey.
    let err = s3
        .get_object_attributes()
        .bucket("bucket1")
        .key("ghost.bin")
        .object_attributes(ObjectAttributes::ObjectSize)
        .send()
        .await
        .expect_err("attributes of a missing key must fail");
    assert_eq!(err.code(), Some("NoSuchKey"), "expected NoSuchKey: {err:?}");

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

/// Date-based conditional GETs (If-Modified-Since / If-Unmodified-Since) and the
/// Last-Modified response header. Raw HTTP so exact statuses are observable; the
/// object's mtime is FakeOm's fixed 2021-01-01.
#[tokio::test]
async fn s3_date_conditional_requests() {
    use aws_sdk_s3::primitives::ByteStream;

    let (base, dns) = spawn_stack().await;
    let s3 = s3_client(&base);
    s3.put_object()
        .bucket("bucket1")
        .key("d.bin")
        .body(ByteStream::from(vec![1u8; 10]))
        .send()
        .await
        .expect("put");

    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
    let url = format!("{base}/bucket1/d.bin");
    let after = "Sat, 01 Jan 2022 00:00:00 GMT"; // after the object's 2021 mtime
    let before = "Wed, 01 Jan 2020 00:00:00 GMT"; // before it

    let cases = [
        ("if-modified-since", after, StatusCode::NOT_MODIFIED),
        ("if-modified-since", before, StatusCode::OK),
        ("if-unmodified-since", before, StatusCode::PRECONDITION_FAILED),
        ("if-unmodified-since", after, StatusCode::OK),
    ];
    for (h, v, want) in cases {
        let req = Request::builder()
            .method(Method::GET)
            .uri(url.clone())
            .header("x-auth-principal", "t")
            .header(h, v)
            .body(Full::new(Bytes::new()))
            .unwrap();
        let st = client.request(req).await.unwrap().status();
        assert_eq!(st, want, "GET {h}: {v}");
    }

    // HEAD carries a correct Last-Modified header.
    let req = Request::builder()
        .method(Method::HEAD)
        .uri(url.clone())
        .header("x-auth-principal", "t")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = client.request(req).await.unwrap();
    assert_eq!(
        resp.headers()
            .get(header::LAST_MODIFIED)
            .and_then(|v| v.to_str().ok()),
        Some("Fri, 01 Jan 2021 00:00:00 GMT"),
        "Last-Modified header"
    );

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

/// Frame `data` as a SigV4 streaming (`aws-chunked`) signed body: each chunk is
/// `<hexsize>;chunk-signature=<64 hex>\r\n<data>\r\n`, terminated by a zero
/// chunk. The signatures are placeholders -- the gateway de-frames without
/// verifying them (verification is the upstream proxy's job).
fn aws_chunk_sign(data: &[u8], chunk_size: usize) -> Bytes {
    let sig = "0".repeat(64);
    let mut out = Vec::new();
    for chunk in data.chunks(chunk_size) {
        out.extend_from_slice(format!("{:x};chunk-signature={sig}\r\n", chunk.len()).as_bytes());
        out.extend_from_slice(chunk);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(format!("0;chunk-signature={sig}\r\n\r\n").as_bytes());
    Bytes::from(out)
}

/// A controlled, multi-chunk SigV4-signed (`aws-chunked`) PUT. Frames a 5 KB
/// object into ~8 signed chunks by hand and asserts the gateway de-frames and
/// stores the exact decoded bytes.
#[tokio::test]
async fn signed_chunked_put_multiple_chunks() {
    let (base, dns) = spawn_stack().await;
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();

    let data: Vec<u8> = (0..5000u32).map(|i| (i % 256) as u8).collect();
    let framed = aws_chunk_sign(&data, 700); // 8 chunks (7 of 700 + 1 of 100)

    let req = Request::builder()
        .method(Method::PUT)
        .uri(format!("{base}/bucket1/signed.bin"))
        .header("x-auth-principal", "tester")
        .header("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
        .header("content-encoding", "aws-chunked")
        .body(Full::new(framed))
        .unwrap();
    let resp = client.request(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "signed chunked PUT");

    let (st, _, got) = http(
        &client,
        Method::GET,
        format!("{base}/bucket1/signed.bin"),
        Some("tester"),
        Bytes::new(),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        got.as_ref(),
        &data[..],
        "multi-chunk signed PUT must store the exact decoded bytes"
    );

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}

/// A large (1 MiB) PUT through the real SDK, which splits the body into many
/// SigV4-signed aws-chunked frames -- exercising the multi-chunk signed path
/// end to end through erasure coding.
#[tokio::test]
async fn s3_sdk_large_chunk_signed_put() {
    use aws_sdk_s3::primitives::ByteStream;

    let (base, dns) = spawn_stack().await;
    let s3 = s3_client(&base);

    let body: Vec<u8> = (0..1024u32 * 1024).map(|i| (i % 251) as u8).collect();
    s3.put_object()
        .bucket("bucket1")
        .key("big-signed.bin")
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .expect("large chunk-signed put");

    let got = s3
        .get_object()
        .bucket("bucket1")
        .key("big-signed.bin")
        .send()
        .await
        .expect("get large object");
    let got_bytes = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(got_bytes.len(), body.len(), "length match");
    assert_eq!(
        got_bytes.as_ref(),
        &body[..],
        "large chunk-signed object must round-trip exactly through EC"
    );

    for d in &dns {
        d.handle.abort();
        tokio::fs::remove_dir_all(&d.dir).await.ok();
    }
}
