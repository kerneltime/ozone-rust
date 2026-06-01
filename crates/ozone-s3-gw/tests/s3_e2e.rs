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

    // Two parts, each spanning multiple EC stripes.
    let part1: Vec<u8> = (0..3000u32).map(|i| (i % 256) as u8).collect();
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
