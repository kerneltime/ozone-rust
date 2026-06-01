//! S3 gateway for Ozone OBS buckets.
//!
//! Translates the S3 REST API into OM metadata calls and erasure-coded chunk
//! I/O against the Rust datanodes. The HTTP surface is intentionally thin: it
//! parses the bucket/key out of the path, reads the proxy-attested principal
//! from the `x-auth-principal` header, dispatches to [`Gateway`], and renders
//! the result (or an S3-style XML error). All policy/security is the upstream
//! proxy's job; this gateway trusts the principal it is handed.
//!
//! # Implemented surface (this slice)
//! - `PUT  /{bucket}/{key}` — store an object (EC-encoded).
//! - `GET  /{bucket}/{key}` — read an object (degraded-read tolerant).
//! - `HEAD /{bucket}/{key}` — object metadata (size + ETag).
//! - `DELETE /{bucket}/{key}` — delete an object (idempotent).
//! - `HEAD /{bucket}` — bucket existence.
//!
//! Multipart upload and `ListObjectsV2` are not yet routed here.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod backend;

pub use backend::{Gateway, GatewayError};

use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

/// Header carrying the proxy-attested S3 principal (access key id).
const PRINCIPAL_HEADER: &str = "x-auth-principal";

/// Serve the S3 API on `listener` until the process is stopped. Each connection
/// is handled on its own task; handler errors become S3 XML error responses, so
/// the connection itself never fails from application errors.
pub async fn serve(gateway: Arc<Gateway>, listener: TcpListener) -> std::io::Result<()> {
    loop {
        let (stream, _peer) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let gw = gateway.clone();
        tokio::spawn(async move {
            let service = service_fn(move |req| handle(gw.clone(), req));
            if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                tracing::debug!("connection closed with error: {e}");
            }
        });
    }
}

/// Top-level connection handler: route, and turn any [`GatewayError`] into an
/// S3 error response (the service is infallible from hyper's perspective).
async fn handle(
    gw: Arc<Gateway>,
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    Ok(match route(gw, req).await {
        Ok(resp) => resp,
        Err(e) => error_response(e),
    })
}

/// Parse and dispatch one request.
async fn route(
    gw: Arc<Gateway>,
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, GatewayError> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let query = req.uri().query().map(|q| q.to_string());
    let principal = extract_principal(req.headers());
    let aws_chunked = is_aws_chunked(req.headers());

    let (bucket, key) = split_path(&path);
    if bucket.is_empty() {
        return Err(GatewayError::BadRequest("missing bucket in path".into()));
    }

    match (&method, key.is_empty()) {
        (&Method::HEAD, true) => {
            gw.head_bucket(&bucket, &principal).await?;
            Ok(status_only(StatusCode::OK))
        }
        (&Method::GET, true) => {
            let q = query.as_deref();
            let prefix = query_param(q, "prefix").unwrap_or_default();
            let delimiter = query_param(q, "delimiter").unwrap_or_default();
            let max_keys = query_param(q, "max-keys")
                .and_then(|v| v.parse().ok())
                .unwrap_or(1000u32);
            let token = query_param(q, "continuation-token").unwrap_or_default();
            let listing = gw
                .list_objects(&bucket, &principal, prefix, delimiter, max_keys, token)
                .await?;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(hyper::header::CONTENT_TYPE, "application/xml")
                .body(Full::new(Bytes::from(listing_xml(&listing))))
                .expect("valid response"))
        }
        (&Method::PUT, false) => {
            let raw = collect_body(req).await?;
            let body = if aws_chunked {
                decode_aws_chunked(&raw)?
            } else {
                raw
            };
            let etag = gw.put_object(&bucket, &key, &principal, body).await?;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(hyper::header::ETAG, quote(&etag))
                .body(Full::new(Bytes::new()))
                .expect("valid response"))
        }
        (&Method::GET, false) => {
            let (data, etag) = gw.get_object(&bucket, &key, &principal).await?;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(hyper::header::ETAG, quote(&etag))
                .header(hyper::header::CONTENT_LENGTH, data.len())
                .header(hyper::header::CONTENT_TYPE, "application/octet-stream")
                .body(Full::new(data))
                .expect("valid response"))
        }
        (&Method::HEAD, false) => {
            let (size, etag) = gw.head_object(&bucket, &key, &principal).await?;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(hyper::header::ETAG, quote(&etag))
                .header(hyper::header::CONTENT_LENGTH, size)
                .header(hyper::header::CONTENT_TYPE, "application/octet-stream")
                .body(Full::new(Bytes::new()))
                .expect("valid response"))
        }
        (&Method::DELETE, false) => {
            gw.delete_object(&bucket, &key, &principal).await?;
            Ok(status_only(StatusCode::NO_CONTENT))
        }
        _ => Err(GatewayError::BadRequest(format!(
            "unsupported operation: {method} {path}"
        ))),
    }
}

/// Split `/{bucket}/{key...}` into `(bucket, key)`. The key keeps any embedded
/// slashes; a bare `/{bucket}` yields an empty key.
fn split_path(path: &str) -> (String, String) {
    let trimmed = path.trim_start_matches('/');
    match trimmed.split_once('/') {
        Some((b, k)) => (b.to_string(), k.to_string()),
        None => (trimmed.to_string(), String::new()),
    }
}

async fn collect_body(req: Request<Incoming>) -> Result<Bytes, GatewayError> {
    req.into_body()
        .collect()
        .await
        .map(|c| c.to_bytes())
        .map_err(|e| GatewayError::BadRequest(format!("reading request body: {e}")))
}

fn status_only(status: StatusCode) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::new()))
        .expect("valid response")
}

/// Wrap an ETag value in the quotes S3 clients expect.
fn quote(etag: &str) -> String {
    format!("\"{etag}\"")
}

/// Render a [`GatewayError`] as an S3 `<Error>` XML body with the right status.
fn error_response(e: GatewayError) -> Response<Full<Bytes>> {
    let message = e.to_string();
    let (status, code) = match e {
        GatewayError::NoSuchKey => (StatusCode::NOT_FOUND, "NoSuchKey"),
        GatewayError::NoSuchBucket => (StatusCode::NOT_FOUND, "NoSuchBucket"),
        GatewayError::BadRequest(_) => (StatusCode::BAD_REQUEST, "InvalidRequest"),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "InternalError"),
    };
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{code}</Code><Message>{}</Message></Error>",
        xml_escape(&message)
    );
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .expect("valid response")
}

/// Minimal XML text escaping for error messages.
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Determine the request principal. Production traffic carries the proxy-
/// attested `x-auth-principal`; for direct testing with an S3 SDK/CLI we fall
/// back to the access key id parsed out of a SigV4 `Authorization` header —
/// WITHOUT verifying the signature, which is the upstream proxy's job. Absent
/// both, the principal is `anonymous`.
fn extract_principal(headers: &hyper::HeaderMap) -> String {
    if let Some(p) = headers.get(PRINCIPAL_HEADER).and_then(|v| v.to_str().ok()) {
        if !p.is_empty() {
            return p.to_string();
        }
    }
    if let Some(auth) = headers
        .get(hyper::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(pos) = auth.find("Credential=") {
            let akid = auth[pos + "Credential=".len()..]
                .split('/')
                .next()
                .unwrap_or("");
            if !akid.is_empty() {
                return akid.to_string();
            }
        }
    }
    "anonymous".to_string()
}

/// Find a query parameter by name and percent-decode its value.
fn query_param(query: Option<&str>, key: &str) -> Option<String> {
    let q = query?;
    q.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
        (k == key).then(|| pct_decode(v))
    })
}

/// Percent-decode a query value (`+` -> space, `%XX` -> byte).
fn pct_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hi = (bytes[i + 1] as char).to_digit(16);
                let lo = (bytes[i + 2] as char).to_digit(16);
                if let (Some(hi), Some(lo)) = (hi, lo) {
                    out.push((hi * 16 + lo) as u8);
                    i += 3;
                } else {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// True if the request body is SigV4 streaming (`aws-chunked`) framed.
fn is_aws_chunked(headers: &hyper::HeaderMap) -> bool {
    let streaming_sha = headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.starts_with("STREAMING"))
        .unwrap_or(false);
    let chunked_enc = headers
        .get(hyper::header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.contains("aws-chunked"))
        .unwrap_or(false);
    streaming_sha || chunked_enc
}

/// Decode an `aws-chunked` request body (SigV4 streaming payload). Strips the
/// per-chunk `<hex-size>;chunk-signature=...\r\n<data>\r\n` framing and the final
/// zero-size chunk plus any trailers. Chunk signatures are NOT verified — the
/// upstream proxy owns authentication; the gateway only de-frames the payload so
/// the stored object is the user's exact bytes.
fn decode_aws_chunked(raw: &[u8]) -> Result<Bytes, GatewayError> {
    let mut out = Vec::with_capacity(raw.len());
    let mut i = 0;
    loop {
        let Some(nl) = find_crlf(raw, i) else {
            return Err(GatewayError::BadRequest(
                "aws-chunked: missing chunk-size line".into(),
            ));
        };
        let header = &raw[i..nl];
        i = nl + 2;
        let size_end = header
            .iter()
            .position(|&b| b == b';')
            .unwrap_or(header.len());
        let size_str = std::str::from_utf8(&header[..size_end])
            .map_err(|_| GatewayError::BadRequest("aws-chunked: non-utf8 size".into()))?;
        let size = usize::from_str_radix(size_str.trim(), 16)
            .map_err(|_| GatewayError::BadRequest("aws-chunked: bad hex size".into()))?;
        if size == 0 {
            break; // terminator chunk; ignore any trailers
        }
        if i + size > raw.len() {
            return Err(GatewayError::BadRequest("aws-chunked: truncated chunk".into()));
        }
        out.extend_from_slice(&raw[i..i + size]);
        i += size;
        if raw.get(i..i + 2) == Some(b"\r\n") {
            i += 2; // CRLF after each data chunk
        }
    }
    Ok(Bytes::from(out))
}

/// Find the next CRLF at or after `from`, returning its start index.
fn find_crlf(raw: &[u8], from: usize) -> Option<usize> {
    raw.get(from..)?
        .windows(2)
        .position(|w| w == b"\r\n")
        .map(|p| from + p)
}

/// Render a [`Listing`](backend::Listing) as an S3 `ListBucketResult` document.
fn listing_xml(l: &backend::Listing) -> String {
    let mut s = String::new();
    s.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    s.push_str("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    s.push_str(&format!("<Name>{}</Name>", xml_escape(&l.name)));
    s.push_str(&format!("<Prefix>{}</Prefix>", xml_escape(&l.prefix)));
    if !l.delimiter.is_empty() {
        s.push_str(&format!("<Delimiter>{}</Delimiter>", xml_escape(&l.delimiter)));
    }
    s.push_str(&format!("<MaxKeys>{}</MaxKeys>", l.max_keys));
    s.push_str(&format!(
        "<KeyCount>{}</KeyCount>",
        l.contents.len() + l.common_prefixes.len()
    ));
    s.push_str(&format!("<IsTruncated>{}</IsTruncated>", l.is_truncated));
    if !l.next_continuation_token.is_empty() {
        s.push_str(&format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            xml_escape(&l.next_continuation_token)
        ));
    }
    for e in &l.contents {
        s.push_str("<Contents>");
        s.push_str(&format!("<Key>{}</Key>", xml_escape(&e.key)));
        s.push_str(&format!(
            "<LastModified>{}</LastModified>",
            iso8601_millis(e.last_modified_ms)
        ));
        s.push_str(&format!("<ETag>&quot;{}&quot;</ETag>", xml_escape(&e.etag)));
        s.push_str(&format!("<Size>{}</Size>", e.size));
        s.push_str("<StorageClass>STANDARD</StorageClass>");
        s.push_str("</Contents>");
    }
    for p in &l.common_prefixes {
        s.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            xml_escape(p)
        ));
    }
    s.push_str("</ListBucketResult>");
    s
}

/// Format epoch milliseconds as an ISO-8601 UTC timestamp (S3 `LastModified`).
/// Howard Hinnant's civil-from-days algorithm; no external date crate.
fn iso8601_millis(ms: u64) -> String {
    let secs = (ms / 1000) as i64;
    let millis = ms % 1000;
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (hh, mm, ss) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        year, m, d, hh, mm, ss, millis
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso8601_epoch_and_known_date() {
        assert_eq!(iso8601_millis(0), "1970-01-01T00:00:00.000Z");
        // 2021-01-01T00:00:00Z = 1609459200 s.
        assert_eq!(iso8601_millis(1_609_459_200_000), "2021-01-01T00:00:00.000Z");
        // With sub-second millis.
        assert_eq!(iso8601_millis(1_609_459_200_123), "2021-01-01T00:00:00.123Z");
    }

    #[test]
    fn pct_decode_basics() {
        assert_eq!(pct_decode("a%2Fb"), "a/b");
        assert_eq!(pct_decode("hello+world"), "hello world");
        assert_eq!(pct_decode("plain"), "plain");
    }

    #[test]
    fn query_param_extracts_and_decodes() {
        let q = Some("prefix=a%2Fb&delimiter=%2F&max-keys=10");
        assert_eq!(query_param(q, "prefix").as_deref(), Some("a/b"));
        assert_eq!(query_param(q, "delimiter").as_deref(), Some("/"));
        assert_eq!(query_param(q, "max-keys").as_deref(), Some("10"));
        assert_eq!(query_param(q, "absent"), None);
    }

    #[test]
    fn principal_prefers_header_then_sigv4() {
        let mut h = hyper::HeaderMap::new();
        assert_eq!(extract_principal(&h), "anonymous");
        h.insert(
            hyper::header::AUTHORIZATION,
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20230101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc"
                .parse()
                .unwrap(),
        );
        assert_eq!(extract_principal(&h), "AKIDEXAMPLE");
        h.insert(PRINCIPAL_HEADER, "proxied-user".parse().unwrap());
        assert_eq!(extract_principal(&h), "proxied-user");
    }

    #[test]
    fn aws_chunked_decodes_multiple_chunks() {
        let body = b"5;chunk-signature=a\r\nhello\r\n6;chunk-signature=b\r\nworld!\r\n0;chunk-signature=c\r\n\r\n";
        assert_eq!(&decode_aws_chunked(body).unwrap()[..], b"helloworld!");
    }

    #[test]
    fn aws_chunked_single_chunk() {
        let body = b"3;chunk-signature=x\r\nabc\r\n0;chunk-signature=y\r\n\r\n";
        assert_eq!(&decode_aws_chunked(body).unwrap()[..], b"abc");
    }

    #[test]
    fn aws_chunked_truncation_is_rejected() {
        let body = b"9;chunk-signature=a\r\nshort\r\n";
        assert!(decode_aws_chunked(body).is_err());
    }
}

