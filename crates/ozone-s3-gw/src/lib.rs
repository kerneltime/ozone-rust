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
    let principal = req
        .headers()
        .get(PRINCIPAL_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous")
        .to_string();

    let (bucket, key) = split_path(&path);
    if bucket.is_empty() {
        return Err(GatewayError::BadRequest("missing bucket in path".into()));
    }

    match (&method, key.is_empty()) {
        (&Method::HEAD, true) => {
            gw.head_bucket(&bucket, &principal).await?;
            Ok(status_only(StatusCode::OK))
        }
        (&Method::PUT, false) => {
            let body = collect_body(req).await?;
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
