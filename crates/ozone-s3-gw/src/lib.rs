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

use std::collections::HashMap;
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
    let copy_source = req
        .headers()
        .get("x-amz-copy-source")
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    // CopyObject directives: REPLACE swaps in the request's metadata/tags, COPY
    // (the default when the header is absent) clones the source's.
    let replace_metadata = req
        .headers()
        .get("x-amz-metadata-directive")
        .and_then(|v| v.to_str().ok())
        .map(|d| d.eq_ignore_ascii_case("REPLACE"))
        .unwrap_or(false);
    let replace_tags = req
        .headers()
        .get("x-amz-tagging-directive")
        .and_then(|v| v.to_str().ok())
        .map(|d| d.eq_ignore_ascii_case("REPLACE"))
        .unwrap_or(false);
    let range_header = req
        .headers()
        .get(hyper::header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let user_metadata = collect_user_metadata(req.headers());
    let if_match = req
        .headers()
        .get(hyper::header::IF_MATCH)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let if_none_match = req
        .headers()
        .get(hyper::header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let if_modified_since = req
        .headers()
        .get(hyper::header::IF_MODIFIED_SINCE)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let if_unmodified_since = req
        .headers()
        .get(hyper::header::IF_UNMODIFIED_SINCE)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    // PUT-time object tags (`x-amz-tagging: k1=v1&k2=v2`) and the attribute
    // selector for GetObjectAttributes (`x-amz-object-attributes: ETag,...`).
    let tagging_header = req
        .headers()
        .get("x-amz-tagging")
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    // The AWS SDK emits one `x-amz-object-attributes` header line per requested
    // attribute (the list-header serialization appends rather than joins), so
    // gather ALL values, not just the first; other clients may send a single
    // comma-separated line, which the parser also handles.
    let object_attributes_header = {
        let joined = req
            .headers()
            .get_all("x-amz-object-attributes")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .collect::<Vec<_>>()
            .join(",");
        (!joined.is_empty()).then_some(joined)
    };

    let (bucket, key) = split_path(&path);
    if bucket.is_empty() {
        // GET / -> ListBuckets; no other verb is valid without a bucket.
        if method == Method::GET {
            let buckets = gw.list_buckets(&principal).await?;
            return Ok(xml_ok(list_buckets_xml(&buckets)));
        }
        return Err(GatewayError::BadRequest("missing bucket in path".into()));
    }

    // Multipart-upload subresources are selected by query string and take
    // precedence over the plain object verbs for the same path.
    if !key.is_empty() {
        let q = query.as_deref();
        // Object tagging subresource (?tagging), dispatched ahead of the plain
        // object verbs. PutObjectTagging replaces the full tag set; GET reads it;
        // DELETE clears it (modeled as a put with an empty set).
        if query_param(q, "tagging").is_some() {
            if method == Method::PUT {
                let raw = collect_body(req).await?;
                let body = if aws_chunked {
                    decode_aws_chunked(&raw)?
                } else {
                    raw
                };
                let tags = parse_tagging(&body)?;
                gw.put_object_tagging(&bucket, &key, &principal, tags).await?;
                return Ok(status_only(StatusCode::OK));
            } else if method == Method::GET {
                let tags = gw.get_object_tagging(&bucket, &key, &principal).await?;
                return Ok(xml_ok(tagging_xml(&tags)));
            } else if method == Method::DELETE {
                gw.put_object_tagging(&bucket, &key, &principal, Vec::new())
                    .await?;
                return Ok(status_only(StatusCode::NO_CONTENT));
            }
            return Err(GatewayError::BadRequest("unsupported tagging operation".into()));
        }
        // GetObjectAttributes (?attributes). The `x-amz-object-attributes` header
        // is required and selects which attributes appear in the response; we can
        // serve ETag and ObjectSize (StorageClass is omitted for the default
        // class, and additional checksums / part records are not stored).
        if query_param(q, "attributes").is_some() {
            if method != Method::GET {
                return Err(GatewayError::BadRequest("unsupported attributes operation".into()));
            }
            let requested = object_attributes_header
                .as_deref()
                .map(parse_object_attributes_header)
                .unwrap_or_default();
            if requested.is_empty() {
                return Err(GatewayError::BadRequest(
                    "missing x-amz-object-attributes header".into(),
                ));
            }
            let (etag, size, mod_ms) = gw.object_attributes(&bucket, &key, &principal).await?;
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header(hyper::header::CONTENT_TYPE, "application/xml")
                .header(hyper::header::LAST_MODIFIED, http_date(mod_ms))
                .body(Full::new(Bytes::from(object_attributes_xml(
                    &requested, &etag, size,
                ))))
                .expect("valid response"));
        }
        if method == Method::POST && query_param(q, "uploads").is_some() {
            let upload_id = gw.initiate_multipart(&bucket, &key, &principal).await?;
            return Ok(xml_ok(initiate_mpu_xml(&bucket, &key, &upload_id)));
        }
        if let Some(upload_id) = query_param(q, "uploadId") {
            if method == Method::PUT {
                let part_number = query_param(q, "partNumber")
                    .and_then(|v| v.parse::<u32>().ok())
                    .ok_or_else(|| {
                        GatewayError::BadRequest("missing or invalid partNumber".into())
                    })?;
                let raw = collect_body(req).await?;
                let body = if aws_chunked {
                    decode_aws_chunked(&raw)?
                } else {
                    raw
                };
                let etag = gw
                    .upload_part(&bucket, &key, &principal, &upload_id, part_number, body)
                    .await?;
                return Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header(hyper::header::ETAG, quote(&etag))
                    .body(Full::new(Bytes::new()))
                    .expect("valid response"));
            } else if method == Method::POST {
                let raw = collect_body(req).await?;
                let body = if aws_chunked {
                    decode_aws_chunked(&raw)?
                } else {
                    raw
                };
                let part_numbers = parse_complete_parts(&body)?;
                let etag = gw
                    .complete_multipart(&bucket, &key, &principal, &upload_id, &part_numbers)
                    .await?;
                return Ok(xml_ok(complete_mpu_xml(&bucket, &key, &etag)));
            } else if method == Method::DELETE {
                gw.abort_multipart(&bucket, &key, &principal, &upload_id)
                    .await?;
                return Ok(status_only(StatusCode::NO_CONTENT));
            } else if method == Method::GET {
                let parts = gw.list_parts(&bucket, &key, &principal, &upload_id).await?;
                return Ok(xml_ok(list_parts_xml(&bucket, &key, &upload_id, &parts)));
            }
            return Err(GatewayError::BadRequest("unsupported multipart operation".into()));
        }
    }

    // Bucket-level subresources (query-string dispatched), taking precedence
    // over plain bucket verbs.
    if key.is_empty() {
        let q = query.as_deref();
        if method == Method::POST && query_param(q, "delete").is_some() {
            let raw = collect_body(req).await?;
            let body = if aws_chunked {
                decode_aws_chunked(&raw)?
            } else {
                raw
            };
            let (keys, quiet) = parse_delete_request(&body)?;
            if keys.len() > 1000 {
                return Err(GatewayError::BadRequest(
                    "a delete request can contain at most 1000 keys".into(),
                ));
            }
            let results = gw.delete_objects(&bucket, &keys, &principal).await;
            return Ok(xml_ok(delete_result_xml(&results, quiet)));
        }
        if method == Method::GET && query_param(q, "location").is_some() {
            return Ok(xml_ok(bucket_location_xml(&gw.region)));
        }
        if method == Method::GET && query_param(q, "uploads").is_some() {
            let uploads = gw.list_multipart_uploads(&bucket, &principal).await?;
            return Ok(xml_ok(list_mpu_xml(&bucket, &uploads)));
        }
    }

    match (&method, key.is_empty()) {
        (&Method::HEAD, true) => {
            gw.head_bucket(&bucket, &principal).await?;
            Ok(status_only(StatusCode::OK))
        }
        (&Method::PUT, true) => {
            gw.create_bucket(&bucket, &principal).await?;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(hyper::header::LOCATION, format!("/{bucket}"))
                .body(Full::new(Bytes::new()))
                .expect("valid response"))
        }
        (&Method::DELETE, true) => {
            gw.delete_bucket(&bucket, &principal).await?;
            Ok(status_only(StatusCode::NO_CONTENT))
        }
        (&Method::GET, true) => {
            let q = query.as_deref();
            let prefix = query_param(q, "prefix").unwrap_or_default();
            let delimiter = query_param(q, "delimiter").unwrap_or_default();
            // max-keys: default 1000, clamp above 1000 (S3 caps, not errors), but
            // a non-integer value is a 400 (S3 InvalidArgument).
            let max_keys = match query_param(q, "max-keys") {
                None => 1000u32,
                Some(v) => match v.trim().parse::<u32>() {
                    Ok(n) => n.min(1000),
                    Err(_) => return Err(GatewayError::BadRequest("invalid max-keys".into())),
                },
            };
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
            // If-None-Match: * -> create-only: fail if the object already exists.
            if if_none_match.as_deref() == Some("*")
                && gw.head_object(&bucket, &key, &principal).await.is_ok()
            {
                return Ok(precondition_failed_response());
            }
            // PutObject with x-amz-copy-source is a server-side CopyObject.
            if let Some(source) = &copy_source {
                let (src_bucket, src_key) = parse_copy_source(source)?;
                // Self-copy is illegal unless metadata is being replaced (the
                // canonical "update metadata in place" idiom).
                if src_bucket == bucket && src_key == key && !replace_metadata {
                    return Err(GatewayError::BadRequest(
                        "copy destination is the same as the source without replacing metadata"
                            .into(),
                    ));
                }
                let tags = if replace_tags {
                    match &tagging_header {
                        Some(h) => parse_tagging_header(h)?,
                        None => Vec::new(),
                    }
                } else {
                    Vec::new()
                };
                let directives = backend::CopyDirectives {
                    replace_metadata,
                    metadata: user_metadata.clone(),
                    replace_tags,
                    tags,
                };
                let (etag, _size) = gw
                    .copy_object(&bucket, &key, &src_bucket, &src_key, &principal, directives)
                    .await?;
                return Ok(xml_ok(copy_object_xml(&etag)));
            }
            let raw = collect_body(req).await?;
            let body = if aws_chunked {
                decode_aws_chunked(&raw)?
            } else {
                raw
            };
            let tags = match &tagging_header {
                Some(h) => parse_tagging_header(h)?,
                None => Vec::new(),
            };
            let etag = gw
                .put_object(&bucket, &key, &principal, body, user_metadata, tags)
                .await?;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(hyper::header::ETAG, quote(&etag))
                .body(Full::new(Bytes::new()))
                .expect("valid response"))
        }
        (&Method::GET, false) => {
            // Conditional GET: evaluate preconditions against the ETag and
            // last-modified time without reading data (an extra metadata lookup,
            // only when some conditional header is present).
            if if_match.is_some()
                || if_none_match.is_some()
                || if_modified_since.is_some()
                || if_unmodified_since.is_some()
            {
                let (_, etag, _, mod_ms) = gw.head_object(&bucket, &key, &principal).await?;
                if let Some(status) = precondition_status(
                    if_match.as_deref(),
                    if_none_match.as_deref(),
                    if_modified_since.as_deref(),
                    if_unmodified_since.as_deref(),
                    &etag,
                    mod_ms,
                ) {
                    return Ok(if status == StatusCode::NOT_MODIFIED {
                        status_only(status)
                    } else {
                        precondition_failed_response()
                    });
                }
            }
            let (data, etag, metadata, mod_ms) = gw.get_object(&bucket, &key, &principal).await?;
            // Range request -> 206 Partial Content with Content-Range. The
            // object is decoded in full and sliced (range-aware EC reads are a
            // later optimization).
            if let Some(range) = &range_header {
                match parse_range(range, data.len()) {
                    RangeSpec::Satisfiable(start, end) => {
                        let total = data.len();
                        let slice = data.slice(start..end + 1);
                        return Ok(apply_object_metadata(
                            Response::builder()
                                .status(StatusCode::PARTIAL_CONTENT)
                                .header(hyper::header::ETAG, quote(&etag))
                                .header(hyper::header::LAST_MODIFIED, http_date(mod_ms))
                                .header(
                                    hyper::header::CONTENT_RANGE,
                                    format!("bytes {start}-{end}/{total}"),
                                )
                                .header(hyper::header::CONTENT_LENGTH, slice.len()),
                            &metadata,
                        )
                        .body(Full::new(slice))
                        .expect("valid response"));
                    }
                    // Valid byte-range syntax but out of bounds -> 416, never a
                    // silent full-body 200.
                    RangeSpec::Unsatisfiable => {
                        return Ok(range_not_satisfiable_response(data.len()));
                    }
                    // Not a usable byte range -> fall through to the full object.
                    RangeSpec::Whole => {}
                }
            }
            Ok(apply_object_metadata(
                Response::builder()
                    .status(StatusCode::OK)
                    .header(hyper::header::ETAG, quote(&etag))
                    .header(hyper::header::LAST_MODIFIED, http_date(mod_ms))
                    .header(hyper::header::CONTENT_LENGTH, data.len()),
                &metadata,
            )
            .body(Full::new(data))
            .expect("valid response"))
        }
        (&Method::HEAD, false) => {
            let (size, etag, metadata, mod_ms) = gw.head_object(&bucket, &key, &principal).await?;
            if let Some(status) = precondition_status(
                if_match.as_deref(),
                if_none_match.as_deref(),
                if_modified_since.as_deref(),
                if_unmodified_since.as_deref(),
                &etag,
                mod_ms,
            ) {
                return Ok(if status == StatusCode::NOT_MODIFIED {
                    status_only(status)
                } else {
                    precondition_failed_response()
                });
            }
            Ok(apply_object_metadata(
                Response::builder()
                    .status(StatusCode::OK)
                    .header(hyper::header::ETAG, quote(&etag))
                    .header(hyper::header::LAST_MODIFIED, http_date(mod_ms))
                    .header(hyper::header::CONTENT_LENGTH, size),
                &metadata,
            )
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
    let code = e.s3_code();
    let status = match e {
        GatewayError::NoSuchKey | GatewayError::NoSuchBucket | GatewayError::NoSuchUpload => {
            StatusCode::NOT_FOUND
        }
        GatewayError::BadRequest(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
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

/// Metadata key under which the gateway stores an object's Content-Type.
const CONTENT_TYPE_META_KEY: &str = "content-type";

/// Collect the object metadata to persist on a PUT: Content-Type and any
/// `x-amz-meta-*` user metadata headers (lowercased header names).
fn collect_user_metadata(headers: &hyper::HeaderMap) -> HashMap<String, String> {
    let mut m = HashMap::new();
    if let Some(ct) = headers
        .get(hyper::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
    {
        m.insert(CONTENT_TYPE_META_KEY.to_string(), ct.to_string());
    }
    for (name, value) in headers.iter() {
        let n = name.as_str();
        if n.starts_with("x-amz-meta-") {
            if let Ok(v) = value.to_str() {
                m.insert(n.to_string(), v.to_string());
            }
        }
    }
    m
}

/// Apply an object's stored metadata to a response: set Content-Type (defaulting
/// to `application/octet-stream`) and re-emit `x-amz-meta-*` headers. The `ETAG`
/// metadata entry is handled separately, so it is skipped here.
fn apply_object_metadata(
    mut builder: hyper::http::response::Builder,
    metadata: &HashMap<String, String>,
) -> hyper::http::response::Builder {
    let content_type = metadata
        .get(CONTENT_TYPE_META_KEY)
        .map(String::as_str)
        .unwrap_or("application/octet-stream");
    builder = builder.header(hyper::header::CONTENT_TYPE, content_type);
    for (k, v) in metadata {
        if k.starts_with("x-amz-meta-") {
            builder = builder.header(k.as_str(), v.as_str());
        }
    }
    builder
}

/// Evaluate the read preconditions against an object's (unquoted) ETag and
/// last-modified time, following RFC 7232 precedence: If-Match takes priority
/// over If-Unmodified-Since, and If-None-Match over If-Modified-Since. Returns
/// `Some(412)` if If-Match / If-Unmodified-Since fails, `Some(304)` if
/// If-None-Match / If-Modified-Since indicates not-modified, else `None`
/// (proceed). An unparseable date header is ignored. This is for GET/HEAD only
/// (a matched If-None-Match yields 304, not 412).
fn precondition_status(
    if_match: Option<&str>,
    if_none_match: Option<&str>,
    if_modified_since: Option<&str>,
    if_unmodified_since: Option<&str>,
    etag: &str,
    last_modified_ms: u64,
) -> Option<StatusCode> {
    let quoted = format!("\"{etag}\"");
    let modified_secs = last_modified_ms / 1000;
    // If-Match wins over If-Unmodified-Since when both are present.
    if let Some(im) = if_match {
        if !etag_list_matches(im, &quoted) {
            return Some(StatusCode::PRECONDITION_FAILED);
        }
    } else if let Some(ius) = if_unmodified_since {
        if let Some(threshold) = parse_http_date(ius) {
            // Modified strictly after the threshold -> precondition fails.
            if modified_secs > threshold {
                return Some(StatusCode::PRECONDITION_FAILED);
            }
        }
    }
    // If-None-Match wins over If-Modified-Since when both are present.
    if let Some(inm) = if_none_match {
        if etag_list_matches(inm, &quoted) {
            return Some(StatusCode::NOT_MODIFIED);
        }
    } else if let Some(ims) = if_modified_since {
        if let Some(threshold) = parse_http_date(ims) {
            // Not modified since the threshold -> 304.
            if modified_secs <= threshold {
                return Some(StatusCode::NOT_MODIFIED);
            }
        }
    }
    None
}

/// True if a `If-(None-)Match` header value matches `quoted_etag`: either `*`,
/// or a comma-separated list containing the (optionally weak `W/`-prefixed) tag.
fn etag_list_matches(header: &str, quoted_etag: &str) -> bool {
    let header = header.trim();
    if header == "*" {
        return true;
    }
    header
        .split(',')
        .map(str::trim)
        .any(|t| t == quoted_etag || t.strip_prefix("W/").map(str::trim) == Some(quoted_etag))
}

/// A 412 Precondition Failed response with an S3 error body.
fn precondition_failed_response() -> Response<Full<Bytes>> {
    let xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<Error><Code>PreconditionFailed</Code>\
<Message>At least one of the preconditions you specified did not hold.</Message></Error>";
    Response::builder()
        .status(StatusCode::PRECONDITION_FAILED)
        .header(hyper::header::CONTENT_TYPE, "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .expect("valid response")
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

/// 200 response with an XML body.
fn xml_ok(xml: String) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .header(hyper::header::CONTENT_TYPE, "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .expect("valid response")
}

/// `InitiateMultipartUploadResult` XML.
fn initiate_mpu_xml(bucket: &str, key: &str, upload_id: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
<Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId>\
</InitiateMultipartUploadResult>",
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(upload_id)
    )
}

/// `CompleteMultipartUploadResult` XML.
fn complete_mpu_xml(bucket: &str, key: &str, etag: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
<Location>/{}/{}</Location><Bucket>{}</Bucket><Key>{}</Key><ETag>&quot;{}&quot;</ETag>\
</CompleteMultipartUploadResult>",
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(etag)
    )
}

/// `ListPartsResult` XML.
fn list_parts_xml(
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: &[backend::PartSummary],
) -> String {
    let mut s = String::new();
    s.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    s.push_str("<ListPartsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    s.push_str(&format!("<Bucket>{}</Bucket>", xml_escape(bucket)));
    s.push_str(&format!("<Key>{}</Key>", xml_escape(key)));
    s.push_str(&format!("<UploadId>{}</UploadId>", xml_escape(upload_id)));
    s.push_str("<IsTruncated>false</IsTruncated>");
    for (pn, etag, size) in parts {
        s.push_str("<Part>");
        s.push_str(&format!("<PartNumber>{pn}</PartNumber>"));
        s.push_str(&format!("<ETag>&quot;{}&quot;</ETag>", xml_escape(etag)));
        s.push_str(&format!("<Size>{size}</Size>"));
        s.push_str("</Part>");
    }
    s.push_str("</ListPartsResult>");
    s
}

/// Extract the ordered `<PartNumber>` values from a `CompleteMultipartUpload`
/// request body. The ETags in the body are not required — the gateway holds the
/// authoritative part records — so only the ordering is read here.
fn parse_complete_parts(body: &[u8]) -> Result<Vec<u32>, GatewayError> {
    let s = String::from_utf8_lossy(body);
    let mut parts = Vec::new();
    let mut rest: &str = s.as_ref();
    while let Some(start) = rest.find("<PartNumber>") {
        let after = &rest[start + "<PartNumber>".len()..];
        let end = after.find("</PartNumber>").ok_or_else(|| {
            GatewayError::BadRequest("malformed Part in complete request".into())
        })?;
        let n: u32 = after[..end]
            .trim()
            .parse()
            .map_err(|_| GatewayError::BadRequest("invalid PartNumber".into()))?;
        parts.push(n);
        rest = &after[end..];
    }
    if parts.is_empty() {
        return Err(GatewayError::BadRequest("no parts in complete request".into()));
    }
    Ok(parts)
}

/// Parse an `x-amz-copy-source` header (`/bucket/key` or `bucket/key`, possibly
/// percent-encoded, with an optional `?versionId=...` suffix) into
/// `(bucket, key)`.
fn parse_copy_source(header: &str) -> Result<(String, String), GatewayError> {
    let trimmed = header.trim_start_matches('/');
    let without_query = trimmed.split('?').next().unwrap_or(trimmed);
    let (bucket, key) = without_query
        .split_once('/')
        .ok_or_else(|| GatewayError::BadRequest("malformed x-amz-copy-source".into()))?;
    if bucket.is_empty() || key.is_empty() {
        return Err(GatewayError::BadRequest("empty copy-source bucket or key".into()));
    }
    Ok((pct_decode(bucket), pct_decode(key)))
}

/// `CopyObjectResult` XML.
fn copy_object_xml(etag: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<CopyObjectResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
<LastModified>{}</LastModified><ETag>&quot;{}&quot;</ETag>\
</CopyObjectResult>",
        iso8601_millis(0),
        xml_escape(etag)
    )
}

/// `Tagging` XML for GetObjectTagging (also the body shape PutObjectTagging
/// accepts). An empty tag set still renders the `<TagSet/>` wrapper.
fn tagging_xml(tags: &[(String, String)]) -> String {
    let mut s = String::new();
    s.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    s.push_str("<Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    s.push_str("<TagSet>");
    for (k, v) in tags {
        s.push_str("<Tag>");
        s.push_str(&format!("<Key>{}</Key>", xml_escape(k)));
        s.push_str(&format!("<Value>{}</Value>", xml_escape(v)));
        s.push_str("</Tag>");
    }
    s.push_str("</TagSet>");
    s.push_str("</Tagging>");
    s
}

/// Enforce the S3 tagging limits: at most 10 tags, key length 1..=128, value
/// length 0..=256 (counted in Unicode scalar values). Shared by the XML body
/// parser and the `x-amz-tagging` header parser.
fn validate_tags(tags: &[(String, String)]) -> Result<(), GatewayError> {
    if tags.len() > 10 {
        return Err(GatewayError::BadRequest(
            "a request can contain at most 10 tags".into(),
        ));
    }
    for (key, value) in tags {
        if key.is_empty() || key.chars().count() > 128 {
            return Err(GatewayError::BadRequest("tag key length out of range".into()));
        }
        if value.chars().count() > 256 {
            return Err(GatewayError::BadRequest("tag value length out of range".into()));
        }
    }
    Ok(())
}

/// Parse a `Tagging` request body into `(key, value)` pairs (S3 limits enforced
/// by [`validate_tags`]). Tag text is XML-unescaped. An empty `<TagSet/>` yields
/// an empty vec, which is how DeleteObjectTagging clears the set.
fn parse_tagging(body: &[u8]) -> Result<Vec<(String, String)>, GatewayError> {
    let s = String::from_utf8_lossy(body);
    let mut tags = Vec::new();
    let mut rest: &str = s.as_ref();
    while let Some(start) = rest.find("<Tag>") {
        let after = &rest[start + "<Tag>".len()..];
        let end = after.find("</Tag>").ok_or_else(|| {
            GatewayError::BadRequest("malformed Tagging: unterminated <Tag>".into())
        })?;
        let block = &after[..end];
        let key = extract_xml_element(block, "Key")
            .ok_or_else(|| GatewayError::BadRequest("tag missing <Key>".into()))?;
        let value = extract_xml_element(block, "Value").unwrap_or("");
        tags.push((xml_unescape(key), xml_unescape(value)));
        rest = &after[end + "</Tag>".len()..];
    }
    validate_tags(&tags)?;
    Ok(tags)
}

/// Parse an `x-amz-tagging` header (a URL-encoded `k1=v1&k2=v2` query string, as
/// PutObject sends PUT-time tags) into validated `(key, value)` pairs. An empty
/// header is no tags.
fn parse_tagging_header(header: &str) -> Result<Vec<(String, String)>, GatewayError> {
    let header = header.trim();
    if header.is_empty() {
        return Ok(Vec::new());
    }
    let tags: Vec<(String, String)> = header
        .split('&')
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (pct_decode(k), pct_decode(v))
        })
        .collect();
    validate_tags(&tags)?;
    Ok(tags)
}

/// Return the inner text of the first `<name>…</name>` element in `haystack`, or
/// `None` if absent. Used to pull `<Key>`/`<Value>` out of a `<Tag>` block.
fn extract_xml_element<'a>(haystack: &'a str, name: &str) -> Option<&'a str> {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let start = haystack.find(&open)? + open.len();
    let rest = &haystack[start..];
    let end = rest.find(&close)?;
    Some(&rest[..end])
}

/// Parse the `x-amz-object-attributes` header into the requested attribute names
/// (comma-separated, e.g. `ETag,ObjectSize`), dropping blanks.
fn parse_object_attributes_header(header: &str) -> Vec<String> {
    header
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect()
}

/// `GetObjectAttributesResponse` XML, emitting only the `requested` attributes.
///
/// Two S3 quirks are deliberate here: the `<ETag>` is NOT quoted (unlike the
/// ETag header and every other ETag XML element), and `StorageClass` is omitted
/// for the default class — which is every OBS object, so it never appears.
/// `Checksum` and `ObjectParts` are likewise omitted: this gateway stores
/// neither additional checksums nor queryable post-completion part records.
fn object_attributes_xml(requested: &[String], etag: &str, size: u64) -> String {
    let mut s = String::new();
    s.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    s.push_str("<GetObjectAttributesResponse xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    if requested.iter().any(|a| a == "ETag") {
        s.push_str(&format!("<ETag>{}</ETag>", xml_escape(etag)));
    }
    if requested.iter().any(|a| a == "ObjectSize") {
        s.push_str(&format!("<ObjectSize>{size}</ObjectSize>"));
    }
    s.push_str("</GetObjectAttributesResponse>");
    s
}

/// Format epoch milliseconds as an RFC 7231 IMF-fixdate (HTTP `Last-Modified`),
/// e.g. `Fri, 01 Jan 2021 00:00:00 GMT`. Same civil-from-days core as
/// [`iso8601_millis`], plus the weekday (1970-01-01 was a Thursday).
fn http_date(ms: u64) -> String {
    const DOW: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const MON: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let secs = (ms / 1000) as i64;
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (hh, mm, ss) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let dow = DOW[(((days % 7) + 4).rem_euclid(7)) as usize];
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
        "{}, {:02} {} {:04} {:02}:{:02}:{:02} GMT",
        dow,
        d,
        MON[(m - 1) as usize],
        year,
        hh,
        mm,
        ss
    )
}

/// Parse an RFC 1123 / RFC 7231 IMF-fixdate (`Wed, 12 Oct 2009 17:50:00 GMT`)
/// into epoch seconds. Returns `None` for anything not in that single format
/// (the obsolete RFC 850 / asctime forms are rare from real clients and SDKs and
/// are treated as "no usable date" by the caller). Inverse of [`http_date`].
fn parse_http_date(s: &str) -> Option<u64> {
    // Drop the leading weekday and comma if present.
    let rest = s.split_once(',').map(|(_, r)| r).unwrap_or(s);
    let mut it = rest.split_whitespace();
    let day: i64 = it.next()?.parse().ok()?;
    let month = month_num(it.next()?)?;
    let year: i64 = it.next()?.parse().ok()?;
    let time = it.next()?;
    // A trailing "GMT" (or any zone token) is ignored; HTTP dates are always GMT.
    let mut t = time.split(':');
    let hh: i64 = t.next()?.parse().ok()?;
    let mm: i64 = t.next()?.parse().ok()?;
    let ss: i64 = t.next()?.parse().ok()?;
    if !(1..=31).contains(&day)
        || !(0..=23).contains(&hh)
        || !(0..=59).contains(&mm)
        || !(0..=60).contains(&ss)
    {
        return None;
    }
    let secs = days_from_civil(year, month, day) * 86_400 + hh * 3600 + mm * 60 + ss;
    u64::try_from(secs).ok()
}

/// Month abbreviation (`Jan`..`Dec`) to 1..=12.
fn month_num(m: &str) -> Option<i64> {
    const MON: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    MON.iter().position(|x| *x == m).map(|i| i as i64 + 1)
}

/// Days since 1970-01-01 for a civil (year, month, day) — Howard Hinnant's
/// `days_from_civil`, the inverse of the civil-from-days decomposition in
/// [`http_date`]/[`iso8601_millis`].
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Outcome of parsing a `Range` header against a known object length.
#[derive(Debug, PartialEq, Eq)]
enum RangeSpec {
    /// No usable byte-range header (wrong unit or malformed) — serve the whole
    /// object with `200`.
    Whole,
    /// A satisfiable range: inclusive `(start, end)` byte offsets.
    Satisfiable(usize, usize),
    /// Syntactically a byte range but not satisfiable for this object — the
    /// caller must answer `416 Range Not Satisfiable`.
    Unsatisfiable,
}

/// Parse a single HTTP `Range: bytes=...` header against a known object length.
///
/// Distinguishes "not a byte range / malformed" (serve the whole object, like
/// S3) from "valid byte-range syntax but out of bounds" (which must be a 416,
/// never a silent full-body 200). Multi-range (`bytes=0-9,20-29`) is not
/// supported and is treated as [`RangeSpec::Whole`].
fn parse_range(header: &str, total: usize) -> RangeSpec {
    let Some(spec) = header.trim().strip_prefix("bytes=") else {
        return RangeSpec::Whole; // wrong unit -> ignore the header
    };
    let Some((s, e)) = spec.split_once('-') else {
        return RangeSpec::Whole; // malformed -> ignore the header
    };
    if total == 0 {
        return RangeSpec::Unsatisfiable; // any range over an empty object
    }
    let last = total - 1;
    let (start, end) = if s.is_empty() {
        // Suffix range: the final N bytes. `bytes=-0` is unsatisfiable.
        let Ok(n) = e.trim().parse::<usize>() else {
            return RangeSpec::Whole;
        };
        if n == 0 {
            return RangeSpec::Unsatisfiable;
        }
        (total.saturating_sub(n), last)
    } else {
        let Ok(start) = s.trim().parse::<usize>() else {
            return RangeSpec::Whole;
        };
        let end = if e.trim().is_empty() {
            last
        } else {
            match e.trim().parse::<usize>() {
                Ok(v) => v.min(last),
                Err(_) => return RangeSpec::Whole,
            }
        };
        (start, end)
    };
    if start > end || start > last {
        return RangeSpec::Unsatisfiable;
    }
    RangeSpec::Satisfiable(start, end)
}

/// A `416 Range Not Satisfiable` response with the S3 `InvalidRange` body and the
/// required `Content-Range: bytes */{total}` header.
fn range_not_satisfiable_response(total: usize) -> Response<Full<Bytes>> {
    let xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<Error><Code>InvalidRange</Code>\
<Message>The requested range is not satisfiable</Message></Error>";
    Response::builder()
        .status(StatusCode::RANGE_NOT_SATISFIABLE)
        .header(hyper::header::CONTENT_RANGE, format!("bytes */{total}"))
        .header(hyper::header::CONTENT_TYPE, "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .expect("valid response")
}

/// Extract the `<Key>` values and the `<Quiet>` flag from a `DeleteObjects`
/// request body. In quiet mode the response reports only errors.
fn parse_delete_request(body: &[u8]) -> Result<(Vec<String>, bool), GatewayError> {
    let s = String::from_utf8_lossy(body);
    let quiet = extract_xml_element(&s, "Quiet")
        .map(|v| v.trim().eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let mut keys = Vec::new();
    let mut rest: &str = s.as_ref();
    while let Some(start) = rest.find("<Key>") {
        let after = &rest[start + "<Key>".len()..];
        let end = after
            .find("</Key>")
            .ok_or_else(|| GatewayError::BadRequest("malformed Delete request".into()))?;
        keys.push(xml_unescape(&after[..end]));
        rest = &after[end..];
    }
    if keys.is_empty() {
        return Err(GatewayError::BadRequest("no keys in delete request".into()));
    }
    Ok((keys, quiet))
}

/// Reverse of [`xml_escape`] for text taken from request XML.
fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&") // must be last
}

/// `DeleteResult` XML for a batch delete. In `quiet` mode, successful keys are
/// omitted and only errors are reported.
fn delete_result_xml(results: &[(String, Option<(String, String)>)], quiet: bool) -> String {
    let mut s = String::new();
    s.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    s.push_str("<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    for (key, err) in results {
        match err {
            None => {
                if !quiet {
                    s.push_str(&format!("<Deleted><Key>{}</Key></Deleted>", xml_escape(key)));
                }
            }
            Some((code, msg)) => s.push_str(&format!(
                "<Error><Key>{}</Key><Code>{}</Code><Message>{}</Message></Error>",
                xml_escape(key),
                xml_escape(code),
                xml_escape(msg)
            )),
        }
    }
    s.push_str("</DeleteResult>");
    s
}

/// `LocationConstraint` XML for GetBucketLocation.
fn bucket_location_xml(region: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{}</LocationConstraint>",
        xml_escape(region)
    )
}

/// `ListAllMyBucketsResult` XML for ListBuckets.
fn list_buckets_xml(buckets: &[(String, u64)]) -> String {
    let mut s = String::new();
    s.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    s.push_str("<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    s.push_str("<Owner><ID>ozone</ID><DisplayName>ozone</DisplayName></Owner>");
    s.push_str("<Buckets>");
    for (name, ctime) in buckets {
        s.push_str(&format!(
            "<Bucket><Name>{}</Name><CreationDate>{}</CreationDate></Bucket>",
            xml_escape(name),
            iso8601_millis(*ctime)
        ));
    }
    s.push_str("</Buckets></ListAllMyBucketsResult>");
    s
}

/// `ListMultipartUploadsResult` XML.
fn list_mpu_xml(bucket: &str, uploads: &[backend::UploadSummary]) -> String {
    let mut s = String::new();
    s.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    s.push_str("<ListMultipartUploadsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    s.push_str(&format!("<Bucket>{}</Bucket>", xml_escape(bucket)));
    s.push_str("<IsTruncated>false</IsTruncated>");
    for (upload_id, key) in uploads {
        s.push_str("<Upload>");
        s.push_str(&format!("<Key>{}</Key>", xml_escape(key)));
        s.push_str(&format!("<UploadId>{}</UploadId>", xml_escape(upload_id)));
        s.push_str("</Upload>");
    }
    s.push_str("</ListMultipartUploadsResult>");
    s
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

    #[test]
    fn parse_complete_parts_in_order() {
        let body = b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part><Part><PartNumber>2</PartNumber><ETag>\"b\"</ETag></Part></CompleteMultipartUpload>";
        assert_eq!(parse_complete_parts(body).unwrap(), vec![1, 2]);
        assert!(parse_complete_parts(b"<x/>").is_err());
    }

    #[test]
    fn parse_copy_source_forms() {
        assert_eq!(
            parse_copy_source("/bucket1/dir/obj").unwrap(),
            ("bucket1".to_string(), "dir/obj".to_string())
        );
        assert_eq!(
            parse_copy_source("bucket1/a%2Fb").unwrap(),
            ("bucket1".to_string(), "a/b".to_string())
        );
        assert_eq!(
            parse_copy_source("/bucket1/obj?versionId=9").unwrap(),
            ("bucket1".to_string(), "obj".to_string())
        );
        assert!(parse_copy_source("nobucket").is_err());
    }

    #[test]
    fn parse_range_forms() {
        use RangeSpec::*;
        // bytes 0-9 of 100 -> inclusive (0, 9).
        assert_eq!(parse_range("bytes=0-9", 100), Satisfiable(0, 9));
        // open-ended -> to last byte.
        assert_eq!(parse_range("bytes=50-", 100), Satisfiable(50, 99));
        // suffix -> last 10 bytes.
        assert_eq!(parse_range("bytes=-10", 100), Satisfiable(90, 99));
        // end clamped to last byte.
        assert_eq!(parse_range("bytes=90-999", 100), Satisfiable(90, 99));
        // valid byte-range syntax but out of bounds -> 416, not a silent 200.
        assert_eq!(parse_range("bytes=200-300", 100), Unsatisfiable);
        assert_eq!(parse_range("bytes=-0", 100), Unsatisfiable);
        assert_eq!(parse_range("bytes=0-9", 0), Unsatisfiable);
        // wrong unit / malformed -> serve the whole object.
        assert_eq!(parse_range("items=0-9", 100), Whole);
        assert_eq!(parse_range("bytes=abc", 100), Whole);
    }

    #[test]
    fn parse_delete_request_extracts_keys() {
        let body = b"<Delete><Object><Key>a/b</Key></Object><Object><Key>c&amp;d</Key></Object></Delete>";
        let (keys, quiet) = parse_delete_request(body).unwrap();
        assert_eq!(keys, vec!["a/b".to_string(), "c&d".to_string()]);
        assert!(!quiet, "no <Quiet> -> verbose");
        // Quiet flag is parsed.
        let q = b"<Delete><Quiet>true</Quiet><Object><Key>x</Key></Object></Delete>";
        let (keys, quiet) = parse_delete_request(q).unwrap();
        assert_eq!(keys, vec!["x".to_string()]);
        assert!(quiet, "<Quiet>true</Quiet> -> quiet");
        assert!(parse_delete_request(b"<Delete></Delete>").is_err());
    }

    #[test]
    fn xml_unescape_reverses_escape() {
        assert_eq!(xml_unescape("a &amp;&lt;b&gt; &quot;c&quot;"), "a &<b> \"c\"");
    }

    #[test]
    fn aws_chunked_unsigned_payload_trailer_variant() {
        // STREAMING-UNSIGNED-PAYLOAD-TRAILER: chunks carry no signature, and a
        // checksum trailer follows the terminating zero chunk.
        let body = b"5\r\nhello\r\n6\r\nworld!\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n";
        assert_eq!(&decode_aws_chunked(body).unwrap()[..], b"helloworld!");
    }

    #[test]
    fn aws_chunked_signed_with_trailer() {
        // STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER: signed chunks plus a
        // trailing checksum after the zero chunk.
        let body =
            b"3;chunk-signature=abc\r\nabc\r\n0;chunk-signature=def\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n";
        assert_eq!(&decode_aws_chunked(body).unwrap()[..], b"abc");
    }

    #[test]
    fn is_aws_chunked_detects_all_streaming_variants() {
        let none = hyper::HeaderMap::new();
        assert!(!is_aws_chunked(&none));

        for sha in [
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
            "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
        ] {
            let mut h = hyper::HeaderMap::new();
            h.insert("x-amz-content-sha256", sha.parse().unwrap());
            assert!(is_aws_chunked(&h), "should detect {sha}");
        }

        let mut enc = hyper::HeaderMap::new();
        enc.insert(hyper::header::CONTENT_ENCODING, "aws-chunked".parse().unwrap());
        assert!(is_aws_chunked(&enc));
    }

    #[test]
    fn parse_tagging_round_trips_and_validates() {
        let body = b"<Tagging><TagSet>\
<Tag><Key>env</Key><Value>prod</Value></Tag>\
<Tag><Key>team</Key><Value>a&amp;b</Value></Tag>\
</TagSet></Tagging>";
        assert_eq!(
            parse_tagging(body).unwrap(),
            vec![
                ("env".to_string(), "prod".to_string()),
                ("team".to_string(), "a&b".to_string()),
            ]
        );
        // Empty TagSet -> no tags (the DeleteObjectTagging shape).
        assert_eq!(
            parse_tagging(b"<Tagging><TagSet></TagSet></Tagging>").unwrap(),
            Vec::<(String, String)>::new()
        );
        // A missing <Value> defaults to empty.
        assert_eq!(
            parse_tagging(b"<Tagging><TagSet><Tag><Key>k</Key></Tag></TagSet></Tagging>").unwrap(),
            vec![("k".to_string(), String::new())]
        );
    }

    #[test]
    fn parse_tagging_rejects_limit_violations() {
        // > 10 tags.
        let mut body = String::from("<Tagging><TagSet>");
        for i in 0..11 {
            body.push_str(&format!("<Tag><Key>k{i}</Key><Value>v</Value></Tag>"));
        }
        body.push_str("</TagSet></Tagging>");
        assert!(parse_tagging(body.as_bytes()).is_err());
        // Empty key.
        assert!(
            parse_tagging(b"<Tagging><TagSet><Tag><Key></Key><Value>v</Value></Tag></TagSet></Tagging>")
                .is_err()
        );
        // Over-long value (257 chars).
        let long = "x".repeat(257);
        let over = format!("<Tagging><TagSet><Tag><Key>k</Key><Value>{long}</Value></Tag></TagSet></Tagging>");
        assert!(parse_tagging(over.as_bytes()).is_err());
    }

    #[test]
    fn tagging_xml_renders_tagset() {
        let xml = tagging_xml(&[("a".to_string(), "1".to_string())]);
        assert!(xml.contains("<TagSet><Tag><Key>a</Key><Value>1</Value></Tag></TagSet>"));
        // Empty set still emits the wrapper.
        assert!(tagging_xml(&[]).contains("<TagSet></TagSet>"));
    }

    #[test]
    fn extract_xml_element_first_match() {
        assert_eq!(extract_xml_element("<K>v</K>", "K"), Some("v"));
        assert_eq!(extract_xml_element("<K></K>", "K"), Some(""));
        assert_eq!(extract_xml_element("<A>x</A>", "K"), None);
    }

    #[test]
    fn parse_tagging_header_decodes_and_validates() {
        assert_eq!(
            parse_tagging_header("env=prod&team=storage").unwrap(),
            vec![
                ("env".to_string(), "prod".to_string()),
                ("team".to_string(), "storage".to_string()),
            ]
        );
        // Percent-encoded key/value.
        assert_eq!(
            parse_tagging_header("a%2Fb=c%20d").unwrap(),
            vec![("a/b".to_string(), "c d".to_string())]
        );
        // Empty header -> no tags.
        assert_eq!(parse_tagging_header("").unwrap(), Vec::new());
        // Bare key (no '=') -> empty value.
        assert_eq!(
            parse_tagging_header("flag").unwrap(),
            vec![("flag".to_string(), String::new())]
        );
        // Limits are shared with the XML parser: > 10 tags rejected.
        let many = (0..11)
            .map(|i| format!("k{i}=v"))
            .collect::<Vec<_>>()
            .join("&");
        assert!(parse_tagging_header(&many).is_err());
    }

    #[test]
    fn parse_object_attributes_header_splits() {
        assert_eq!(
            parse_object_attributes_header("ETag, ObjectSize ,StorageClass"),
            vec!["ETag", "ObjectSize", "StorageClass"]
        );
        assert!(parse_object_attributes_header("").is_empty());
        assert!(parse_object_attributes_header("  , ").is_empty());
    }

    #[test]
    fn object_attributes_xml_gates_on_requested() {
        let both = object_attributes_xml(
            &["ETag".to_string(), "ObjectSize".to_string()],
            "abc123",
            4096,
        );
        // ETag here is NOT quoted (S3's per-operation quirk).
        assert!(both.contains("<ETag>abc123</ETag>"), "{both}");
        assert!(both.contains("<ObjectSize>4096</ObjectSize>"), "{both}");
        assert!(
            both.contains("<GetObjectAttributesResponse"),
            "wrong root element: {both}"
        );
        // Only the requested attributes appear.
        let only_size = object_attributes_xml(&["ObjectSize".to_string()], "abc123", 7);
        assert!(!only_size.contains("<ETag>"), "{only_size}");
        assert!(only_size.contains("<ObjectSize>7</ObjectSize>"), "{only_size}");
    }

    #[test]
    fn http_date_formats_rfc1123() {
        assert_eq!(http_date(0), "Thu, 01 Jan 1970 00:00:00 GMT");
        // 2021-01-01T00:00:00Z was a Friday.
        assert_eq!(
            http_date(1_609_459_200_000),
            "Fri, 01 Jan 2021 00:00:00 GMT"
        );
        // 2009-10-12T17:50:00Z was a Monday.
        assert_eq!(
            http_date(1_255_369_800_000),
            "Mon, 12 Oct 2009 17:50:00 GMT"
        );
    }

    #[test]
    fn parse_http_date_round_trips() {
        assert_eq!(parse_http_date("Thu, 01 Jan 1970 00:00:00 GMT"), Some(0));
        assert_eq!(
            parse_http_date("Fri, 01 Jan 2021 00:00:00 GMT"),
            Some(1_609_459_200)
        );
        assert_eq!(
            parse_http_date("Mon, 12 Oct 2009 17:50:00 GMT"),
            Some(1_255_369_800)
        );
        // Inverse of http_date.
        assert_eq!(
            parse_http_date(&http_date(1_700_000_000_000)),
            Some(1_700_000_000)
        );
        // Unparseable -> None (the caller then ignores the condition).
        assert_eq!(parse_http_date("not a date"), None);
        assert_eq!(parse_http_date(""), None);
    }

    #[test]
    fn precondition_logic() {
        let etag = "abc123";
        let q = "\"abc123\"";
        let ts = 1_609_459_200_000; // object last-modified 2021-01-01
        let after = "Sat, 01 Jan 2022 00:00:00 GMT"; // threshold after the object
        let before = "Wed, 01 Jan 2020 00:00:00 GMT"; // threshold before the object
        // ETag-only conditions (no dates).
        let etag_only = |im, inm| precondition_status(im, inm, None, None, etag, ts);
        assert_eq!(etag_only(None, Some(q)), Some(StatusCode::NOT_MODIFIED));
        assert_eq!(etag_only(None, Some("*")), Some(StatusCode::NOT_MODIFIED));
        assert_eq!(etag_only(None, Some("\"other\"")), None);
        assert_eq!(etag_only(Some(q), None), None);
        assert_eq!(etag_only(Some("*"), None), None);
        assert_eq!(
            etag_only(Some("\"other\""), None),
            Some(StatusCode::PRECONDITION_FAILED)
        );
        assert_eq!(etag_only(Some("\"x\", \"abc123\""), None), None);

        // Date conditions.
        // If-Modified-Since after the object's mtime -> not modified -> 304.
        assert_eq!(
            precondition_status(None, None, Some(after), None, etag, ts),
            Some(StatusCode::NOT_MODIFIED)
        );
        // If-Modified-Since before -> modified -> proceed.
        assert_eq!(precondition_status(None, None, Some(before), None, etag, ts), None);
        // If-Unmodified-Since after -> not modified -> proceed.
        assert_eq!(precondition_status(None, None, None, Some(after), etag, ts), None);
        // If-Unmodified-Since before -> modified -> 412.
        assert_eq!(
            precondition_status(None, None, None, Some(before), etag, ts),
            Some(StatusCode::PRECONDITION_FAILED)
        );
        // Precedence: If-Match beats If-Unmodified-Since (which alone would 412).
        assert_eq!(
            precondition_status(Some(q), None, None, Some(before), etag, ts),
            None
        );
        // Precedence: If-None-Match beats If-Modified-Since (which alone -> 304).
        assert_eq!(
            precondition_status(None, Some("\"other\""), Some(after), None, etag, ts),
            None
        );
    }
}

