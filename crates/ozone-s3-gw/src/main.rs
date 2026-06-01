//! `ozone-s3-gw` binary: stand up the S3 gateway over an OM endpoint.
//!
//! Configuration is read from the environment (a fuller config file is wired in
//! a later milestone):
//! - `OZONE_OM_ENDPOINT` (default `http://127.0.0.1:9899`) — OM gRPC endpoint.
//! - `OZONE_S3_LISTEN`   (default `0.0.0.0:9878`)         — S3 HTTP bind addr.
//! - `OZONE_S3_REGION`   (default `us-east-1`)            — reported region.
//! - `RUST_LOG`          — tracing filter (default `info`).

use std::sync::Arc;

use ozone_observability::{init_tracing, TracingOptions};
use ozone_s3_gw::{serve, Gateway};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing(&TracingOptions::default())?;

    let om_endpoint =
        std::env::var("OZONE_OM_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9899".to_string());
    let listen = std::env::var("OZONE_S3_LISTEN").unwrap_or_else(|_| "0.0.0.0:9878".to_string());
    let region = std::env::var("OZONE_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());

    let gateway = Arc::new(Gateway::connect(om_endpoint.clone(), region).await?);
    let listener = TcpListener::bind(&listen).await?;
    tracing::info!(%listen, %om_endpoint, "ozone-s3-gw listening");

    serve(gateway, listener).await?;
    Ok(())
}
