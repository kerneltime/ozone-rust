//! Probe the COMPLIANT Rust `OzoneOmClient` against a REAL Apache Ozone OM over its
//! gRPC port (default 8981). This is the wire-compatibility check the in-process
//! `CompliantOm` fixture cannot give: it talks to the actual Java OM.
//!
//! Usage: `cargo run --example probe_real_om -- [http://host:8981] [s3volume]`

use ozone_om_client::compliant::{OmError, OzoneOmClient};
use ozone_types::EcReplicationConfig;

#[tokio::main]
async fn main() {
    let endpoint = std::env::args().nth(1).unwrap_or_else(|| "http://127.0.0.1:8981".to_string());
    let vol = std::env::args().nth(2).unwrap_or_else(|| "s3v".to_string());
    let bkt = "rustprobe";

    let mut om = match OzoneOmClient::connect(endpoint.clone(), "rust-probe", "testuser").await {
        Ok(c) => {
            println!("[ok] connected to real OM at {endpoint}");
            c
        }
        Err(e) => {
            println!("[FAIL] connect: {e:?}");
            std::process::exit(1);
        }
    };

    step("head_bucket (before create)", om.head_bucket(&vol, bkt).await.map(|e| format!("exists={e}")));
    step("create_bucket", om.create_bucket(&vol, bkt).await.map(|_| "created".into()));
    step("head_bucket (after create)", om.head_bucket(&vol, bkt).await.map(|e| format!("exists={e}")));
    step("list_buckets", om.list_buckets(&vol).await.map(|b| format!("{b:?}")));

    // Key lifecycle. EC needs 5 datanodes; a tiny single-DN cluster may only form a
    // RATIS pipeline, so try EC then fall back to the bucket default (None).
    println!("--- key lifecycle (EC-3-2, then bucket default) ---");
    let ec = EcReplicationConfig::rs(3, 2, 1024);
    let open = match om.create_key(&vol, bkt, "k1", Some(ec), 5).await {
        Ok(o) => {
            println!("[ok] create_key(EC): client_id={} blocks={}", o.client_id, o.blocks.len());
            Some(o)
        }
        Err(e) => {
            println!("[info] create_key(EC) failed ({}); retrying with bucket default", short(&e));
            match om.create_key(&vol, bkt, "k1", None, 5).await {
                Ok(o) => {
                    println!("[ok] create_key(default): client_id={} blocks={}", o.client_id, o.blocks.len());
                    Some(o)
                }
                Err(e) => {
                    println!("[info] create_key(default) also failed: {}", short(&e));
                    None
                }
            }
        }
    };
    if let Some(o) = open {
        if let Some(b) = o.blocks.first() {
            println!("      block: container={} local={} ec={}-{} members={}",
                b.container_id, b.local_id, b.ec.data, b.ec.parity, b.members.len());
            for m in &b.members {
                println!("        slot {} -> {}", m.replica_index, m.endpoint);
            }
        }
        let commit = om.commit_key(&vol, bkt, "k1", o.client_id, &o.blocks, 5, Some("etag-probe"), &[]).await;
        step("commit_key", commit.map(|_| "committed".into()));
        step("get_key_info", om.get_key_info(&vol, bkt, "k1").await.map(|m| format!("size={} etag={:?} blocks={}", m.size, m.etag, m.blocks.len())));
        step("delete_key", om.delete_key(&vol, bkt, "k1").await.map(|_| "deleted".into()));
    }

    step("delete_bucket", om.delete_bucket(&vol, bkt).await.map(|_| "deleted".into()));
    println!("--- done ---");
}

fn step(name: &str, r: Result<String, OmError>) {
    match r {
        Ok(v) => println!("[ok] {name}: {v}"),
        Err(e) => println!("[FAIL] {name}: {e:?}"),
    }
}

fn short(e: &OmError) -> String {
    format!("{e:?}").chars().take(160).collect()
}
