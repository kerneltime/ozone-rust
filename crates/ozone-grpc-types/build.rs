//! Generates Rust code from the three .proto files via tonic-build.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.join("proto"))
        .expect("workspace root not found");

    let protos = [
        proto_root.join("datanode_gateway_v1.proto"),
        proto_root.join("om_rust_gateway_v1.proto"),
        proto_root.join("scm_rust_datanode_v1.proto"),
    ];

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &protos.iter().map(|p| p.as_path()).collect::<Vec<_>>(),
            &[proto_root.as_path()],
        )?;

    for p in &protos {
        println!("cargo:rerun-if-changed={}", p.display());
    }

    Ok(())
}
