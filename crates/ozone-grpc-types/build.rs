//! Generates Rust code from the three .proto files via tonic-build.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.join("proto"))
        .expect("workspace root not found");

    // Vendored real Apache Ozone protos (proto2) live under proto/ozone/. The SCM
    // proto imports "hdds.proto" relative to that dir, so it is a second include
    // path. hdds.proto is NOT a compile input (it is pulled in via the import; its
    // types are still generated) — listing it as an input too would make protoc
    // canonicalize it under two names and double-define every symbol.
    let ozone_root = proto_root.join("ozone");
    let compile_inputs = [
        proto_root.join("datanode_gateway_v1.proto"),
        proto_root.join("om_rust_gateway_v1.proto"),
        ozone_root.join("ScmServerDatanodeHeartbeatProtocol.proto"),
    ];

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &compile_inputs.iter().map(|p| p.as_path()).collect::<Vec<_>>(),
            &[proto_root.as_path(), ozone_root.as_path()],
        )?;

    for p in compile_inputs.iter().chain([ozone_root.join("hdds.proto")].iter()) {
        println!("cargo:rerun-if-changed={}", p.display());
    }

    Ok(())
}
