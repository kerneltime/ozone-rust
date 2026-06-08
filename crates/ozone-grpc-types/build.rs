//! Generates Rust code from the workspace .proto files via tonic-build.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.join("proto"))
        .expect("workspace root not found");

    // Vendored real Apache Ozone protos (proto2) live under proto/ozone/. The SCM and
    // OM protos import "hdds.proto" (and OM also imports "Security.proto") relative to
    // that dir, so it is a second include path. hdds.proto and Security.proto are NOT
    // compile inputs (they are pulled in via the imports; their types are still
    // generated) — listing an imported file as an input too would make protoc
    // canonicalize it under two names and double-define every symbol.
    let ozone_root = proto_root.join("ozone");
    let compile_inputs = [
        proto_root.join("datanode_gateway_v1.proto"),
        ozone_root.join("ScmServerDatanodeHeartbeatProtocol.proto"),
        ozone_root.join("OmClientProtocol.proto"),
    ];

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &compile_inputs.iter().map(|p| p.as_path()).collect::<Vec<_>>(),
            &[proto_root.as_path(), ozone_root.as_path()],
        )?;

    let imported = [
        ozone_root.join("hdds.proto"),
        ozone_root.join("Security.proto"),
    ];
    for p in compile_inputs.iter().chain(imported.iter()) {
        println!("cargo:rerun-if-changed={}", p.display());
    }

    Ok(())
}
