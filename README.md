# ozone-rust

A Rust S3 gateway and minimal Rust datanode for Apache Ozone, supporting OBS
buckets + erasure-coded data only. Fronted by a secure proxy that handles all
S3 auth.

## Status

**Phase 4 scaffolding (M0).** Workspace structure, proto files, and empty
crate stubs are in place. `cargo check --workspace` passes; no real
functionality is implemented yet.

See the design corpus in
`~/notetaker/Projects/Apache Ozone/S3 Gateway Rust/` (Phases 1-3, ~99K words)
for the full design before reading code.

## Workspace

15 crates under `crates/` plus `test-fixtures`:

- `ozone-types` -- shared types
- `ozone-config` -- TOML + env config
- `ozone-observability` -- Prometheus + tracing
- `isa-l-sys` / `isa-l-safe` / `ozone-ec` -- Reed-Solomon EC via ISA-L FFI
- `ozone-grpc-types` -- tonic-generated gRPC types
- `ozone-om-client` / `ozone-scm-client` -- OM and SCM gRPC clients
- `ozone-dn-client` / `ozone-dn-server` -- datanode gRPC client and server
- `ozone-storage` / `ozone-fjall-store` -- volume + container + metadata store
- `ozone-s3-gw` -- gateway binary
- `ozone-dn` -- datanode binary
- `test-fixtures` -- shared test helpers

## Three .proto files

- `proto/datanode_gateway_v1.proto` -- gateway <-> Rust datanode
- `proto/om_rust_gateway_v1.proto` -- OM <-> Rust gateway (Java-side addition)
- `proto/scm_rust_datanode_v1.proto` -- SCM <-> Rust datanode (Java-side addition)

## Building

```bash
cargo check --workspace        # validates structure
cargo test --workspace          # placeholder tests
cargo build --release --workspace
```

## Milestones

- **M0** (this commit): workspace scaffold + proto files + empty crate stubs
- **M1**: ISA-L FFI + ozone-ec with byte-equivalence tests vs Java
- **M2**: gRPC types + storage layer + fjall integration
- **M3**: Datanode binary MVP (single volume, single container, PUT+GET)
- **M4**: Gateway binary MVP (S3 PUT+GET without EC)
- **M5**: EC end-to-end (encode, store, read, degraded read)
- **M6**: Reconstruction in DN
- **M7**: Multipart upload
- **M8**: Listing + delimiter folding
- **M9**: mTLS + observability + production hardening
- **M10**: Performance tuning to hit 100 GbE target

Full plan: `notetaker/Projects/Apache Ozone/S3 Gateway Rust/2026-05-30 Skeleton Implementation Plan.md`.

## License

Apache-2.0.
