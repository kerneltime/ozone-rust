# ozone-rust

A Rust S3 gateway and minimal Rust datanode for Apache Ozone, supporting OBS
buckets + erasure-coded data only. Fronted by a secure proxy that handles all
S3 auth: the gateway trusts the proxy-attested principal and does NOT verify
SigV4 (by design).

## Status

Two compliance tracks are implemented and proven, making the Rust components
wire-compatible, drop-in peers of a real Apache Ozone (2.0.0) cluster:

- **Track 1 -- Rust S3 gateway <-> real OM.** The gateway speaks Ozone's real
  `OzoneManagerService.submitRequest(OMRequest)` gRPC contract (OM port 8981).
  The full `aws-sdk-s3` surface passes end-to-end through a wire-compliant OM
  fixture, and the OM client has been validated against an actual Java OM
  (bucket lifecycle accepted; EC `create_key` reaching OM + SCM).
- **Track 2 -- Rust datanode <-> real SCM.** The datanode speaks the real
  `StorageContainerDatanodeProtocol` (version -> register -> heartbeat poll),
  with wire-faithful EC reconstruction and a proven scrubber -> SCM -> self-heal
  loop.

230 workspace tests pass (0 failed, 0 ignored); clippy is clean under `-D warnings`.

The authoritative, living status lives in two directories -- read them before
the code:

- **[`finished-work/`](finished-work/)** -- what is DONE and how it was proven.
- **[`pending-work/`](pending-work/)** -- what remains, ordered by value
  (real-cluster data-plane integration + hardening).

[`GAPS.md`](GAPS.md) is the known-gaps register for the S3 surface; per-flow
design docs are under [`docs/flows/`](docs/flows/).

## Architecture: two real control planes, one bespoke data plane

The two CONTROL planes use real, vendored Ozone protos. Only the gateway<->datanode
DATA plane (shard I/O) is still a bespoke Rust gRPC service -- this is the main
remaining gap to a mixed Rust/Java cluster (see `pending-work/` item 2):

- gateway -> OM: real `OmClientProtocol` (vendored) -- Track 1
- datanode <-> SCM: real `StorageContainerDatanodeProtocol` (vendored) -- Track 2
- gateway <-> Rust datanode: bespoke `datanode_gateway_v1` (Rust-only data plane)

## Workspace

15 crates under `crates/` plus `test-fixtures`:

- `ozone-types` -- shared types
- `ozone-config` -- TOML + env config
- `ozone-observability` -- Prometheus + tracing
- `isa-l-sys` / `isa-l-safe` / `ozone-ec` -- Reed-Solomon EC via ISA-L FFI
- `ozone-grpc-types` -- tonic-generated gRPC types (bespoke + vendored Ozone protos)
- `ozone-om-client` / `ozone-scm-client` -- OM and SCM gRPC clients
- `ozone-dn-client` / `ozone-dn-server` -- datanode gRPC client and server
- `ozone-storage` / `ozone-fjall-store` -- volume + container + metadata store
- `ozone-s3-gw` -- gateway binary
- `ozone-dn` -- datanode binary
- `test-fixtures` -- shared test helpers (incl. the wire-compliant `CompliantOm`
  / `CompliantScm`)

## Proto files

One bespoke data-plane proto plus the vendored REAL Ozone control-plane protos:

- `proto/datanode_gateway_v1.proto` -- gateway <-> Rust datanode (bespoke shard I/O)
- `proto/ozone/OmClientProtocol.proto` + `Security.proto` -- vendored real OM
  contract (Track 1)
- `proto/ozone/ScmServerDatanodeHeartbeatProtocol.proto` + `hdds.proto` --
  vendored real SCM <-> datanode contract (Track 2)

(The earlier bespoke `om_rust_gateway_v1.proto` and `scm_rust_datanode_v1.proto`
were retired when Tracks 1 and 2 landed.)

## Building

Requires Intel ISA-L (erasure coding) installed on the system -- discovered via
`pkg-config libisal` (>= 2.31), then env vars (`ISA_L_INCLUDE_DIR` /
`ISA_L_LIB_DIR`), then standard prefixes:

```bash
brew install isa-l              # macOS (arm64/x86); Linux: install libisal + libisal.pc
cargo test --workspace          # 230 tests
cargo clippy --workspace --tests
cargo build --release --workspace
```

Run the live stack for black-box acceptance (gateway + 5 EC datanodes +
`CompliantOm` on `:9878`): `cargo run --example rust_stack` -- see
[`acceptance/README.md`](acceptance/README.md).

## License

Apache-2.0.
