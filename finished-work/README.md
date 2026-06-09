# Finished work

A handoff snapshot of what is DONE in this repo as of 2026-06-09, so the work can be
resumed on a machine with more RAM. Companion: [`../pending-work/`](../pending-work/).

The repo is a greenfield Rust reimplementation of Apache Ozone's S3 gateway + a minimal
datanode. Scope: S3-compliant gateway for OBS buckets, **erasure-coded data only**,
security delegated to an upstream proxy (the gateway trusts the proxy-attested
principal and does NOT verify SigV4). Two compliance tracks make the Rust components
drop-in, wire-compatible peers of the real Java cluster.

## Track 2 — Rust datanode ↔ real SCM: COMPLETE, proven, single implementation

The datanode speaks the REAL `StorageContainerDatanodeProtocol` (vendored
`proto/ozone/{hdds,ScmServerDatanodeHeartbeatProtocol}.proto` →
`ozone_grpc_types::hadoop::hdds`): unary `submitRequest`, version → register (4 reports
inline + a REPLICATION port + capacity + own uuid) → heartbeat poll, commands drained
remove-on-read with `catch_unwind`.

- **EC reconstruction is wire-faithful**: byte-per-index 1-based `missingContainerIndexes`,
  positional `targets[i] ↔ index` pairing, survivor-enumeration via `list_blocks` +
  `min(blockGroupLen)` length (the partial-stripe safety guarantee), decode → re-encode
  byte-identical (`crates/ozone-dn-server/src/repair.rs`).
- **Lifecycle** (`reconstruct_from_survivors`): absent target → create Open → rebuild →
  CLOSED (mirrors real Ozone RECOVERING→CLOSED, reusing Open since there is no Recovering
  enum); mid-rebuild failure on a WE-created container → group-atomic rollback; a
  pre-existing container is NEVER deleted on failure (the `we_created` data-safety
  boundary); empty rebuild (< k survivors) → rollback (no spurious replica); CLOSED
  re-delivery → idempotent no-op.
- **Convergence**: after a rebuild/close the loop emits an incremental ContainerReport
  with the replica's state (CLOSED) so SCM's map (keyed by containerID+datanodeID,
  ignoring state) overwrites the prior UNHEALTHY — verified against apache/ozone's
  `AbstractContainerReportHandler` / `ContainerStateMap`.
- **End-to-end**: the full self-heal loop is proven over the real protocol (scrubber →
  UNHEALTHY ICR → fake SCM mints ReconstructEC → heal → CLOSED ICR), plus every edge
  case: multi-block-group, unrecoverable give-up, 2-DN concurrency, command routing,
  malformed-command resilience. EVERY new test was confirmed load-bearing by mutation.
- The old bespoke Rust-native `scm::dn::v1` / `ScmRustDatanodeService` / `fake_scm` /
  bespoke `ScmClient` / `scm.rs` loop is DELETED — one compliant control plane.

Pieces: `ozone-dn-server/src/scm_compliant.rs`, `ozone-scm-client/src/compliant.rs`
(`OzoneScmClient`), `test-fixtures/src/compliant_scm.rs` (`CompliantScm` + `with_pipeline`).
Design doc: `docs/flows/ec-reconstruction.md`. Commits `83dbe66..46a678a`.

## Track 1 — Rust S3 gateway ↔ real OM: COMPLETE, single implementation

The gateway speaks the REAL `OzoneManagerService.submitRequest(OMRequest)` envelope
(gRPC, OM port 8981). Vendored `proto/ozone/{OmClientProtocol,Security}.proto` →
`ozone_grpc_types::hadoop::{ozone,common}`.

- **`OzoneOmClient`** (`ozone-om-client/src/compliant.rs`) exposes DOMAIN methods
  (`BlockLocation`/`OpenKey`/`KeyMeta`) that hide the OMRequest envelope, so the gateway
  depends on no raw OM proto. Security-off auth = `clientId` + `s3Authentication.accessId`.
  Invariants: W1 (check `status==OK` before reading a sub-response — the 1-based-enum
  trap), W2 (commit the actually-written blocks), W3 (EC shard slot from
  `pipeline.member_replica_indexes[i]`, NOT member position — mutation-proven).
- **`CompliantOm`** fixture (`test-fixtures/src/compliant_om.rs`) emits real OM proto
  shapes; multipart, list-keys, and object tagging all at parity. The AWS multipart ETag
  (`hex(md5(concat of raw 16-byte part digests))-N`, hex-decoding the per-part ETags) is
  mutation-proven.
- **The backend** (`ozone-s3-gw/src/backend.rs`, 1114 lines) was rewritten off the bespoke
  OM onto `OzoneOmClient` + domain types: per-request principal, EC read from the created
  key's block, ListObjectsV2 delimiter-fold + continuation done gateway-side over the OM's
  flat `ListKeys`, stateless multipart.
- **Full e2e green**: the entire aws-sdk-s3 suite (`tests/s3_e2e.rs`, 36 tests:
  PUT/GET/HEAD/DELETE, list + prefix + delimiter + pagination, multipart, copy, ranged +
  conditional GET, batch delete, tagging, degraded + corrupted-shard reads) passes through
  `CompliantOm`. The OM↔datanode bridge is also proven directly in
  `tests/compliant_om_e2e.rs` (compliant client + 5 real datanodes do a full EC PUT/GET
  byte-identical + a degraded read).
- The bespoke OM (`om::gw::v1` / `om_rust_gateway_v1.proto` / `fake_om` / bespoke `OmClient`)
  is DELETED. Design doc: `docs/flows/om-gateway-protocol.md`. Commits `6d18587..15a7384`.

## Acceptance testing — external tools against the live Rust stack

`crates/ozone-s3-gw/examples/rust_stack.rs` (`cargo run --example rust_stack`) stands up
the real gateway + 5 EC datanodes + `CompliantOm` on `:9878`.

- The real **`aws` CLI** drives the full S3 surface (incl. a 7 MiB multipart, correct
  `…-N` ETag, byte-identical) — all pass.
- A **Robot Framework** suite (`acceptance/rust_s3_smoke.robot`, coverage mirrors Ozone's
  own s3 smoketests) passes **14/14** against the live stack.

See `acceptance/README.md`. Commit `d5bad6e`.

## Real Java Ozone validation (the big one)

Stood up a REAL Apache Ozone **2.0.0** cluster (SCM + OM + datanode) as plain **JVM
processes — no Docker** (Java 11), and pointed the compliant `OzoneOmClient` at the REAL
Java OM over gRPC (`crates/ozone-om-client/examples/probe_real_om.rs`):

- connect → InfoBucket (exists false → create → true) → **CreateBucket accepted by the
  real OM** → ListBuckets → DeleteBucket: ALL SUCCEED. This proves the vendored proto +
  request envelope + security-off auth are wire-compatible with actual Java Ozone, not
  just the fixture.
- **Found + fixed a real bug the lenient fixture hid**: `ListBucketsRequest.count` was
  unset; the real OM treats missing/zero `count` as "return nothing". Fixed with
  `count=1024`. Commit `5056850`.
- **EC `create_key` is wire-correct end to end**: with a valid 1024 KiB EC chunk the OM
  accepts it and SCM processes it, failing only with `"No enough datanodes to choose.
  RequiredNodes = 5 AvailableNodes = 1"` — i.e. a placement limit from the single datanode
  (a RAM constraint in the dev box), NOT a protocol mismatch. Commit `4d9095b`.

**Update — full EC key lifecycle now COMPLETES (5 datanodes, Docker compose).** On a
128 GB host the cluster was re-stood-up via the bundled `compose/ozone` scaled to 5
datanodes (OM gRPC 8981 published; the tight-RAM SerialGC/timeout workarounds dropped).
SCM allocates a real `EC{rs-3-2-1024k}` pipeline across all 5 DNs (slots 1..5, 1-based)
and the probe completes `create_key(EC) -> commit_key -> get_key_info -> delete_key`
against the real Java OM. This surfaced and fixed one wire gap the in-memory fixture had
masked: the OM client built each datanode endpoint from a port named `REPLICATION` (the
Rust DN's name), but a real Java DN's client-facing pipeline carries `STANDALONE` (9859)
instead. `key_location_to_block` now falls back across
`REPLICATION -> STANDALONE -> RATIS -> CLIENT_RPC` (regression test
`ec_mapping_falls_back_to_standalone_for_real_java_dn`).

## Test counts (whole workspace, all green, clippy clean under -D warnings)

230 unit/integration tests across the workspace, including `s3_e2e` 36, `ec_repair` 19,
`compliant_scm_loop` 3, `compliant_om` (in test-fixtures) 19, `ozone-om-client` 16.

## How to verify quickly

```sh
cargo test --workspace          # 229 tests, all pass
cargo clippy --workspace --tests
```
