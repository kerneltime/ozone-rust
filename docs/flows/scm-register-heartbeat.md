# Flow: SCM ↔ Datanode register / heartbeat / version handshake (Track 2, B1–B2)

> **STATUS: IMPLEMENTED (Track 2 complete, commits `83dbe66..46a678a`).** This is the
> design doc that drove the work, retained for rationale. The Rust datanode now speaks
> the real `StorageContainerDatanodeProtocol` over gRPC via
> `ozone-dn-server/src/scm_compliant.rs` + `ozone-scm-client/src/compliant.rs`
> (`OzoneScmClient`); the bespoke `scm.rs` loop and `scm_rust_datanode_v1` proto have
> been DELETED. Every reference below to the "current" bespoke code (the bidi-streaming
> heartbeat, the `scm.rs:80-84` `assigned_uuid` rewrite, the `replicaIndex=0` /
> zero-capacity reports) describes the PRE-migration state — those are the bugs this
> design FIXED, not open issues. The `[I]` questions in §6 were resolved during
> implementation. See `finished-work/` for the proven end state.

Design doc for making the Rust datanode's control-plane loop a drop-in, compliant
peer of the real Apache Ozone SCM. Grounded in the vendored real proto
(`proto/ozone/ScmServerDatanodeHeartbeatProtocol.proto`, read directly) and the
compliance spec. `[V-source]` = verified against the vendored proto / quoted Ozone
source; `[I]` = inferred, must verify before relying on it.

## 1. The wire model (what must change)

Real Ozone exposes exactly ONE unary RPC and multiplexes by a `required Type`
discriminator — there is NO streaming. `[V-source: proto:579-583, 39-70]`

```
service StorageContainerDatanodeProtocolService {
  rpc submitRequest (SCMDatanodeRequest) returns (SCMDatanodeResponse);
}
SCMDatanodeRequest  { required Type cmdType; optional getVersionRequest|registerRequest|sendHeartbeatRequest; }
SCMDatanodeResponse { required Type cmdType; required Status status; optional *Response; }
Type { GetVersion=1; Register=2; SendHeartbeat=3; }
```

The current Rust loop uses four RPCs incl. a **bidi-streaming** heartbeat and a
separate ContainerReport stream (`scm.rs` over the bespoke `scm_rust_datanode_v1`).
Compliance requires collapsing to: version → register → a **DN-driven poll** that
repeatedly calls `submitRequest(SendHeartbeat)` and dispatches the commands in the
response. SCM never pushes. `[V-source]`

### 1.1 Handshake sequence (steady state)
1. `submitRequest(GetVersion, SCMVersionRequestProto{})` → `SCMVersionResponseProto{ softwareVersion, keys[] }`. Cache `keys` opaquely. `[V-source:89-98]`
2. `submitRequest(Register, SCMRegisterRequestProto{ extendedDatanodeDetails, nodeReport, containerReport, pipelineReports, [layout] })` → `SCMRegisteredResponseProto{ errorCode, datanodeUUID, clusterID, ... }`. **All four report fields are `required`** — the wrapping messages must be present even if their repeated lists are empty. **Keep the DN's own UUID** (the response merely echoes it; there is no rename). `[V-source:100-125]`
3. Loop every `interval`: `submitRequest(SendHeartbeat, SCMHeartbeatRequestProto{ datanodeDetails, [nodeReport], [containerReport], incrementalContainerReport[], commandStatusReports[], ... })` → `SCMHeartbeatResponseProto{ datanodeUUID, commands[], [term] }`; dispatch each command; sleep. `[V-source:131-159]`

## 2. Invariants (safety + liveness)

- **SAFETY S1 — UUID stability.** The DN registers and heartbeats under a single,
  self-owned UUID for its process lifetime. It MUST NOT adopt the response's
  `datanodeUUID` as a new identity (the current `assigned_uuid` rewrite at
  `scm.rs:80-84` is non-compliant; the real response only echoes). `[V-source]`
- **SAFETY S2 — once-only command delivery.** SCM drains its per-DN command queue
  remove-on-read inside `processHeartbeat`. `[V-source: CommandQueue]` Therefore the
  DN MUST execute (or durably enqueue) every command in a heartbeat response before
  issuing the next heartbeat; a dropped command is never redelivered until SCM
  re-mints from a fresh container report. The current per-command panic-isolation
  guard (`scm.rs:168`) is compliant-friendly and must be preserved.
- **SAFETY S3 — EC replica index correctness.** Every `ContainerReplicaProto` for an
  EC container MUST carry `replicaIndex ∈ [1, k+p]` (1-based slot). SCM throws
  `IllegalArgumentException` (crashing its replica-count math for that container) on
  index `< 1` or `> k+p`. **The FULL container report currently sends
  `replicaIndex=0` (`scm.rs:359`) — non-compliant; this is a real bug to fix in
  B2.** A replica is a reconstruction source only when `state == CLOSED(4)`.
  `[V-source: ECContainerReplicaCount + proto:212-238]`
- **SAFETY S4 — register requires non-empty real capacity.** `StorageReportProto`
  requires `storageUuid` + `storageLocation`; SCM placement/target-selection uses
  `capacity/remaining`. The current `node_report()` reports all-zero capacity
  (`scm.rs:411-423`) → real SCM cannot place reconstruction targets. Fix in B2.
  `[V-source: proto:175-192]`
- **LIVENESS L1 — heartbeat progress.** The DN heartbeats at the interval SCM
  dictated at register (or its configured fallback); a transient `submitRequest`
  failure logs and retries on the next tick (does not tear down the loop). The
  ticker stops cleanly on shutdown (no orphan/half-open busy-loop).
- **LIVENESS L2 — command drain.** Every command in a response is dispatched before
  the next heartbeat, so SCM's pending work for this DN makes progress.

## 3. Edge conditions / failure modes to model + test

| Condition | Required behavior | Test |
|---|---|---|
| `submitRequest` transient error (SCM restart / TCP drop) | log, retry next tick; loop survives | unit: inject N failures, assert loop continues + recovers |
| Response carries 0 commands | no-op, keep heartbeating | covered by steady-state test |
| Response carries multiple commands | dispatch ALL before next heartbeat (S2) | compliant-fixture: 2 commands in one response, both execute |
| Unknown/unhandled command type | ignore, keep looping (S2 once-only ⇒ must not stall) | "unknown command" test, loop survives |
| Poison command (handler panics) | catch_unwind, log, continue (preserve `scm.rs:168`) | port `poison_reconstruct...` onto compliant fixture |
| EC container in FULL report | `replicaIndex ∈ [1,k+p]`, `state=CLOSED` (S3) | assert decoded report has correct per-slot index |
| Corrupt shard detected (scrubber) | `incrementalContainerReport` with `state=UNHEALTHY, replicaIndex=slot` on the heartbeat | assert ICR shape on the heartbeat (not a separate RPC) |
| Register with empty pipeline list | wrapping `PipelineReportsProto` present, list empty; SCM accepts | `[I]` verify SCM tolerates empty pipeline report |
| Zero-capacity volume | non-compliant; must report real capacity (S4) | assert StorageReport has non-zero capacity + storageUuid |

## 4. Concurrency contract

- The heartbeat poll loop is single-threaded per DN (one in-flight `submitRequest`
  at a time); no bidi stream, so no producer/consumer split on the heartbeat side.
- The **scrubber → reporter** path still runs concurrently with the heartbeat loop:
  the scrubber emits findings; the heartbeat loop must fold them into the NEXT
  heartbeat's `incrementalContainerReport[]`. The bespoke design used a separate
  reporter task + its own SCM client (`scm.rs:99-136, 380-407`); compliant design
  moves the UNHEALTHY signal onto the heartbeat, so the reporter becomes a
  shared-state producer (a queue of pending ICRs) drained by the heartbeat loop.
  The rising-edge latch (report each `(container,slot)` once until healed) stays
  valid and prevents ICR spam. `[V-source: scm.rs latch + proto ICR]`
- No two heartbeats overlap, so the ICR queue drain is a simple lock-or-channel; no
  reorder hazard on the wire (one heartbeat at a time, FIFO).

## 5. Tests this flow needs (the B1–B2 worklist)

1. **B0 (done):** wire-encoding smoke (`real_scm_proto.rs`).
2. **Compliant fixture** `compliant_scm.rs`: a tonic server implementing
   `StorageContainerDatanodeProtocolService.submitRequest`, recording decoded
   `SCMRegisterRequestProto` + each `SCMHeartbeatRequestProto`, and returning
   queued `SCMCommandProto`s on a heartbeat response (mimics remove-on-read).
3. **Register compliance:** all four report messages present; `ExtendedDatanodeDetails`
   has a `REPLICATION` named Port; the DN keeps its own UUID.
4. **EC report compliance:** FULL report lists each held slot with `replicaIndex ∈
   [1,k+p]` and `state=CLOSED` (fails today — guards S3).
5. **Capacity compliance:** node report has real, non-zero `capacity/remaining` +
   `storageUuid/storageLocation` (guards S4).
6. **Poll + command dispatch:** a `SendHeartbeat` whose response carries a
   `CloseContainer` (then a `DeleteContainer{replicaIndex}`) is executed; multiple
   commands in one response all execute (S2); unknown command ignored; poison
   command survived.
7. **Liveness:** transient `submitRequest` failures don't kill the loop; the loop
   recovers and keeps heartbeating.

## 6. Open `[I]` items to verify before/while coding

- Whether SCM accepts an empty `PipelineReportsProto` at register for a data-only
  EC DN. `[I]`
- Which `SCMVersionResponseProto.keys` are load-bearing (clusterId? scmId?) vs
  opaque cache. `[I]`
- Whether `CommandStatus EXECUTED` must be sent on the heartbeat for SCM to
  consider a command done, or SCM relies solely on the next container report. `[I]`
  (affects whether B2 must populate `commandStatusReports`).
