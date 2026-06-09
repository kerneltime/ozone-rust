# Flow: SCM gRPC datanode adapter (Java-side, Track 2 / pending item 3)

> **STATUS: SPEC (not yet implemented).** This is the design for a Java-side change to
> Apache Ozone SCM. It is the prerequisite for an all-Rust data plane against a real
> control plane (`pending-work/README.md` item 2a / item 3). No Java has been written;
> this doc is the spec to review BEFORE coding, per spec -> plan -> tests -> code.

Design doc for letting the Rust datanode register + heartbeat with a REAL Apache Ozone
SCM by adding a gRPC transport to SCM's datanode protocol — "the only change being adding
gRPC support to SCM." Grounded in a direct read of apache/ozone at tag `ozone-2.0.0`
(local clone). `[V-source: path:line]` = verified against that source this session; `[I]` =
inferred / design decision.

Companion: [`scm-register-heartbeat.md`](scm-register-heartbeat.md) (the Rust DN side of
this protocol, already IMPLEMENTED) and [`om-gateway-protocol.md`](om-gateway-protocol.md)
(the Track-1 precedent: OM already serves its protocol over both Hadoop-RPC and gRPC).

## 1. Why this is needed (the exact gap)

The Rust datanode already speaks the REAL `StorageContainerDatanodeProtocol` — the single
unary `submitRequest(SCMDatanodeRequest) -> SCMDatanodeResponse` multiplexed by
`Type{GetVersion=1,Register=2,SendHeartbeat=3}` — but over **gRPC/tonic**
(`ozone-scm-client/src/compliant.rs`, `ozone-dn-server/src/scm_compliant.rs`). Stock SCM
serves that identical protobuf **only over Hadoop-RPC** on `ozone.scm.datanode.port`
(default **9861**) using the legacy `ProtobufRpcEngine`
`[V-source: SCMDatanodeProtocolServer.java:81,163; ScmConfigKeys.java:166-168]`. There is
no gRPC transport for it and **no gRPC stub is generated** for this proto today
`[V-source: interface-server/pom.xml:80-83,99-102 — the grpc execution includes only
InterSCMProtocol/SCMUpdateProtocol; the datanode proto falls to the proto2/Hadoop-RPC
execution]`. So a Rust DN cannot register with a real SCM: same messages, incompatible
transport. Closing this lets a real OM hand the Rust gateway pipelines that point at Rust
datanodes, and the existing Rust EC data path (Track 1 + Track 2) works end to end on a
real control plane.

## 2. The wire model is already identical (only transport differs)

The protobuf `service` already exists in Ozone's own interface:

```
// hadoop-hdds/interface-server/src/main/proto/ScmServerDatanodeHeartbeatProtocol.proto:579
service StorageContainerDatanodeProtocolService {
  rpc submitRequest (SCMDatanodeRequest) returns (SCMDatanodeResponse);
}
```
`[V-source: ScmServerDatanodeHeartbeatProtocol.proto:579-582]`. The envelope, the `Type`
discriminator, and every sub-message (`SCMVersion*`, `SCMRegister*`, `SCMHeartbeat*`) are
exactly what the Rust client emits and matches the vendored
`proto/ozone/ScmServerDatanodeHeartbeatProtocol.proto` byte-for-byte (Track 2 vendored it).
So the adapter writes **no new wire contract** — it re-exposes the existing service over a
new transport and delegates to the existing handler.

The delegation target already exists and is transport-agnostic:
`StorageContainerDatanodeProtocolServerSideTranslatorPB.submitRequest(RpcController,
SCMDatanodeRequest)` performs the full `Type` switch (getVersion/register/sendHeartbeat),
the register-field unpacking, and the metrics/tracing dispatch, depending ONLY on a
`StorageContainerDatanodeProtocol impl` — not on any Hadoop-RPC machinery
`[V-source: StorageContainerDatanodeProtocolServerSideTranslatorPB.java:55,88-126]`. The
handler itself is `SCMDatanodeProtocolServer`, which implements the interface directly
`[V-source: SCMDatanodeProtocolServer.java:113-114]`. This mirrors EXACTLY how OM's gRPC
impl `OzoneManagerServiceGrpc` delegates to `OzoneManagerProtocolServerSideTranslatorPB`
`[V-source: OzoneManagerServiceGrpc.java:44,74-76]`.

## 3. THE key decision: the protobuf-2.5.0 vs protobuf-3 fork

This is the one design fork that determines blast radius, and it needs an explicit call.

The datanode protocol is generated and served with **legacy protobuf 2.5.0** under the
legacy `ProtobufRpcEngine` `[V-source: SCMDatanodeProtocolServer.java:163;
pom.xml:200 proto2.hadooprpc.protobuf.version=2.5.0]`. gRPC's `protoc-gen-grpc-java`
REQUIRES protobuf 3.x `[V-source: pom.xml:97 grpc.protobuf-compile.version=3.19.6,
:110 io.grpc.version=1.58.0]`. The 2.5.0 `com.google.protobuf.Message` classes and the
3.x classes are different, incompatible types — so a gRPC stub (necessarily protobuf-3)
**cannot share generated message classes with the existing Hadoop-RPC server** (2.5.0).
Two coherent strategies:

### Strategy A — additive isolated gRPC stub + byte-bridge  [RECOMMENDED]

Generate a protobuf-3 + grpc-java version of the datanode service/messages in an ISOLATED
java package (new `.proto` copy or new module), unshaded `io.grpc` (the datanode proto's
package `org.apache.hadoop.hdds.protocol.proto` escapes interface-server's ratis-shading
antrun, which only rewrites `.../scm/proto` `[V-source: interface-server/pom.xml:120-122]`
— matching OM's unshaded-grpc setup). The new gRPC service impl:

1. receives a protobuf-3 `SCMDatanodeRequest`,
2. `toByteArray()` -> re-`parseFrom()` as the legacy 2.5.0 `SCMDatanodeRequest` (the wire
   schema is identical, so the bytes round-trip exactly),
3. calls the EXISTING `StorageContainerDatanodeProtocolServerSideTranslatorPB.submitRequest(
   null, legacyReq)`,
4. byte-bridges the legacy `SCMDatanodeResponse` back to protobuf-3 and returns it.

- **Blast radius: zero on the existing fleet.** The Hadoop-RPC datanode path, every Java
  datanode, and the 2.5.0 classes are untouched. Purely additive.
- **Cost:** one extra serialize+parse per RPC. Heartbeats are O(seconds) per DN; negligible.
- **Upstreamable** as a clean "adds a gRPC transport, off by default" change.
- The byte-bridge is the deliberate price of NOT migrating the legacy engine.

### Strategy B — migrate the datanode protocol to ProtobufRpcEngine2 / protobuf-3

Move `ScmServerDatanodeHeartbeatProtocol.proto` to the protoc3+grpc execution (like
`OmClientProtocol.proto`), generating hadoop-shaded protobuf-3 messages + the grpc stub as
ONE class set, and migrate `SCMDatanodeProtocolServer` from `ProtobufRpcEngine` to
`ProtobufRpcEngine2`. Both transports then share one class set; the gRPC impl calls the
translator with no conversion.

- **Cleaner end state**, but **changes the existing Hadoop-RPC datanode<->SCM path used by
  ALL Java datanodes** (server + client translators + every consumer of the 2.5.0 classes).
  High regression surface across the whole project; not "minimal."
- Justified only as a separate, project-wide modernization — out of scope for "let the Rust
  DN join a real SCM."

**Recommendation: Strategy A.** It is additive, isolates risk to new code, honors the
"only adds gRPC" intent, and is the smallest path to the goal. The rest of this spec
assumes A; the only A-specific artifact is the byte-bridge (§5.2), which is independently
unit-testable.

## 4. Change surface (Strategy A) — what gets added, what is untouched

ADDED (all new code/config; nothing existing is modified except the lifecycle hook + ctor):

1. **gRPC stub generation** for the datanode service, protobuf-3 + grpc-java, unshaded
   `io.grpc`, isolated java package. Mechanism options (finalize in the plan): a new
   schema-identical `.proto` copy with a distinct `java_package`/`java_outer_classname`,
   compiled by a protoc3+grpc execution in a new or existing build module. `[I]`
2. **`ScmDatanodeGrpcServer`** — a Netty gRPC server, modeled on `GrpcOzoneManagerServer`
   `[V-source: GrpcOzoneManagerServer.java:144-173]` (or the in-SCM
   `InterSCMGrpcProtocolService` `[V-source: InterSCMGrpcProtocolService.java:56-114]`,
   which already lives in `server-scm` and gives the SCM TLS/port pattern — but uses
   ratis-shaded grpc, so model imports on `GrpcOzoneManagerServer` to match the unshaded
   stub). Holds boss/worker `NioEventLoopGroup` + read executor + `start()/stop()`.
3. **`StorageContainerDatanodeGrpcService`** — `extends
   StorageContainerDatanodeProtocolServiceGrpc.StorageContainerDatanodeProtocolServiceImplBase`;
   its `submitRequest(req, StreamObserver)` does the byte-bridge of §3.A into the EXISTING
   translator and completes the observer. The single point that bridges transports.
4. **Config keys** (new; do not reuse `ozone.scm.grpc.port`, which is InterSCM's
   `[V-source: ScmConfigKeys.java:486-488]`):
   - `ozone.scm.datanode.grpc.port` — listen port, default a currently-unused SCM port
     (NOT 9861, which is the DN Hadoop-RPC port). Finalize the number in the plan. `[I]`
   - `ozone.scm.datanode.grpc.enabled` — boolean, **default false** (off by default keeps
     stock behavior; our test cluster sets it true), mirroring OM's
     `isOmGrpcServerEnabled` gate `[V-source: OzoneManager.java:560-562,1730-1731]`.
5. **Thread-local call-context seeding** in the gRPC impl. The handler reads
   `Server.getRemoteAddress()` for audit (`atIp(...)`)
   `[V-source: SCMDatanodeProtocolServer.java:463,476]`; on a gRPC worker thread there is
   no Hadoop IPC `Call`, so this returns null (or risks NPE in any non-null-safe read).
   Seed a synthetic `Server.Call` (and set the client address from gRPC metadata) before
   delegating, exactly as OM's gRPC impl does `[V-source: OzoneManagerServiceGrpc.java:
   60-65]`, clearing it in a finally. This is a real correctness item, not cosmetic.

MODIFIED (minimal):

6. **`SCMDatanodeProtocolServer`** — hoist the already-constructed
   `StorageContainerDatanodeProtocolServerSideTranslatorPB`
   `[V-source: SCMDatanodeProtocolServer.java:165-170]` into a field, construct
   `ScmDatanodeGrpcServer` (passing that same translator instance) when the enable flag is
   set, and drive its lifecycle ALONGSIDE the existing `datanodeRpcServer`: build in the
   ctor, `start()` in `start()` `[V-source: :192-199]`, `stop()`/`join()` in the existing
   `stop()`/`join()` `[V-source: :441-455]`. Because the lifecycle rides on the existing
   server, it automatically inherits the non-HA-inline vs HA-leader-ready start timing
   `[V-source: StorageContainerManager.java:1518-1521; SCMStateMachine.java:366]`.

UNTOUCHED (the safety boundary of Strategy A): the legacy `ProtobufRpcEngine` server, the
2.5.0 generated classes, the datanode client-side translator, every Java datanode, and the
entire OM/Track-1 path.

## 5. Invariants (safety + compatibility)

- **C1 — existing path unchanged.** No behavioral or wire change to the Hadoop-RPC
  datanode<->SCM path. A Java datanode registering over 9861 must be byte-identical
  before/after. (Strategy A guarantees this structurally; Strategy B would not.)
- **C2 — single handler, single source of truth.** Both transports delegate to the SAME
  `StorageContainerDatanodeProtocolServerSideTranslatorPB` -> same `impl`. No forked
  dispatch logic; SCM's view of a DN is identical regardless of transport.
- **C3 — byte-bridge is schema-faithful.** The protobuf-3<->2.5.0 conversion is pure
  `toByteArray`/`parseFrom` over the IDENTICAL schema; it must never field-map by hand
  (that would silently drift). A required-field or unknown-field mismatch must surface as
  an error, never a partial message.
- **C4 — security-off first; TLS gated.** First cut targets `ozone.security.enabled=false`
  (the trusted-proxy model already used for Track 1/2). The TLS block from
  `GrpcOzoneManagerServer` is gated on `SecurityConfig.isSecurityEnabled() &&
  isGrpcTlsEnabled()` `[V-source: GrpcOzoneManagerServer.java:158-171]` and is simply
  omitted when off. No Kerberos/SASL interceptor exists on OM's gRPC server to replicate
  `[V-source: agent B reuse assessment]`.
- **C5 — DN identity + command-drain semantics are inherited, not re-implemented.** UUID
  stability, once-only command delivery, EC replica-index correctness, and report shapes
  are properties of the Rust DN side (already done, `scm-register-heartbeat.md` S1-S4) and
  of SCM's existing handler. The adapter is pure transport and must add NO semantics.
- **C6 — off by default.** With `ozone.scm.datanode.grpc.enabled=false` the new server is
  never built/started; the change is inert until explicitly enabled.

## 6. Edge conditions / failure modes to model + test

| Condition | Required behavior | Test |
|---|---|---|
| Byte-bridge round-trips a Register/Heartbeat/Version envelope | proto3 -> bytes -> 2.5.0 -> handler -> 2.5.0 -> bytes -> proto3, semantically identical | Java unit: build each envelope on both sides, assert equality after bridge |
| Malformed/foreign bytes in the bridge | surface as a gRPC `INVALID_ARGUMENT`/error, never a partial handler call | Java unit: corrupt bytes -> error, handler not invoked |
| gRPC worker thread has no Hadoop `Call` | seed synthetic `Server.Call`; audit `atIp` gets the gRPC client address, no NPE | Java unit: invoke impl off-IPC-thread, assert no NPE + audit IP set |
| Enable flag false | server not built/started; `getDatanodeRpcAddress()` and 9861 unchanged | Java unit/integration: flag off -> no gRPC port open |
| Real Rust DN registers over gRPC | SCM lists it via `ozone admin datanode list`, HEALTHY, EC pipeline can include it | e2e: Rust DN + Java SCM, assert datanode list + EC pipeline placement |
| Mixed fleet command delivery | a command SCM mints (e.g. ReconstructEC) reaches the Rust DN on a heartbeat and is drained once | e2e: trigger reconstruction, assert Rust DN heals (Track 2 loop) |
| HA SCM | gRPC server starts only on the Ratis leader (rides existing lifecycle) | integration (optional first pass): non-HA only `[I]` |

## 7. Plan (each phase builds; Java phases gated behind the Strategy-A decision)

- **P0 (DONE this session): spec.** This document; the wire model, the 2.5.0/3 fork, the
  change surface, all cited against `ozone-2.0.0`.
- **P1 — build spike: generate the stub.** Stand up the isolated protoc3+grpc generation
  (schema-identical `.proto` copy, distinct package, unshaded io.grpc) and confirm
  `StorageContainerDatanodeProtocolServiceGrpc` + proto3 messages compile in an Ozone build.
  Acceptance: `mvn` builds the new module; the grpc base class exists. This de-risks the
  build before any server code.
- **P2 — byte-bridge + service impl (TDD).** Write the Java unit tests of §6 rows 1-3
  FIRST (bridge round-trip, malformed bytes, off-IPC-thread call-context), then
  `StorageContainerDatanodeGrpcService` to pass them. No server/lifecycle yet.
- **P3 — server + lifecycle + config.** `ScmDatanodeGrpcServer` (modeled on
  `GrpcOzoneManagerServer`), the two config keys, and the `SCMDatanodeProtocolServer`
  ctor/start/stop/join hook. Acceptance: flag on -> port listens; flag off -> inert
  (§6 row 4).
- **P4 — custom dist + Rust DN registration.** Build the patched Ozone dist, run it in the
  existing Docker compose with `ozone.scm.datanode.grpc.enabled=true`, point the Rust DN's
  `OzoneScmClient` at the new gRPC port, and confirm register + heartbeat + datanode-list
  HEALTHY (§6 row 5).
- **P5 — whole-Ozone e2e (mixed fleet).** Real OM + real SCM (patched) + Rust datanode(s);
  a real OM hands the Rust gateway a pipeline pointing at the Rust DN; PUT/GET an EC object
  end to end; then drive a reconstruction and assert the Track-2 self-heal loop over the
  real SCM (§6 rows 6). This is the true acceptance bar (`pending-work` item 4).

## 8. Open questions / decisions to resolve before coding `[I]`

1. **Strategy A vs B** (§3) — the load-bearing call. Recommend A (additive, zero
   blast radius). Needs the maintainer's nod since B would be a project-wide change.
2. **Stub generation mechanism** — schema-identical `.proto` copy in a new module vs a new
   execution in `interface-server`. Leaning: new isolated artifact so `interface-server`'s
   ratis-shading antrun and the 2.5.0 execution are both untouched.
3. **Default gRPC port number** for `ozone.scm.datanode.grpc.port` — pick one not in SCM's
   existing port map (verify against `ScmConfigKeys` + the running cluster).
4. **Upstream vs fork** — is the goal a PR to apache/ozone (then Strategy A + off-by-default
   + a config doc entry matter a lot) or a local patched dist for validation only (then we
   can be more expedient)? This changes how much polish P1-P3 need.
5. **HA** — first pass non-HA only? The lifecycle hook rides the existing server so HA
   "just works" in principle, but proving it is extra scope.

## 9. What is already done vs what this unlocks

Already done (no work here): the Rust DN's compliant SCM client + register/heartbeat state
machine + EC reconstruction loop (Track 2), and the Rust gateway's compliant OM client incl.
the real-OM EC key lifecycle (Track 1, validated this session). This adapter is the missing
Java-side transport that connects them to a real SCM, enabling the all-Rust data plane on a
real Ozone control plane and the whole-Ozone acceptance run.
