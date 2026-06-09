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

## 3. The protobuf reality: mirror OM exactly — no protobuf-3 migration, no bridge

> **Correction (supersedes an earlier draft of this section).** The earlier draft claimed
> a gRPC stub is "necessarily protobuf-3" and so could not share the existing 2.5.0 handler
> classes, forcing a choice between a byte-bridge and a `ProtobufRpcEngine2` migration.
> Verifying OM's actual mechanism showed that is WRONG: Ozone generates its gRPC stubs with
> **protoc 2.5.0 + the grpc-java plugin** over the SAME proto2/2.5.0 message classes, and
> `io.grpc` marshals them directly. There is no protobuf fork and no bridge — the adapter
> just mirrors OM. The old two-strategy framing is retracted.

The datanode protocol is generated and served with protobuf **2.5.0** under the legacy
`ProtobufRpcEngine` `[V-source: SCMDatanodeProtocolServer.java:81,163; pom.xml:200
proto2.hadooprpc.protobuf.version=2.5.0]`, and `protobuf-java` is pinned to 2.5.0 repo-wide
— there is NO unshaded `com.google.protobuf` 3.x dependency `[V-source: pom.xml:399]`. The
intuition "gRPC needs protobuf-3" is true only of the gRPC RUNTIME libraries, NOT of the
generated message classes. OM proves you can bolt `protoc-gen-grpc-java` onto a proto2/2.5.0
protocol and marshal the 2.5.0 messages over `io.grpc` unchanged:

- OM's gRPC stub is generated by **protoc 2.5.0 + grpc-java** in one execution, output to
  the base unshaded package `[V-source: interface-client/pom.xml:104-120 — protocArtifact
  uses proto2.hadooprpc.protobuf.version; pluginId grpc-java]`.
- `OzoneManagerServiceGrpc` (the gRPC impl) imports the SAME unshaded
  `...proto.OzoneManagerProtocolProtos.OMRequest` the Hadoop-RPC translator uses and calls
  `omTranslator.submitRequest(null, request)` with NO conversion `[V-source:
  OzoneManagerServiceGrpc.java:26,73-76; OzoneManagerProtocolServerSideTranslatorPB.java:50,104-105]`.
- It depends on `io.grpc:grpc-protobuf` to marshal those 2.5.0 messages `[V-source:
  interface-client/pom.xml:56]`. This is the stack serving OM's gRPC on port 8981 today —
  empirically working (we used it for the Track-1 real-OM probe).

**Therefore the SCM datanode adapter mirrors OM 1:1:** generate
`StorageContainerDatanodeProtocolServiceGrpc` with protoc 2.5.0 + grpc-java over the EXISTING
`org.apache.hadoop.hdds.protocol.proto` `SCMDatanodeRequest`/`Response` classes, and the new
gRPC service impl delegates DIRECTLY to the existing
`StorageContainerDatanodeProtocolServerSideTranslatorPB` (the same instance the Hadoop-RPC
server holds) — one handler, one class set, no byte-bridge, no `ProtobufRpcEngine2`
migration, no change to the existing fleet. The DN proto is structurally identical to OM's
(proto2, `java_generic_services=true`, unshaded `org.apache.hadoop.hdds.protocol.proto`
package, which escapes interface-server's `.../scm/proto` ratis-shading antrun `[V-source:
interface-server/pom.xml:120-122]`), so the same recipe applies cleanly.

**The one build-time unknown to confirm in P1:** that `protoc-gen-grpc-java` 1.58 over the
datanode proto compiles and links within the Ozone reactor exactly as it does for OM (the
io.grpc-1.58-marshals-protobuf-2.5.0 stack is unusual but proven by OM). Contingency only,
NOT the plan: if a build surprise ever forced isolation, fall back to a schema-identical
proto3 copy + byte-bridge.

## 4. Change surface — what gets added, what is untouched

ADDED (all new code/config; nothing existing is modified except the lifecycle hook + ctor):

1. **gRPC stub generation** for the datanode service — add a `protoc 2.5.0 + grpc-java`
   execution over the EXISTING `ScmServerDatanodeHeartbeatProtocol.proto` (mirroring OM's
   `compile-protoc-OmGrpc`), generating `StorageContainerDatanodeProtocolServiceGrpc` over
   the same unshaded 2.5.0 message classes. Scope it to that proto (an `<includes>` list) so
   nothing else in `interface-server` changes. `[I]`
2. **`ScmDatanodeGrpcServer`** — a Netty gRPC server, modeled on `GrpcOzoneManagerServer`
   `[V-source: GrpcOzoneManagerServer.java:144-173]` (or the in-SCM
   `InterSCMGrpcProtocolService` `[V-source: InterSCMGrpcProtocolService.java:56-114]`,
   which already lives in `server-scm` and gives the SCM TLS/port pattern — but uses
   ratis-shaded grpc, so model imports on `GrpcOzoneManagerServer` to match the unshaded
   stub). Holds boss/worker `NioEventLoopGroup` + read executor + `start()/stop()`.
3. **`StorageContainerDatanodeGrpcService`** — `extends
   StorageContainerDatanodeProtocolServiceGrpc.StorageContainerDatanodeProtocolServiceImplBase`;
   its `submitRequest(req, StreamObserver)` calls the EXISTING translator
   (`translator.submitRequest(null, req)`) DIRECTLY on the shared 2.5.0 classes and completes
   the observer — exactly as `OzoneManagerServiceGrpc` does. No conversion.
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

UNTOUCHED (the safety boundary): the legacy `ProtobufRpcEngine` server, the
2.5.0 generated classes, the datanode client-side translator, every Java datanode, and the
entire OM/Track-1 path.

## 5. Invariants (safety + compatibility)

- **C1 — existing path unchanged.** No behavioral or wire change to the Hadoop-RPC
  datanode<->SCM path. A Java datanode registering over 9861 must be byte-identical
  before/after. The additive design (new grpc execution + new server) guarantees this
  structurally — the existing execution, classes, and RPC server are untouched.
- **C2 — single handler, single source of truth.** Both transports delegate to the SAME
  `StorageContainerDatanodeProtocolServerSideTranslatorPB` -> same `impl`. No forked
  dispatch logic; SCM's view of a DN is identical regardless of transport.
- **C3 — shared classes, no conversion.** The gRPC impl passes the EXACT
  `SCMDatanodeRequest`/`Response` instances to/from the existing translator (the OM pattern),
  so there is no message copy or field-mapping to drift. (Were the P1 contingency proto3-copy
  bridge ever needed, it must be pure `toByteArray`/`parseFrom`, never hand field-mapped.)
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
| gRPC `submitRequest` dispatches each `Type` (Version/Register/Heartbeat) | delegates to the shared translator; response equals the Hadoop-RPC path for the same request | Java unit: drive the impl with a stub handler, assert the translator is called + response returned |
| Handler throws (e.g. IOException) | surface as a gRPC error via `onError`, never a half-written response | Java unit: handler throws -> observer.onError, no onNext |
| gRPC worker thread has no Hadoop `Call` | seed synthetic `Server.Call`; audit `atIp` gets the gRPC client address, no NPE | Java unit: invoke impl off-IPC-thread, assert no NPE + audit IP set |
| Enable flag false | server not built/started; `getDatanodeRpcAddress()` and 9861 unchanged | Java unit/integration: flag off -> no gRPC port open |
| Real Rust DN registers over gRPC | SCM lists it via `ozone admin datanode list`, HEALTHY, EC pipeline can include it | e2e: Rust DN + Java SCM, assert datanode list + EC pipeline placement |
| Mixed fleet command delivery | a command SCM mints (e.g. ReconstructEC) reaches the Rust DN on a heartbeat and is drained once | e2e: trigger reconstruction, assert Rust DN heals (Track 2 loop) |
| HA SCM | gRPC server starts only on the Ratis leader (rides existing lifecycle) | integration (optional first pass): non-HA only `[I]` |

## 7. Plan (each phase builds; Java phases gated behind the maintainer's go-ahead)

- **P0 (DONE this session): spec.** This document; the wire model, the protobuf reality
  (mirror OM, no fork), the change surface, all cited against `ozone-2.0.0`.
- **P1 — build spike: generate the stub.** Add the `protoc 2.5.0 + grpc-java` execution over
  the datanode proto (mirroring `compile-protoc-OmGrpc`) and confirm
  `StorageContainerDatanodeProtocolServiceGrpc` compiles + links over the EXISTING 2.5.0
  classes within the Ozone reactor. Acceptance: `mvn` builds; the grpc base class exists and
  references the same `SCMDatanodeRequest`. De-risks the one build unknown (§3) before any
  server code.
- **P2 — service impl (TDD).** Write the Java unit tests of §6 rows 1-3 FIRST (Type dispatch
  to the shared translator, handler-throws, off-IPC-thread call-context), then
  `StorageContainerDatanodeGrpcService` (direct delegation) to pass them. No server/lifecycle
  yet.
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

1. **Confirm the OM-mirror recipe builds** (§3, P1) — the one remaining technical unknown:
   that protoc-2.5.0 + grpc-java over the datanode proto compiles/links like OM's. No
   strategy fork remains (the byte-bridge / engine-migration framing was retracted).
2. **Stub generation placement** — a new grpc execution scoped via `<includes>` to the
   datanode proto inside `interface-server` (mirroring `compile-protoc-OmGrpc`), vs a separate
   module. Leaning: the includes-scoped execution, leaving the proto2 execution + ratis-shading
   antrun untouched.
3. **Default gRPC port number** for `ozone.scm.datanode.grpc.port` — pick one not in SCM's
   existing port map (verify against `ScmConfigKeys` + the running cluster).
4. **Upstream vs fork** — RESOLVED: target an upstream-quality PR to apache/ozone. So the
   polish bar is REQUIRED, not optional: the additive off-by-default design, a
   config-key documentation entry, unit + integration tests, and clean isolation (no edits
   to the existing 2.5.0 execution or interface-server's ratis-shading antrun). P1-P3 carry
   this bar; P1 should also confirm the change builds cleanly within the full Ozone reactor,
   not just the new module.
5. **HA** — first pass non-HA only? The lifecycle hook rides the existing server so HA
   "just works" in principle, but proving it is extra scope.

## 9. What is already done vs what this unlocks

Already done (no work here): the Rust DN's compliant SCM client + register/heartbeat state
machine + EC reconstruction loop (Track 2), and the Rust gateway's compliant OM client incl.
the real-OM EC key lifecycle (Track 1, validated this session). This adapter is the missing
Java-side transport that connects them to a real SCM, enabling the all-Rust data plane on a
real Ozone control plane and the whole-Ozone acceptance run.
