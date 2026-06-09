# Flow: S3 gateway → real Ozone Manager (Track 1)

> **STATUS: IMPLEMENTED (Track 1 complete, commits `6d18587..15a7384`).** This is the
> design doc that drove the migration in §5 (B0–B5), retained for rationale. The gateway
> now speaks the real `OzoneManagerService.submitRequest(OMRequest)` via
> `ozone-om-client/src/compliant.rs` (`OzoneOmClient`); the bespoke `om::gw::v1` /
> `om_rust_gateway_v1.proto` / `fake_om` have been DELETED and replaced in tests by the
> wire-compliant `CompliantOm`. References below to the "current" bespoke `backend.rs` /
> `OmClient` describe the PRE-migration state. The OM client has since been validated
> against a REAL Java OM (bucket lifecycle + EC `create_key`) — see `finished-work/`.

Design doc for making the Rust S3 gateway a DROP-IN client of a real Apache Ozone
Manager, speaking OM's actual gRPC contract `OzoneManagerService.submitRequest(OMRequest)
→ OMResponse`. The only OM-side change is enabling its existing gRPC transport. `[V]` =
verified against apache/ozone master (see the Track-1 OM-protocol research output / the
vendored `proto/ozone/OmClientProtocol.proto`); `[I]` = inferred / decision taken.

This mirrors Track 2 (datanode↔SCM): vendor the real wire contract, build a COMPLIANT
client alongside the bespoke one, prove parity, then retire the bespoke path. Track 2 is
the precedent — same method, same rigor (load-bearing tests by mutation, commit each green
increment).

## 1. What the real OM contract is (what we must match)

- **One unary RPC, multiplexed by `cmdType`.** `OmRequest{cmdType:Type, clientId(required
  String), version, traceID?, userInfo?, s3Authentication?, <per-type sub-request>}` →
  `OmResponse{cmdType, status:Status, message?, <per-type sub-response>}`. NO streaming.
  `[V]`
- **Transport: gRPC on the OM gRPC port (default 8981).** The default Java client transport
  is Hadoop-IPC on 9862 (length-prefixed RPC framing + SASL) — we do NOT implement that;
  the gRPC transport carries the IDENTICAL `OmRequest`/`OmResponse` and is plain tonic.
  `[V]` OM must be started with `ozone.om.transport.class=...GrpcOmTransportFactory` (or the
  cluster fronted so the gRPC port is reachable). `[V]`
- **Status is 1-based; success is `status == OK(1)`.** A zero/default status is NOT success.
  Map the error codes the gateway cares about: `KEY_NOT_FOUND=12 → NoSuchKey`,
  `BUCKET_NOT_FOUND=8 → NoSuchBucket`, `BUCKET_ALREADY_EXISTS=10`, `VOLUME_NOT_FOUND=3`,
  `INVALID_REQUEST=39`, `PERMISSION_DENIED=48`, `NO_SUCH_MULTIPART_UPLOAD_ERROR=26`,
  `ENTITY_TOO_SMALL=30`, `INVALID_PART=55`, `INVALID_PART_ORDER=56`. `[V]`
- **Auth with security OFF (our trusted-proxy model).** Minimum accepted request = envelope
  (`cmdType`/`clientId`/`version`) + `s3Authentication.accessId = <proxy-attested principal>`
  (OM derives the owner principal from it; with security off the signature is not
  cryptographically verified). Optionally also set `userInfo.userName = <principal>` (the
  gRPC server honors it when the RPC-context user is null). NO Kerberos/SASL/block-tokens.
  `[V for the fields; I that signature-verification is skipped when security is off]`
- **S3 buckets live under volume `/s3v`** (`ozone.s3g.volume.name`). The gateway sends
  `volumeName="s3v"` directly in every `KeyArgs`/bucket op (non-multitenant; skip
  `GetS3VolumeContext`). `[V]`
- **No pre-handshake** for a single non-HA OM: just `submitRequest`. HA failover (read
  `OmResponse.leaderOMNodeId`, retry the leader) is `[I]` deferred — single-OM first.

### 1a. Write path (the bridge to the Track-2 datanode data plane)

`CreateKey(31)` → `AllocateBlock(37)`* → `CommitKey(36)`, all carrying a `KeyArgs`. `[V]`
- `CreateKey` opens the key (returns `id` = the open/client session id, plus `keyInfo` with
  the first pre-allocated block(s)). EC config travels in `KeyArgs{type=EC,
  ecReplicationConfig=<k,m,codec,chunk>}` — or omit and inherit the bucket default. `[V]`
- Each block to write is a `KeyLocation{blockID{containerID,localID}, offset, length,
  pipeline}`. The `pipeline` (`hadoop.hdds.Pipeline`) carries `members:[DatanodeDetailsProto]`
  and the parallel `memberReplicaIndexes:[u32]` — member `i` holds EC slot
  `memberReplicaIndexes[i]` (1-based: data `1..=k`, parity `k+1..=k+m`). The datanode's data
  endpoint is `ip` + the named data port. `[V for the shape; I for which Port name our Rust
  DN exposes — it registers a "REPLICATION" port to SCM (Track 2), so the OM pipeline carries
  that; confirm at e2e]`
- The gateway EC-encodes each block group into `k+m` shards and writes shard `s` to the
  member whose `memberReplicaIndexes` is `s`, via the Track-2 `DatanodeGatewayService`
  (`DnClient::write_chunk`) — UNCHANGED from today; only how the gateway LEARNS the pipeline
  changes (compliant OM vs bespoke OM).
- `AllocateBlock` (echo `clientID=id`) returns the next block+pipeline when a group fills.
- `CommitKey` finalizes: `KeyArgs.keyLocations = [every block actually written, real
  length/offset]`, `dataSize=total`, `metadata["ETAG"]=<md5>`. Check `status==OK`. `[V]`

### 1b. Read path

S3 uses `GetKeyInfo(111)` (NOT `LookupKey(32)`, which is the FS path). `[V]`
`GetKeyInfoRequest{keyArgs{volumeName="s3v", bucketName, keyName, sortDatanodes=true,
latestVersionLocation=true}}` → `keyInfo.keyLocationList[latest].keyLocations[]` gives each
block's `containerID/localID`, `offset`, `length`, and `pipeline` (members +
memberReplicaIndexes + ecReplicationConfig). The gateway gathers surviving shards and
decodes. ETag = `keyInfo.metadata["ETAG"]`. `[V]`

## 2. The gap vs the current gateway

`backend.rs` (1114 lines) builds the BESPOKE `om::gw::v1` proto directly (`om::CreateKeyRequest`
etc.) via the thin `OmClient` transport wrapper, which "introduces no domain types" — so the
gateway is coupled to the bespoke wire shape. The bespoke `OmRustGatewayService` (one RPC per
op) is a hand-rolled mirror of the real OM ops; its `KeyLocation`/`Pipeline`/`DatanodeDetails`
already resemble the real ones, so the gateway's EC/datanode logic carries over — only the OM
WIRE call changes.

## 3. The compliant client: a DOMAIN method surface (anti-pattern: re-coupling to raw proto)

The bespoke client returned raw proto; the gateway then owned wire↔domain at its boundary.
For the compliant client we do BETTER: `OzoneOmClient` (in `ozone-om-client/src/compliant.rs`)
exposes DOMAIN methods that build the `OmRequest` envelope, submit, check `status==OK`, and
return small gateway-facing domain structs — so `backend.rs` depends on NEITHER OM proto.
This is the single decoupling boundary; do not let `hadoop::ozone` types leak past it.

Domain types (new, minimal — in `ozone-om-client` or `ozone-types`):
```
struct DatanodeSlot { replica_index: u8, endpoint: String }     // endpoint = "http://ip:port"
struct BlockLocation {
    container_id: u64, local_id: u64, offset: u64, length: u64,
    ec: EcReplicationConfig,            // reuse ozone_types::EcReplicationConfig
    members: Vec<DatanodeSlot>,         // EC slot -> datanode data endpoint
}
struct OpenKey { client_id: u64, blocks: Vec<BlockLocation> }   // CreateKey result
struct KeyMeta { size: u64, etag: Option<String>, metadata: Vec<(String,String)>,
                 blocks: Vec<BlockLocation> }                    // GetKeyInfo result
```
Method surface (mirrors the ops backend.rs needs; add incrementally, each tested):
`head_bucket`, `create_bucket`, `delete_bucket`, `list_buckets`; `create_key` → `OpenKey`,
`allocate_block` → `BlockLocation`, `commit_key`, `get_key_info` → `KeyMeta`, `head_key`
(GetKeyInfo without forcing block read), `delete_key`/`delete_keys`; multipart
(`initiate`/`commit_part`/`complete`/`abort`/`list_parts`/`list_uploads`); `list_keys`.

Envelope construction (one private helper): every call stamps `cmdType`, a stable
`clientId` (the gateway's session UUID), `version`, and `s3Authentication.accessId =
principal` (the trusted-proxy-attested caller). Status handling (one private helper):
`OK → Ok`, `KEY_NOT_FOUND/BUCKET_NOT_FOUND → typed domain errors`, else `OmError::Scm-like
{status, message}`.

## 4. Invariants (data safety + auth)

- **W1 status-checked.** Every response is checked `status==OK` before its sub-response is
  read; a non-OK status with a present-but-stale sub-message must NOT be treated as success
  (the 1-based-enum trap). Missing expected sub-response on OK → `Internal`.
- **W2 commit lists what was written.** `CommitKey.keyLocations` reflect the ACTUAL written
  blocks/lengths, never the pre-allocation, or the object is corrupt/truncated on read.
- **W3 EC slot fidelity.** Shard `s` is written to the member with `memberReplicaIndexes==s`;
  a mismatch silently swaps data/parity → unrecoverable corruption. Mirror Track 2's
  1-based discipline.
- **W4 principal attested, never forged.** `accessId` is the proxy-attested principal passed
  through; the gateway does not synthesize or verify it (trust-the-proxy). Never log it as a
  secret, but it is not a credential.
- **A1 auth-off minimum.** Requests carry no Kerberos/token; only `clientId` + `accessId`
  (+ optional `userInfo.userName`). Anti-pattern: sending `s3Authentication.signature` we
  cannot compute — leave it unset (security off ignores it).
- **R1 read uses GetKeyInfo, not LookupKey.** LookupKey is the FS path; GetKeyInfo is the S3
  entrypoint and resolves the s3 context.

## 5. Migration plan (each step builds + tests green, commit + push)

- **B0 DONE** (6d18587): vendored OM+Security protos → `ozone_grpc_types::hadoop::{ozone,
  common}`; `tests/real_om_proto.rs` pins Type/Status/envelope/EC-write-path.
- **B1 CompliantOm fixture** (`test-fixtures/src/compliant_om.rs`): a tonic
  `OzoneManagerService` impl dispatching `submitRequest` by `cmdType`, with an inspectable
  record + a configurable in-memory bucket/key store (enough to drive the gateway). Mirrors
  `CompliantScm`.
- **B2 OzoneOmClient** (`ozone-om-client/src/compliant.rs`): the domain surface in §3 over
  `submitRequest`; unit-tested against the B1 fixture (envelope correctness, status mapping,
  EC pipeline → `BlockLocation` decode, the auth fields).
- **B3 switch backend.rs** to `OzoneOmClient` + the domain types; delete all `om::gw::v1`
  construction from the gateway. The S3 HTTP surface + EC + datanode I/O are unchanged.
- **B4 end-to-end** through the compliant OM: extend `s3_e2e.rs` (real aws-sdk-s3 + 5 real
  datanodes + the CompliantOm + the gateway) to prove PUT/GET/HEAD/DELETE/list/multipart/
  range/degraded-read over the compliant OM path. Reuse the Track-2 datanodes as the data
  plane so OM↔datanode↔gateway is exercised together.
- **B5 retire bespoke OM**: delete `om::gw::v1`/`om_rust_gateway_v1.proto`/`fake_om.rs`/the
  bespoke `OmClient`, once B4 parity holds (the Track-2 B7 precedent).

## 6. Tests (identify-before-implement)

1. envelope: a create_key call emits `OmRequest{cmdType=CreateKey, clientId set,
   s3Authentication.accessId=principal, createKeyRequest.keyArgs{volume=s3v,...}}` (assert via
   the fixture's record). Load-bearing: drop the accessId → assert absent.
2. status mapping: fixture returns `KEY_NOT_FOUND` → client yields the NoSuchKey domain error,
   not a success with empty body (the 1-based trap).
3. EC pipeline decode: fixture returns a block with `pipeline{members,memberReplicaIndexes,
   ecReplicationConfig}` → `BlockLocation.members` maps slot→endpoint correctly (1-based);
   mutation: swap two memberReplicaIndexes → assert the mapping follows the index, not order.
4. write round-trip (B4): PUT an object → the shards land on the datanodes named by the OM
   pipeline at the right slots → GET reads them back byte-identical; degraded GET (kill one
   data shard) still returns the object.
5. commit fidelity (W2): commit_key sends the real written lengths; a single-block tiny object
   skips AllocateBlock; a >block_group object commits multiple blocks.
6. auth-off (A1): no signature field set; the fixture accepts the request (security-off).

## 7. Open questions to resolve during implementation `[I]`

- Which `Port` name our Rust datanode exposes in the OM pipeline (it registers "REPLICATION"
  to SCM in Track 2). Resolve at B4 by reading what the pipeline actually carries.
  **RESOLVED (real-OM probe, 5-DN cluster):** the Rust DN advertises `REPLICATION`, but a
  REAL Java DN's client-facing EC pipeline carries `STANDALONE` (9859), not `REPLICATION`.
  `key_location_to_block` now falls back `REPLICATION → STANDALONE → RATIS → CLIENT_RPC`.
- Whether to reuse `ozone_types::EcReplicationConfig` directly in `BlockLocation` (yes — it
  already models k/p/chunk/codec and the EC crate consumes it).
- Multipart part block layout under the compliant OM (CreateKey with isMultipartKey +
  multipartUploadID + multipartNumber) — defer the exact mapping to B2/B4.
