# Pending work

What remains, ordered by value, for resumption on a machine with more RAM (≥8 GB).
Companion: [`../finished-work/`](../finished-work/). Full reasoning is in
[`chat-log/`](chat-log/) (the entire session transcript, my inner thoughts included;
one GitHub token that appeared in it has been redacted — see `chat-log/README.md`).

The two compliance tracks are DONE (see finished-work). What's left is deeper real-cluster
integration that the 3.8 GB / no-swap dev box could not run, plus hardening.

## 1. Full EC key-metadata lifecycle against the REAL Java OM  (needs ≥5 datanodes)

`crates/ozone-om-client/examples/probe_real_om.rs` already validated bucket ops against a
real Java OM, and showed EC `create_key` reaching the OM/SCM and failing only on
`RequiredNodes = 5 AvailableNodes = 1`. With a beefy machine running **5 datanodes** the
same probe should complete `create_key (EC) → commit_key → get_key_info → delete_key`
against the real OM, proving the OM key-metadata path, not just bucket ops.

Step-by-step bring-up (config + scripts here): see
[`resume-real-cluster.md`](resume-real-cluster.md). The blocker was purely RSS: each Ozone
JVM floors at ~300 MB of NATIVE memory (RocksDB/Netty/Ratis/metaspace) regardless of
`-Xmx`, so 5 datanodes + SCM + OM ≈ 2.2 GB RSS did not fit. This is documented with the
measurements in the chat log.

## 2. The data-plane protocol gap  (design decision needed)

The Rust gateway writes/reads EC shards via its OWN gRPC `DatanodeGatewayService`
(`ozone-grpc-types::dn::v1`), NOT Ozone's Java container protocol. So a Rust gateway and a
Java datanode CANNOT exchange shard data, even on a healthy cluster. Two ways to close it:

- **(a) All-Rust fleet against real SCM/OM control plane.** Keep the Rust data plane; make
  the Rust DATANODE join a real SCM. This needs item 3 (SCM gRPC datanode adapter). Then a
  real OM hands the gateway pipelines that point at Rust datanodes, and the existing Rust
  data path works. This matches the project's "only change is adding gRPC support" intent.
- **(b) Speak Ozone's container protocol from the Rust gateway.** Implement Ozone's
  datanode container client (the `XceiverClient`/`ContainerProtocol` over Ratis/standalone)
  in `ozone-dn-client`, so the Rust gateway can write/read shards on REAL Java datanodes.
  Larger, but enables a Rust-gateway-over-stock-Java-cluster deployment.

(a) is the smaller, design-consistent path.

## 3. SCM gRPC datanode adapter  (Java-side change)

Stock Ozone SCM speaks the datanode protocol over Hadoop-RPC (port 9861), NOT gRPC. The
Rust datanode speaks the same `StorageContainerDatanodeProtocol` but over gRPC/tonic. To
let the Rust datanode register with a real SCM, add a thin gRPC transport adapter to SCM's
`SCMDatanodeProtocolServer` (the messages are identical; only the transport differs — the
project's stated "only change being adding gRPC support to SCM"). This was scoped earlier
in the session (see chat log). It is the prerequisite for item 2(a) and for a true
whole-Ozone e2e with the Rust datanode in the fleet.

## 4. Whole-Ozone e2e + Robot acceptance against a real cluster

Once 2+3 land, run Ozone's own `hadoop-ozone/dist/src/main/smoketest` Robot suite (and the
`acceptance/rust_s3_smoke.robot` here) against a cluster that includes the Rust gateway
and/or Rust datanodes — the real acceptance bar.

## 5. Fuzz / adversarial S3 API testing

Property/fuzz tests over the S3 surface (malformed requests, boundary part numbers, range
edge cases, concurrent multipart) against the gateway. `proptest` is already a dev-dep of
`ozone-s3-gw`.

## 6. Minor follow-ups

- `OzoneOmClient::list_buckets` caps at `count=1024`; paginate (loop on `start_key`) for
  volumes with >1024 buckets. Same review for `list_keys`/`list_multipart_uploads` counts
  against a real OM.
- `acceptance/` uses the in-memory `CompliantOm`; once the data plane (item 2) is closed,
  point the acceptance launcher at a real OM too.
- The probe's key lifecycle currently bails on a non-EC (RATIS) block with
  `Missing("pipeline.ec")` because the client is EC-only by design; that's expected — an EC
  bucket (5 DNs) is the correct test, not RATIS.
