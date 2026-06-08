# Acceptance testing the Rust S3 gateway

Black-box acceptance tests that drive the **running** Rust S3 gateway with external
S3 clients — the `aws` CLI and Apache Ozone-style Robot Framework smoketests — rather
than the in-process `aws-sdk-s3` integration tests in `crates/ozone-s3-gw/tests/s3_e2e.rs`.

## What is under test, and what is not

The launcher `cargo run --example rust_stack` (in `crates/ozone-s3-gw/examples/`) stands
up, in one process:

- the **real S3 gateway** binary path (`ozone_s3_gw::serve` + `Gateway`) on `:9878`,
- **5 real datanodes** (`DatanodeService`, EC-3-2, one slot each) over temp fjall +
  filesystem stores,
- the **compliant OM** (`test_fixtures::compliant_om::CompliantOm`).

So the EC math, the OM-client `submitRequest(OMRequest)` envelope, the datanode data
path, and the full S3 HTTP surface are all exercised end to end by an external client.

**Deliberately NOT a full Java cluster.** The OM here is the *wire-compliant* in-memory
`CompliantOm` fixture, not a real Apache Ozone Java OM, and the datanodes are the Rust
`DatanodeService`, not Java datanodes. Standing the Rust components up against a real
Java OM/SCM cluster additionally requires: (1) the OM's gRPC transport enabled
(`ozone.om.transport.class=...GrpcOmTransportFactory`, port 8981 — config only); (2) an
SCM-side gRPC adapter for the datanode protocol, which stock Ozone does not ship (the
datanode↔SCM protocol is Hadoop-RPC); and (3) a build/runtime environment with Docker
+ a built Ozone dist. None of those are present in the dev sandbox where this was run.

**Auth is trust-the-proxy.** The gateway takes the SigV4 access-key id as the principal
and does NOT verify the signature (a fronting proxy is expected to attest the caller),
so any credentials work and the upstream suite's signature-rejection cases are omitted.

## Run it

```sh
# 1. start the stack (blocks; prints "READY")
cargo run --release --example rust_stack          # gateway on http://127.0.0.1:9878

# 2a. Robot Framework S3 smoketests (operation coverage mirrors Ozone's s3 smoketests)
python3 -m venv ~/.venvs/ozone-test
~/.venvs/ozone-test/bin/pip install robotframework awscli
PATH=~/.venvs/ozone-test/bin:$PATH robot acceptance/rust_s3_smoke.robot

# 2b. or raw aws CLI
AWS_ACCESS_KEY_ID=u AWS_SECRET_ACCESS_KEY=s AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url http://127.0.0.1:9878 s3api create-bucket --bucket b
```

## Last run

`rust_s3_smoke.robot`: **14/14 passed** — bucket create/head/list, missing-bucket 404,
object put/get (byte-identical) / head / zero-byte / prefix list, missing-key `NoSuchKey`,
multipart upload (2 parts → AWS `...-N` ETag, assembled object byte-identical),
list-parts, abort, copy-object, delete, batch delete. A raw `aws` CLI run of the same
operations (including a 7 MiB multipart) also round-trips byte-identical.
