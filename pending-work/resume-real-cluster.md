# Resume: real Java Ozone cluster + the Rust OM-client probe

Exact steps to reproduce (and finish) the real-cluster validation on a machine with more
RAM. Everything here was run on the dev box EXCEPT the 5-datanode EC step, which OOM'd in
3.8 GB. The working config + start scripts are in [`ozone-cluster/`](ozone-cluster/).

## Prereqs

```sh
# Java 11 (Ozone 2.0.0 is tested on Java 8/11; Java 17 risks reflective-access issues)
brew install openjdk@11          # or your distro's JDK 11
export JAVA_HOME=/path/to/jdk11

# Prebuilt Ozone (no Maven build needed; ~520 MB extracted)
curl -O https://downloads.apache.org/ozone/2.0.0/ozone-2.0.0.tar.gz
tar xzf ozone-2.0.0.tar.gz
cp pending-work/ozone-cluster/ozone-site.xml ozone-2.0.0/etc/hadoop/ozone-site.xml
```

No Docker is required — Ozone runs as plain JVM processes. (If you DO have Docker, the
easiest path for 5 datanodes is the bundled `ozone-2.0.0/compose/ozone/` compose file with
the datanode service scaled to 5; then skip to "Run the probe".)

## Key facts (verified this session)

- The OM's gRPC server (`GrpcOzoneManagerServer`) starts **unconditionally** on
  `ozone.om.grpc.port` (default **8981**). The Rust `OzoneOmClient` dials `http://host:8981`.
- Security off (`ozone.security.enabled=false`): the Rust client sends only
  `clientId` + `s3Authentication.accessId`; no Kerberos/SASL.
- EC-3-2 needs **5 datanodes**. The single-host limit on the dev box was RAM, not config:
  each Ozone JVM floors at ~300 MB RSS (native: RocksDB/Netty/Ratis/metaspace), so 5 DNs +
  SCM + OM ≈ 2.2 GB RSS — fine on ≥8 GB, OOM on 3.8 GB.
- A datanode registers reliably only if SCM responds within the datanode→SCM RPC timeout.
  On tight heaps the default 5 s timed out; the config here sets it to 60 s + SerialGC. On
  a beefy machine you can raise heaps and drop these workarounds.
- OM and SCM both default their Ratis storage to `${metadata.dirs}/ratis` and COLLIDE on a
  single host; the config gives the OM its own dir (`ozone.om.ratis.storage.dir`). Keep
  that.

## Bring it up (JVM processes, no Docker)

```sh
cd ozone-2.0.0
export PATH="$JAVA_HOME/bin:$PATH" OZONE_CONF_DIR=$PWD/etc/hadoop OZONE_LOG_DIR=/tmp/ozone-data/log
export OZONE_OPTS="-XX:+UseSerialGC"            # on ≥8 GB you can drop this
rm -rf /tmp/ozone-data && mkdir -p /tmp/ozone-data/{metadata,hdds,log,http}

OZONE_HEAPSIZE_MAX=1024 bin/ozone scm --init && OZONE_HEAPSIZE_MAX=1024 bin/ozone --daemon start scm
OZONE_HEAPSIZE_MAX=1024 bin/ozone om  --init && OZONE_HEAPSIZE_MAX=1024 bin/ozone --daemon start om
# Datanode 0 uses the main config:
OZONE_HEAPSIZE_MAX=1024 bin/ozone --daemon start datanode
# Datanodes 1..4 each need a UNIQUE data dir + id dir + ports (see below):
pending-work/ozone-cluster/start-extra-datanodes.sh 4

bin/ozone admin datanode list      # expect 5 nodes
bin/ozone admin safemode wait      # or check the SCM log for "SCM exiting safe mode"
bin/ozone sh volume create /s3v
```

### Multiple datanodes on one host

Each datanode must bind unique ports and use unique storage. Three ports are randomizable
(set `=true`): `hdds.container.ipc.random.port`, `hdds.container.ratis.ipc.random.port`,
`hdds.container.ratis.datastream.random.port`. The Ratis server/admin/client/replication
ports are NOT randomizable, so `start-extra-datanodes.sh` writes a per-datanode config dir
that offsets them and sets a unique `hdds.datanode.dir` + `ozone.scm.datanode.id.dir` and
`hdds.datanode.http.enabled=false`. (On a real multi-host cluster or Docker, this is moot —
each datanode is its own host/container with the same config.)

## Run the probe (the goal)

```sh
cd /path/to/ozone-rust
cargo build --example probe_real_om
./target/debug/examples/probe_real_om http://127.0.0.1:8981 s3v
```

Expected on a healthy 5-datanode cluster: bucket ops succeed (already proven), and the EC
key lifecycle now completes —
`create_key(EC) → block with 5 members → commit_key → get_key_info → delete_key`. That
proves the Rust OM client's full key-metadata path against the real Java OM. (Actual EC
shard I/O to the datanodes is the separate data-plane gap; see `README.md` item 2.)

## Also worth running on the beefy machine

```sh
cargo test --workspace                       # 230 tests
cargo run --example rust_stack &             # Rust gateway+datanodes+CompliantOm on :9878
robot acceptance/rust_s3_smoke.robot         # 14 S3 acceptance tests (needs robotframework+awscli)
```
