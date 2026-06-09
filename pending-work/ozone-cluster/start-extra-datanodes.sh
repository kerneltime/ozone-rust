#!/usr/bin/env bash
# Start N extra Apache Ozone datanodes (indexes 1..N) on a SINGLE host as JVM processes,
# each with its own config dir, storage, pid/log dir, and offset ports. Datanode 0 uses
# the main config and is started separately (see ../resume-real-cluster.md).
#
# This is the NO-DOCKER path. On a real machine with Docker, scaling the bundled
# `ozone-2.0.0/compose/ozone/` datanode service to 5 is simpler and avoids all the
# per-host port juggling below.
#
# Required env: OZONE_HOME (ozone dist dir), OZONE_CONF_DIR (the main etc/hadoop), JAVA_HOME.
# Usage: start-extra-datanodes.sh [N=4] ; tune DN_HEAP (MB).
#
# NOTE: the port KEYS below are best-effort from Ozone 2.0.0's reported datanode ports
# (STANDALONE/RATIS/RATIS_ADMIN/RATIS_SERVER/REPLICATION/CLIENT_RPC). If a datanode fails
# to start with a bind/"address in use" error, check its log under /tmp/ozone-data/dnI/log
# and adjust the offending key. The three *.random.port flags are an alternative for the
# ipc/ratis-ipc/datastream ports.
set -euo pipefail
N=${1:-4}
: "${OZONE_HOME:?set OZONE_HOME to the ozone-2.0.0 dir}"
: "${OZONE_CONF_DIR:?set OZONE_CONF_DIR to ozone-2.0.0/etc/hadoop}"
OZ="$OZONE_HOME/bin/ozone"
HEAP=${DN_HEAP:-1024}
export OZONE_OPTS="${OZONE_OPTS:--XX:+UseSerialGC}"

for i in $(seq 1 "$N"); do
  off=$((i * 20))
  base=/tmp/ozone-data/dn$i
  conf=$base/conf
  mkdir -p "$base"/{hdds,id,log} "$conf"
  cp -r "$OZONE_CONF_DIR"/* "$conf"/
  python3 - "$conf/ozone-site.xml" "$i" "$off" <<'PY'
import sys
path, i, off = sys.argv[1], sys.argv[2], int(sys.argv[3])
props = {
    "hdds.datanode.dir": f"/tmp/ozone-data/dn{i}/hdds",
    "ozone.scm.datanode.id.dir": f"/tmp/ozone-data/dn{i}/id",
    "hdds.datanode.http.enabled": "false",
    "hdds.container.ipc.port": 9859 + off,
    "hdds.container.ratis.ipc.port": 9858 + off,
    "hdds.container.ratis.server.port": 9856 + off,
    "hdds.container.ratis.admin.port": 9857 + off,
    "hdds.container.ratis.datastream.port": 9855 + off,
    "hdds.datanode.client.port": 19864 + off,
    "hdds.datanode.replication.port": 9886 + off,
}
xml = "".join(
    f"  <property><name>{k}</name><value>{v}</value></property>\n" for k, v in props.items()
)
s = open(path).read().replace("</configuration>", xml + "</configuration>")
open(path, "w").write(s)
PY
  OZONE_CONF_DIR="$conf" OZONE_LOG_DIR="$base/log" OZONE_PID_DIR="$base" \
    OZONE_HEAPSIZE_MAX="$HEAP" "$OZ" --daemon start datanode
  echo "started datanode $i (conf=$conf, port offset +$off, storage=$base)"
done
echo "waiting ~20s for registration, then: $OZ admin datanode list"
