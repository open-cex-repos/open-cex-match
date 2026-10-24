#!/bin/bash
# 一主两备高压验证（2026-09-01）：主机五进程 + 备机1(instance-id=2) + 备机2(instance-id=3)
# 双备机陪跑，仲裁层 Raft 3 节点裁决；kill 仲裁 leader 验证自愈，再 kill 主机验证
# 从两个备机中选一个接管（bestStandby 按复制位置选优，position 相同取小 id）。
#
# 用法: ./standby2-test.sh [--tps 150000] [--duration 15] [--pool-secs 8]
#        [--heap 3g] [--sb-heap 2g] [--cap 1000000] [--symbol BTCUSDT]
#        [--out /tmp/perf-standby2]

set -euo pipefail

TPS=150000; DURATION=15; POOL_SECS=8; HEAP=3g; SB_HEAP=2g; CAP=1000000; SYMBOL=BTCUSDT
OUT=/tmp/perf-standby2
while [ $# -gt 0 ]; do
    case "$1" in
        --tps) TPS="$2"; shift ;;
        --duration) DURATION="$2"; shift ;;
        --pool-secs) POOL_SECS="$2"; shift ;;
        --heap) HEAP="$2"; shift ;;
        --sb-heap) SB_HEAP="$2"; shift ;;
        --cap) CAP="$2"; shift ;;
        --symbol) SYMBOL="$2"; shift ;;
        --out) OUT="$2"; shift ;;
        *) echo "未知参数: $1"; exit 1 ;;
    esac
    shift
done

GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
log_info() { echo -e "${GREEN}[INFO]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# 编排脚本自 2026-09-02 统一在 ocx-match-bench/scripts/ 维护；
# backend 根（含 pom.xml 与各引擎模块）= SCRIPT_DIR/../..
BACKEND_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
JAR="$BACKEND_DIR/ocx-match-ingress/target/ocx-match-ingress-1.0.jar"
SIDECAR_JAR="$BACKEND_DIR/ocx-metrics-exporter/target/ocx-metrics-exporter-1.0.jar"
RUST_BIN="${OCX_PRODUCER_BIN:-$BACKEND_DIR/ocx-stress-producer/target/release/ocx-stress-producer}"

JVM_OPENS="--add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED"

# ===== 清理 =====
log_info "清理残留进程与产物..."
pkill -9 -f "com.ocx.MatchCoreBootstrap|com.ocx.ingress.MatchIngressBootstrap|com.ocx.egress.MatchEgressBootstrap|com.ocx.egress.OcxArchiveMain|com.ocx.egress.StandbyArchiveMain|com.ocx.egress.ReplicationIngressBridge|com.ocx.match.aeron.OcxMediaDriverMain|com.ocx.match.aeron.OutboundDrainMain|com.ocx.match.aeron.HaRaftCoordinatorMain|com.ocx.match.aeron.HaCoordinatorMain|ocx-stress-producer|ocx-metrics-exporter" 2>/dev/null || true
sleep 1
rm -rf "$OUT"
mkdir -p "$OUT"

# ===== 构建 =====
log_info "构建引擎 jar..."
mvn -q -pl ocx-match-ingress -am package -DskipTests
if [ ! -x "$RUST_BIN" ]; then
    log_info "构建 Rust producer..."
    (cd "$BACKEND_DIR/ocx-stress-producer" && cargo build --release)
fi

AERON_DIR="$OUT/aeron"
AERON_ARB_DIR="$OUT/aeron-arb"
AERON_SB1_DIR="$OUT/aeron-sb1"
AERON_SB2_DIR="$OUT/aeron-sb2"
ARCHIVE_DIR="$OUT/archive"
ARCHIVE_SB1_DIR="$OUT/archive-sb1"
ARCHIVE_SB2_DIR="$OUT/archive-sb2"
SB1_CONTROL="aeron:udp?endpoint=localhost:18010"
SB1_REPLICATION="aeron:udp?endpoint=localhost:18011"
SB2_CONTROL="aeron:udp?endpoint=localhost:18020"
SB2_REPLICATION="aeron:udp?endpoint=localhost:18021"
HA_ENDPOINTS="aeron:udp?endpoint=localhost:53101,aeron:udp?endpoint=localhost:53102,aeron:udp?endpoint=localhost:53103"

ENG_CFG=(
    -Dspring.profiles.active=spot
    "-Dspring.match.symbols.$SYMBOL.coin-scale=8"
    "-Dspring.match.symbols.$SYMBOL.base-coin-scale=8"
    "-Dspring.match.max-open-orders=$CAP"
    "-Docx.ingress.aeron.udp.endpoint=0.0.0.0:11000"
    "-Daeron.socket.so_rcvbuf=8388608"
    "-Daeron.socket.so_sndbuf=8388608"
    "-Daeron.rcv.initial.window.length=4194304"
    -Dmatch.wal.enabled=true
    "-Dmatch.wal.data-dir=$OUT/wal"
    "-Dmatch.metrics.data-dir=$OUT/metrics"
    -Dmatch.metrics.lag-trigger=50000000
    -Dmatch.metrics.lag-recover=20000000
    "-Docx.journal-bridge.enabled=true"
    "-Daeron.untethered.window.limit.timeout=60000000000"
)

wait_ready_ctl() { # name out proc dir
    local name="$1" out="$2" proc="$3" dir="$4"
    for _ in $(seq 1 60); do
        java $JVM_OPENS "-Docx.aeron.dir=$dir" -cp "$JAR" \
            com.ocx.match.aeron.StressControl --wait-ready "$name" --timeout 5 \
            && return 0
        sleep 1
    done
    log_error "$proc 启动失败（控制流未收到 READY）"
    tail -20 "$out"
    exit 1
}

wait_ready_grep() { # marker out proc
    local marker="$1" out="$2" proc="$3"
    for _ in $(seq 1 90); do
        rg -q "$marker" "$out" 2>/dev/null && return 0
        sleep 1
    done
    log_error "$proc 启动失败（日志未出现 ${marker}）"
    tail -20 "$out"
    exit 1
}

metric() { # port metric
    curl -s "http://localhost:$1/metrics" | grep "^$2[ {]" | awk '{print $NF}' | tail -1
}

HOST_MEDIA_PID="" HOST_ARCHIVE_PID="" HOST_EGRESS_PID="" HOST_CORE_PID="" HOST_INGRESS_PID=""
SB1_MEDIA_PID="" SB1_ARCHIVE_PID="" SB1_BRIDGE_PID="" SB1_CORE_PID="" SB1_EGRESS_PID="" SB1_INGRESS_PID=""
SB2_MEDIA_PID="" SB2_ARCHIVE_PID="" SB2_BRIDGE_PID="" SB2_CORE_PID="" SB2_EGRESS_PID="" SB2_INGRESS_PID=""
COORD1_PID="" COORD2_PID="" COORD3_PID=""
HOST_SIDECAR_PID="" SB1_SIDECAR_PID="" SB2_SIDECAR_PID="" PROD_PID=""
ARB_MEDIA_PID=""

cleanup_tracked() {
    for p in "$HOST_MEDIA_PID" "$HOST_ARCHIVE_PID" "$HOST_EGRESS_PID" "$HOST_CORE_PID" "$HOST_INGRESS_PID" \
             "$SB1_MEDIA_PID" "$SB1_ARCHIVE_PID" "$SB1_BRIDGE_PID" "$SB1_CORE_PID" "$SB1_EGRESS_PID" "$SB1_INGRESS_PID" \
             "$SB2_MEDIA_PID" "$SB2_ARCHIVE_PID" "$SB2_BRIDGE_PID" "$SB2_CORE_PID" "$SB2_EGRESS_PID" "$SB2_INGRESS_PID" \
             "$HOST_SIDECAR_PID" "$SB1_SIDECAR_PID" "$SB2_SIDECAR_PID" "$PROD_PID" \
             "$COORD1_PID" "$COORD2_PID" "$COORD3_PID" "$ARB_MEDIA_PID"; do
        [ -n "$p" ] && kill -9 "$p" 2>/dev/null || true
    done
}
trap cleanup_tracked EXIT

# ===== 主机五进程 =====
log_info "启动主机媒体驱动 (dir=$AERON_DIR)"
(cd "$OUT" && exec java -Xms256m -Xmx512m $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_DIR" -cp "$JAR" com.ocx.match.aeron.OcxMediaDriverMain) \
    > "$OUT/host-media.out" 2>&1 &
HOST_MEDIA_PID=$!
wait_ready_ctl media-driver "$OUT/host-media.out" 主机媒体驱动 "$AERON_DIR"

log_info "启动主机 Archive"
(cd "$OUT" && exec java -Xms512m -Xmx1g -XX:+UseG1GC $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_DIR" "-Docx.archive.dir=$ARCHIVE_DIR" \
    -cp "$JAR" com.ocx.egress.OcxArchiveMain) > "$OUT/host-archive.out" 2>&1 &
HOST_ARCHIVE_PID=$!
wait_ready_ctl archive "$OUT/host-archive.out" 主机Archive "$AERON_DIR"

log_info "启动主机 egress"
(cd "$OUT" && exec java -Xms512m -Xmx1g -XX:+UseG1GC $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_DIR" -cp "$JAR" com.ocx.egress.MatchEgressBootstrap) \
    > "$OUT/host-egress.out" 2>&1 &
HOST_EGRESS_PID=$!
wait_ready_ctl egress "$OUT/host-egress.out" 主机egress "$AERON_DIR"

# ===== 仲裁层 Raft 协调组（独立媒体驱动 aeron-arb：不随主机/备机 kill 受影响；
#        先于引擎 core 就绪：primary 启动即有 ACK，避免启动期误自杀）=====
log_info "启动仲裁媒体驱动（dir=${AERON_ARB_DIR}）"
(cd "$OUT" && exec java -Xms256m -Xmx512m $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_ARB_DIR" -cp "$JAR" com.ocx.match.aeron.OcxMediaDriverMain) \
    > "$OUT/arb-media.out" 2>&1 &
ARB_MEDIA_PID=$!
wait_ready_ctl media-driver "$OUT/arb-media.out" 仲裁媒体驱动 "$AERON_ARB_DIR"

COORD_OUTS="$OUT/coordinator-arb1.out $OUT/coordinator-arb2.out $OUT/coordinator-arb3.out"
log_info "启动仲裁层 Raft 协调组（arb1/arb2/arb3，UDP 53101/53102/53103，Raft 6001/6002/6003，连仲裁驱动）"
for i in 1 2 3; do
    PORT_UDP=$((53100 + i)); PORT_RAFT=$((6000 + i))
    (cd "$OUT" && exec java -Xms256m -Xmx1024m $JVM_OPENS \
        "-Docx.aeron.dir=$AERON_ARB_DIR" \
        "-Docx.ha.raft.id=arb$i" \
        "-Docx.ha.raft.peers=arb1:127.0.0.1:6001,arb2:127.0.0.1:6002,arb3:127.0.0.1:6003" \
        "-Docx.ha.raft.storage.dir=$OUT/raft/arb$i" \
        "-Docx.ha.metrics.port=$((9700 + i))" \
        "-Docx.ha.raft.lease-ttl-ms=5000" \
        "-Docx.ha.raft.renew-interval-ms=2000" \
        "-Docx.ha.udp.listen=aeron:udp?endpoint=0.0.0.0:$PORT_UDP" \
        "-Docx.ha.udp.endpoint=aeron:udp?endpoint=localhost:53000" \
        "-Docx.ha.promote.endpoints=aeron:udp?endpoint=localhost:53000,aeron:udp?endpoint=localhost:53021" \
        "-Docx.ha.primary.ack.endpoint=aeron:udp?endpoint=localhost:53001" \
        "-Docx.ha.standby.ack.endpoints=aeron:udp?endpoint=localhost:53000,aeron:udp?endpoint=localhost:53020" \
        "-Docx.ha.primary-id=1" "-Docx.ha.standby-ids=2,3" \
        "-Docx.ha.primary-timeout-ms=5000" \
        -cp "$JAR" com.ocx.match.aeron.HaRaftCoordinatorMain) \
        > "$OUT/coordinator-arb$i.out" 2>&1 &
    eval "COORD${i}_PID=\$!"
done
wait_ready_ctl coordinator-arb1 "$OUT/coordinator-arb1.out" 仲裁节点arb1 "$AERON_ARB_DIR"
wait_ready_ctl coordinator-arb2 "$OUT/coordinator-arb2.out" 仲裁节点arb2 "$AERON_ARB_DIR"
wait_ready_ctl coordinator-arb3 "$OUT/coordinator-arb3.out" 仲裁节点arb3 "$AERON_ARB_DIR"

log_info "启动主机 core (heap=$HEAP cap=$CAP)"
(cd "$OUT" && exec java "-Xms$HEAP" "-Xmx$HEAP" -Xmn1g -XX:+UseG1GC \
    -XX:MaxGCPauseMillis=20 -XX:+AlwaysPreTouch $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_DIR" \
    "-Docx.ha.udp.listen=aeron:udp?endpoint=0.0.0.0:53001" \
    "-Docx.ha.udp.endpoints=$HA_ENDPOINTS" \
    -cp "$JAR" com.ocx.MatchCoreBootstrap) \
    > "$OUT/host-core.out" 2>&1 &
HOST_CORE_PID=$!
wait_ready_ctl core "$OUT/host-core.out" 主机core "$AERON_DIR"

log_info "启动主机 ingress"
(cd "$OUT" && exec java -Xms512m -Xmx1g -XX:+UseG1GC $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_DIR" -cp "$JAR" com.ocx.ingress.MatchIngressBootstrap) \
    > "$OUT/host-ingress.out" 2>&1 &
HOST_INGRESS_PID=$!
wait_ready_ctl ingress "$OUT/host-ingress.out" 主机ingress "$AERON_DIR"

# ===== 备机1（instance-id=2）=====
log_info "启动备机1 媒体驱动 (dir=$AERON_SB1_DIR)"
(cd "$OUT" && exec java -Xms256m -Xmx512m $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_SB1_DIR" -cp "$JAR" com.ocx.match.aeron.OcxMediaDriverMain) \
    > "$OUT/sb1-media.out" 2>&1 &
SB1_MEDIA_PID=$!
wait_ready_ctl media-driver "$OUT/sb1-media.out" 备机1媒体驱动 "$AERON_SB1_DIR"

log_info "启动备机1 StandbyArchiveMain（复制主机入站 → 备机1 Archive）"
(cd "$OUT" && exec java -Xms512m -Xmx1g -XX:+UseG1GC $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_SB1_DIR" "-Docx.archive.dir=$ARCHIVE_SB1_DIR" \
    "-Docx.standby.archive.control=$SB1_CONTROL" \
    "-Docx.standby.archive.replication=$SB1_REPLICATION" \
    "-Docx.standby.primary.control=aeron:udp?endpoint=localhost:8010" \
    -cp "$JAR" com.ocx.egress.StandbyArchiveMain) > "$OUT/sb1-archive.out" 2>&1 &
SB1_ARCHIVE_PID=$!
wait_ready_ctl standby-archive "$OUT/sb1-archive.out" 备机1Archive "$AERON_SB1_DIR"

log_info "启动备机1 ReplicationIngressBridge（live replay → 备机1 core 入站环）"
(cd "$OUT" && exec java -Xms256m -Xmx512m $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_SB1_DIR" \
    "-Docx.standby.archive.control=$SB1_CONTROL" \
    "-Docx.ha.instance-id=2" \
    -cp "$JAR" com.ocx.egress.ReplicationIngressBridge) > "$OUT/sb1-bridge.out" 2>&1 &
SB1_BRIDGE_PID=$!
wait_ready_ctl bridge "$OUT/sb1-bridge.out" 备机1bridge "$AERON_SB1_DIR"

log_info "启动备机1 core（恢复连备机1 Archive ${SB1_CONTROL}）"
(cd "$OUT" && exec java "-Xms$SB_HEAP" "-Xmx$SB_HEAP" -Xmn1g -XX:+UseG1GC \
    -XX:MaxGCPauseMillis=20 -XX:+AlwaysPreTouch $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_SB1_DIR" \
    "-Dmatch.wal.archive-control-channel=$SB1_CONTROL" \
    "-Docx.standby.inbound-stream=true" \
    "-Docx.ha.instance-id=2" "-Docx.ha.role=standby" \
    "-Docx.ha.udp.listen=aeron:udp?endpoint=0.0.0.0:53000" \
    "-Docx.ha.udp.endpoints=$HA_ENDPOINTS" \
    -cp "$JAR" com.ocx.MatchCoreBootstrap) > "$OUT/sb1-core.out" 2>&1 &
SB1_CORE_PID=$!
wait_ready_ctl core "$OUT/sb1-core.out" 备机1core "$AERON_SB1_DIR"

log_info "启动备机1 egress（影子输出）"
(cd "$OUT" && exec java -Xms512m -Xmx1g $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_SB1_DIR" -cp "$JAR" com.ocx.egress.MatchEgressBootstrap) \
    > "$OUT/sb1-egress.out" 2>&1 &
SB1_EGRESS_PID=$!
wait_ready_ctl egress "$OUT/sb1-egress.out" 备机1egress "$AERON_SB1_DIR"

log_info "启动备机1 ingress（收单端点 0.0.0.0:11001）"
(cd "$OUT" && exec java -Xms256m -Xmx512m $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_SB1_DIR" \
    "-Docx.ingress.aeron.udp.endpoint=0.0.0.0:11001" \
    "-Docx.standby.inbound-stream=true" \
    -cp "$JAR" com.ocx.ingress.MatchIngressBootstrap) > "$OUT/sb1-ingress.out" 2>&1 &
SB1_INGRESS_PID=$!
wait_ready_ctl ingress "$OUT/sb1-ingress.out" 备机1ingress "$AERON_SB1_DIR"

# ===== 备机2（instance-id=3；就绪用日志标记，READY 控制流名字与备机1相同）=====
log_info "启动备机2 媒体驱动 (dir=$AERON_SB2_DIR)"
(cd "$OUT" && exec java -Xms256m -Xmx512m $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_SB2_DIR" -cp "$JAR" com.ocx.match.aeron.OcxMediaDriverMain) \
    > "$OUT/sb2-media.out" 2>&1 &
SB2_MEDIA_PID=$!
wait_ready_grep "MEDIA_DRIVER_READY" "$OUT/sb2-media.out" 备机2媒体驱动

log_info "启动备机2 StandbyArchiveMain（复制主机入站 → 备机2 Archive）"
(cd "$OUT" && exec java -Xms512m -Xmx1g -XX:+UseG1GC $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_SB2_DIR" "-Docx.archive.dir=$ARCHIVE_SB2_DIR" \
    "-Docx.standby.archive.control=$SB2_CONTROL" \
    "-Docx.standby.archive.replication=$SB2_REPLICATION" \
    "-Docx.standby.primary.control=aeron:udp?endpoint=localhost:8010" \
    -cp "$JAR" com.ocx.egress.StandbyArchiveMain) > "$OUT/sb2-archive.out" 2>&1 &
SB2_ARCHIVE_PID=$!
wait_ready_grep "STANDBY_ARCHIVE_READY" "$OUT/sb2-archive.out" 备机2Archive

log_info "启动备机2 ReplicationIngressBridge（live replay → 备机2 core 入站环）"
(cd "$OUT" && exec java -Xms256m -Xmx512m $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_SB2_DIR" \
    "-Docx.standby.archive.control=$SB2_CONTROL" \
    "-Docx.ha.instance-id=3" \
    "-Docx.ha.udp.listen=aeron:udp?endpoint=0.0.0.0:53021" \
    -cp "$JAR" com.ocx.egress.ReplicationIngressBridge) > "$OUT/sb2-bridge.out" 2>&1 &
SB2_BRIDGE_PID=$!
wait_ready_grep "BRIDGE_READY" "$OUT/sb2-bridge.out" 备机2bridge

log_info "启动备机2 core（恢复连备机2 Archive ${SB2_CONTROL}）"
(cd "$OUT" && exec java "-Xms$SB_HEAP" "-Xmx$SB_HEAP" -Xmn1g -XX:+UseG1GC \
    -XX:MaxGCPauseMillis=20 -XX:+AlwaysPreTouch $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_SB2_DIR" \
    "-Dmatch.wal.archive-control-channel=$SB2_CONTROL" \
    "-Docx.standby.inbound-stream=true" \
    "-Docx.ha.instance-id=3" "-Docx.ha.role=standby" \
    "-Docx.ha.udp.listen=aeron:udp?endpoint=0.0.0.0:53020" \
    "-Docx.ha.udp.endpoints=$HA_ENDPOINTS" \
    -cp "$JAR" com.ocx.MatchCoreBootstrap) > "$OUT/sb2-core.out" 2>&1 &
SB2_CORE_PID=$!
wait_ready_grep "MATCH_CORE_READY" "$OUT/sb2-core.out" 备机2core

log_info "启动备机2 egress（影子输出）"
(cd "$OUT" && exec java -Xms512m -Xmx1g $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_SB2_DIR" -cp "$JAR" com.ocx.egress.MatchEgressBootstrap) \
    > "$OUT/sb2-egress.out" 2>&1 &
SB2_EGRESS_PID=$!
wait_ready_grep "MATCH_EGRESS_READY" "$OUT/sb2-egress.out" 备机2egress

log_info "启动备机2 ingress（收单端点 0.0.0.0:11002）"
(cd "$OUT" && exec java -Xms256m -Xmx512m $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_SB2_DIR" \
    "-Docx.ingress.aeron.udp.endpoint=0.0.0.0:11002" \
    "-Docx.standby.inbound-stream=true" \
    -cp "$JAR" com.ocx.ingress.MatchIngressBootstrap) > "$OUT/sb2-ingress.out" 2>&1 &
SB2_INGRESS_PID=$!
wait_ready_grep "MATCH_INGRESS_READY" "$OUT/sb2-ingress.out" 备机2ingress

# ===== sidecar（9201 host / 9202 sb1 / 9203 sb2）=====
log_info "启动 sidecar（host :9201 / sb1 :9202 / sb2 :9203）"
(cd "$OUT" && exec java $JVM_OPENS -jar "$SIDECAR_JAR" \
    "--metrics.aeron.dir=$AERON_DIR" "--metrics.symbols=$SYMBOL" \
    "--metrics.profile=spot" "--server.port=9201") > "$OUT/host-sidecar.out" 2>&1 &
HOST_SIDECAR_PID=$!
(cd "$OUT" && exec java $JVM_OPENS -jar "$SIDECAR_JAR" \
    "--metrics.aeron.dir=$AERON_SB1_DIR" "--metrics.symbols=$SYMBOL" \
    "--metrics.profile=spot" "--server.port=9202") > "$OUT/sb1-sidecar.out" 2>&1 &
SB1_SIDECAR_PID=$!
(cd "$OUT" && exec java $JVM_OPENS -jar "$SIDECAR_JAR" \
    "--metrics.aeron.dir=$AERON_SB2_DIR" "--metrics.symbols=$SYMBOL" \
    "--metrics.profile=spot" "--server.port=9203") > "$OUT/sb2-sidecar.out" 2>&1 &
SB2_SIDECAR_PID=$!
for _ in $(seq 1 30); do
    curl -s -m 2 -o /dev/null "http://localhost:9201/metrics" && \
    curl -s -m 2 -o /dev/null "http://localhost:9202/metrics" && \
    curl -s -m 2 -o /dev/null "http://localhost:9203/metrics" && break
    sleep 1
done

# ===== GO：第一段压测（producer1 → 主机）=====
log_info "引擎就绪，发 go 信号 → producer1 重放（${TPS} TPS × ${DURATION}s）"
env AERON_SOCKET_SO_SNDBUF=8388608 AERON_SOCKET_SO_RCVBUF=8388608 \
    AERON_RCV_INITIAL_WINDOW_LENGTH=4194304 \
    "$RUST_BIN" --pool-size "$((TPS * POOL_SECS))" --go-aeron-dir "$AERON_DIR" \
    --aeron-endpoint localhost:11000 --profile spot --symbol "$SYMBOL" \
    --instance-id 1 --tps "$TPS" --duration "$DURATION" \
    --price 70000 --spread 200 --depth exp --tick 0.01 --decay 10 --mid-vol 1.0 \
    --batch 500 --side 0 --order-lifetime-secs 2.0 --batch-cancel-ratio 0.000001 \
    --market-ratio 0.10 --stop-ratio 0.02 --iceberg-ratio 0.05 --large-ratio 0.02 --price-regime \
    > "$OUT/producer1.out" 2>&1 &
PROD_PID=$!
for _ in $(seq 1 120); do
    rg -q "POOL_READY" "$OUT/producer1.out" 2>/dev/null && break
    sleep 1
done
rg -q "POOL_READY" "$OUT/producer1.out" || { log_error "producer1 池子预生成超时"; exit 1; }
java $JVM_OPENS "-Docx.aeron.dir=$AERON_DIR" -cp "$JAR" \
    com.ocx.match.aeron.StressControl --go || true
wait "$PROD_PID" || true
sleep 5

echo ""
echo "━━━ 三实例指标对比（${TPS} TPS 高压段）━━━"
echo "match_trades_total        host=$(metric 9201 match_trades_total)  sb1=$(metric 9202 match_trades_total)  sb2=$(metric 9203 match_trades_total)"
echo "match_order_book_size     host=$(metric 9201 match_order_book_size)  sb1=$(metric 9202 match_order_book_size)  sb2=$(metric 9203 match_order_book_size)"
echo "match_orders_rejected_total host=$(metric 9201 match_orders_rejected_total)  sb1=$(metric 9202 match_orders_rejected_total)  sb2=$(metric 9203 match_orders_rejected_total)"
echo ""

HOST_TRADES=$(metric 9201 match_trades_total)
SB1_TRADES=$(metric 9202 match_trades_total)
SB2_TRADES=$(metric 9203 match_trades_total)
if [ "$HOST_TRADES" = "$SB1_TRADES" ] && [ "$HOST_TRADES" = "$SB2_TRADES" ]; then
    echo "✅ 双备机陪跑一致：host=sb1=sb2=$HOST_TRADES"
else
    echo "⚠️ 高压下备机存在复制追赶延迟（回放吞吐≈写入速率）："
    echo "   host=$HOST_TRADES sb1=$SB1_TRADES sb2=$SB2_TRADES"
    echo "   （继续故障接管验证；接管后观察被提升备机是否继续增长、无回退）"
fi

# ===== kill 仲裁 leader：仲裁组自愈 =====
echo "━━━ 仲裁组自愈 ━━━"
LEADER_FILE=""
for _ in $(seq 1 15); do
    LEADER_FILE=$(rg -l "LEADER_ELECTED|LEADER id=" $COORD_OUTS 2>/dev/null | head -1 || true)
    [ -n "$LEADER_FILE" ] && break
    sleep 1
done
if [ -z "$LEADER_FILE" ]; then
    log_error "未发现仲裁 leader"
    exit 1
fi
ARB_NUM=${LEADER_FILE##*coordinator-arb}; ARB_NUM=${ARB_NUM%.out}
eval "LEADER_PID=\$COORD${ARB_NUM}_PID"
log_info "kill 仲裁 leader=arb${ARB_NUM} (pid=${LEADER_PID:-unknown})"
[ -n "$LEADER_PID" ] && kill -9 "$LEADER_PID" 2>/dev/null || true
NEW_LEADER=0
for _ in $(seq 1 15); do
    for f in $COORD_OUTS; do
        [ "$f" = "$LEADER_FILE" ] && continue
        if rg -q "LEADER_ELECTED" "$f" 2>/dev/null; then NEW_LEADER=1; break; fi
    done
    [ "$NEW_LEADER" = "1" ] && break
    sleep 1
done
[ "$NEW_LEADER" = "1" ] || { log_error "仲裁组未选出新 leader"; exit 1; }
sleep 3
FENCED=$(rg -c "SELF_FENCED" "$OUT/host-core.out" 2>/dev/null || echo 0)
if [ "$FENCED" != "0" ]; then
    log_error "仲裁 leader 切换期间主机被误 fence"
    exit 1
fi
log_info "✅ 仲裁组自愈：新 leader 已当选，主机续租未中断"

# ===== kill 主机：仲裁组选备机接管 =====
echo "━━━ 故障接管（双备机选一）━━━"
log_info "kill 主机链路（core/ingress/egress/Archive/media）..."
for p in "$HOST_INGRESS_PID" "$HOST_CORE_PID" "$HOST_EGRESS_PID" "$HOST_ARCHIVE_PID" "$HOST_MEDIA_PID"; do
    [ -n "$p" ] && kill -9 "$p" 2>/dev/null || true
done
sleep 3
log_info "主机已宕机；等待仲裁组租约过期自动 PROMOTE..."
PROMOTED=0
for _ in $(seq 1 40); do
    if grep -q "PROMOTE_SENT" $COORD_OUTS 2>/dev/null; then PROMOTED=1; break; fi
    PCOUNT=$(grep -h "PROMOTE_SENT" $COORD_OUTS 2>/dev/null | wc -l | tr -d ' ' || true)
    LCOUNT=$(wc -l $COORD_OUTS 2>/dev/null | awk '{s+=$1} END {print s+0}' || true)
    echo "[wait-promote] t=${_:-?} promote_hits=${PCOUNT:-0} coord_lines=${LCOUNT:-0}"
    sleep 1
done
if [ "$PROMOTED" != "1" ]; then
    log_error "仲裁组未发出 PROMOTE"
    for f in $COORD_OUTS; do
        echo "--- $(basename "$f") 全文 ---"
        cat "$f" 2>/dev/null || true
    done
    exit 1
fi
NEW_ACTIVE=$(grep -h "PROMOTE_SENT" $COORD_OUTS 2>/dev/null \
    | grep -oE "standby=[0-9]+" | tail -1 | cut -d= -f2 || true)
log_info "✅ 仲裁组已提升 standby=${NEW_ACTIVE:-?}（EPOCH=2）"

if [ "$NEW_ACTIVE" = "2" ]; then
    ACTIVE_DIR="$AERON_SB1_DIR"; ACTIVE_INGRESS=11001
    ACTIVE_BRIDGE_OUT="$OUT/sb1-bridge.out"; ACTIVE_SIDECAR=9202
    PASSIVE_SIDECAR=9203
elif [ "$NEW_ACTIVE" = "3" ]; then
    ACTIVE_DIR="$AERON_SB2_DIR"; ACTIVE_INGRESS=11002
    ACTIVE_BRIDGE_OUT="$OUT/sb2-bridge.out"; ACTIVE_SIDECAR=9203
    PASSIVE_SIDECAR=9202
else
    log_error "无法解析被提升备机: ${NEW_ACTIVE:-empty}"
    exit 1
fi

log_info "等待被提升备机 bridge 停止转发（STANDBY_PROMOTED）..."
for _ in $(seq 1 15); do
    rg -q "STANDBY_PROMOTED" "$ACTIVE_BRIDGE_OUT" 2>/dev/null && break
    sleep 1
done
rg -q "STANDBY_PROMOTED" "$ACTIVE_BRIDGE_OUT" || log_error "bridge 未输出 STANDBY_PROMOTED"
sleep 2

# ===== 第二段压测（producer2 → 被提升备机）=====
SB_TRADES_BEFORE=$(metric "$ACTIVE_SIDECAR" match_trades_total)
log_info "启动 producer2（instance-id=2 → 被提升备机 ingress ${ACTIVE_INGRESS}，${TPS} TPS × ${DURATION}s）..."
env AERON_SOCKET_SO_SNDBUF=8388608 AERON_SOCKET_SO_RCVBUF=8388608 \
    AERON_RCV_INITIAL_WINDOW_LENGTH=4194304 \
    "$RUST_BIN" --pool-size "$((TPS * POOL_SECS))" --go-aeron-dir "$ACTIVE_DIR" \
    --aeron-endpoint "localhost:$ACTIVE_INGRESS" --profile spot --symbol "$SYMBOL" \
    --instance-id 2 --tps "$TPS" --duration "$DURATION" \
    --price 70000 --spread 200 --depth exp --tick 0.01 --decay 10 --mid-vol 1.0 \
    --batch 500 --side 0 --order-lifetime-secs 2.0 --batch-cancel-ratio 0.000001 \
    --market-ratio 0.10 --stop-ratio 0.02 --iceberg-ratio 0.05 --large-ratio 0.02 --price-regime \
    > "$OUT/producer2.out" 2>&1 &
PROD2_PID=$!
for _ in $(seq 1 120); do
    rg -q "POOL_READY" "$OUT/producer2.out" 2>/dev/null && break
    sleep 1
done
rg -q "POOL_READY" "$OUT/producer2.out" || { log_error "producer2 池子预生成超时"; exit 1; }
java $JVM_OPENS "-Docx.aeron.dir=$ACTIVE_DIR" -cp "$JAR" \
    com.ocx.match.aeron.StressControl --go || true
wait "$PROD2_PID" || true
sleep 5

SB_TRADES_AFTER=$(metric "$ACTIVE_SIDECAR" match_trades_total)
SB_BOOK_AFTER=$(metric "$ACTIVE_SIDECAR" match_order_book_size)
PASSIVE_TRADES=$(metric "$PASSIVE_SIDECAR" match_trades_total)
PASSIVE_FENCED=$(rg -c "SELF_FENCED" "$OUT/sb1-core.out" "$OUT/sb2-core.out" 2>/dev/null || echo 0)
echo "━━━ 双备机接管验证 ━━━"
echo "被提升备机(standby=${NEW_ACTIVE}) trades: 接管前=$SB_TRADES_BEFORE → 接管后=$SB_TRADES_AFTER  book=$SB_BOOK_AFTER"
echo "落选备机 trades=${PASSIVE_TRADES}（保持 standby，无新输入不增长属正常）"
echo "两台备机 SELF_FENCED 计数=${PASSIVE_FENCED}"
if [ -n "$SB_TRADES_BEFORE" ] && [ -n "$SB_TRADES_AFTER" ] \
        && [ "$SB_TRADES_AFTER" -gt "$SB_TRADES_BEFORE" ] 2>/dev/null \
        && [ "$PASSIVE_FENCED" = "0" ]; then
    echo "✅ 双备机接管成功：standby=${NEW_ACTIVE} 在主机宕机后继续撮合（无回退），落选备机未误自杀"
else
    echo "❌ 接管验证失败（before=$SB_TRADES_BEFORE after=${SB_TRADES_AFTER} fenced=${PASSIVE_FENCED}）"
    exit 1
fi
echo ""

# ===== 停链 =====
log_info "停止链路..."
for p in "$SB2_INGRESS_PID" "$SB2_EGRESS_PID" "$SB2_CORE_PID" "$SB2_BRIDGE_PID" "$SB2_ARCHIVE_PID" "$SB2_MEDIA_PID" \
         "$SB1_INGRESS_PID" "$SB1_EGRESS_PID" "$SB1_CORE_PID" "$SB1_BRIDGE_PID" "$SB1_ARCHIVE_PID" "$SB1_MEDIA_PID"; do
    [ -n "$p" ] && kill -TERM "$p" 2>/dev/null || true
done
echo ""
echo "━━━ 仲裁层日志摘要 ━━━"
for f in $COORD_OUTS; do
    echo "--- $(basename "$f") ---"
    rg -o "ARB_READY|LEADER_ELECTED|PROMOTE_SENT[^\"]*|RENEW_COMMITTED EPOCH=[0-9]+" "$f" 2>/dev/null | tail -3 || true
done
echo ""
echo "产物目录: $OUT"
echo "━━━ 完成 ━━━"
