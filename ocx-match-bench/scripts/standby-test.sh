#!/bin/bash
# 主备热备 M1 验证脚本（2026-09-01）：主机完整五进程 + 备机陪跑
#   host: media-driver + Archive + egress + core + ingress（现有完整形态）
#   standby: media-driver + StandbyArchiveMain(复制主机入站 recording + 影子输出录制)
#            + ReplicationIngressBridge(live replay → 备机 core 入站环)
#            + core + egress（影子输出，写备机 Archive）+ ingress(UDP 11001，接管用)
# 主备各自独立媒体驱动目录（/tmp/.../aeron 与 aeron-sb），互不干扰。
#
# 用法: ./standby-test.sh [--tps 100000] [--duration 30] [--pool-secs 10]
#        [--heap 3g] [--cap 1000000] [--symbol BTCUSDT] [--out /tmp/perf-standby]
#        [--failover]  # 正常段后执行阶段 A 故障接管实验（kill 主机 → 备机接管）
#        [--reconcile] # 成交记录级对账（关快照保完整入站；主机段 kill 前、备机段 kill 后各跑一次）
#        [--fund-users N]  # producer 确定性专用用户场景帧（对账 U3 用）
#        [--raft]      # 仲裁层 Raft 模式：3 节点 Ratis 协调组替代单点协调者
#                      #   （--failover 时先 kill 仲裁 leader 验证仲裁组自愈，再 kill 主机）

set -euo pipefail

TPS=100000; DURATION=30; POOL_SECS=10; HEAP=3g; CAP=1000000; SYMBOL=BTCUSDT
OUT=/tmp/perf-standby
FAILOVER=0; RECONCILE=0; FUND_USERS=0; RAFT=0
DRILL_ARB_QUORUM=0; DRILL_PARTITION=0
while [ $# -gt 0 ]; do
    case "$1" in
        --tps) TPS="$2"; shift ;;
        --duration) DURATION="$2"; shift ;;
        --pool-secs) POOL_SECS="$2"; shift ;;
        --heap) HEAP="$2"; shift ;;
        --cap) CAP="$2"; shift ;;
        --symbol) SYMBOL="$2"; shift ;;
        --out) OUT="$2"; shift ;;
        --failover) FAILOVER=1 ;;
        --reconcile) RECONCILE=1 ;;
        --fund-users) FUND_USERS="$2"; shift ;;
        --raft) RAFT=1 ;;
        --drill-arb-quorum) DRILL_ARB_QUORUM=1; RAFT=1 ;;
        --drill-partition) DRILL_PARTITION=1; RAFT=1 ;;
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
pkill -9 -f "com.ocx.MatchCoreBootstrap|com.ocx.ingress.MatchIngressBootstrap|com.ocx.egress.MatchEgressBootstrap|com.ocx.egress.OcxArchiveMain|com.ocx.egress.StandbyArchiveMain|com.ocx.egress.ReplicationIngressBridge|com.ocx.match.aeron.OcxMediaDriverMain|com.ocx.match.aeron.OutboundDrainMain|com.ocx.match.aeron.HaCoordinatorMain|com.ocx.match.aeron.HaRaftCoordinatorMain|ocx-stress-producer|ocx-metrics-exporter" 2>/dev/null || true
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
AERON_SB_DIR="$OUT/aeron-sb"
ARCHIVE_DIR="$OUT/archive"
ARCHIVE_SB_DIR="$OUT/archive-sb"
SB_CONTROL="aeron:udp?endpoint=localhost:18010"
SB_REPLICATION="aeron:udp?endpoint=localhost:18011"

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
    # 录制解耦（2026-09-02）：主机/备机全链路接入 journal bridge——
    # 主机录 inboundJournal，备机复制该 journal 经备机 bridge 转发 live+journal
    "-Docx.journal-bridge.enabled=true"
    "-Daeron.untethered.window.limit.timeout=60000000000"
)
if [ "$RECONCILE" = "1" ]; then
    # 对账需要完整入站 recording：关快照，避免快照后段回收裁头导致幽灵单误报
    ENG_CFG+=("-Docx.snapshot.interval-ms=0")
fi

wait_ready() {
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

run_recon() { # label aeron_dir archive_control out
    local label="$1" dir="$2" ctrl="$3" out="$4"
    # expected 用 ArchiveProbe 逐帧计数（比 sidecar 采样准；10-performance.md §11.3 口径）
    log_info "${label} Archive 成交逐帧计数（ArchiveProbe ${ctrl}）..."
    java $JVM_OPENS "-Docx.aeron.dir=$dir" "-Docx.archive.control-channel=$ctrl" \
        "-Docx.probe.symbol=$SYMBOL" "-Docx.probe.count-trades=true" \
        -cp "$JAR" com.ocx.egress.ArchiveProbe > "$out.probe" 2>&1 || true
    cat "$out.probe" || true
    local expected
    expected=$(rg -o "trade ${SYMBOL} frames=[0-9]+" "$out.probe" 2>/dev/null \
        | tail -1 | sed 's/.*frames=//')
    log_info "${label} Archive 成交记录对账（${ctrl}，expected_trades=${expected:-N/A}）"
    java $JVM_OPENS -Xms1g -Xmx4g -XX:+UseG1GC -XX:ParallelGCThreads=4 \
        -Daeron.driver.timeout=60000 "${ENG_CFG[@]}" \
        ${RECON_EXTRA[@]+"${RECON_EXTRA[@]}"} \
        "-Docx.aeron.dir=$dir" \
        "-Dmatch.wal.archive-control-channel=$ctrl" \
        "-Docx.probe.symbol=$SYMBOL" "-Docx.fund.users=$FUND_USERS" \
        "-Docx.fund.expected-trades=${expected:--1}" \
        -cp "$JAR" com.ocx.match.TradeRecordReconciliationMain \
        > "$out" 2>&1 || true
    cat "$out" || true
}

HOST_MEDIA_PID="" HOST_ARCHIVE_PID="" HOST_EGRESS_PID="" HOST_CORE_PID="" HOST_INGRESS_PID=""
SB_MEDIA_PID="" SB_ARCHIVE_PID="" SB_BRIDGE_PID="" SB_CORE_PID="" SB_EGRESS_PID="" SB_INGRESS_PID=""
COORD_PID="" HOST_SIDECAR_PID="" SB_SIDECAR_PID="" PROD_PID=""
COORD1_PID="" COORD2_PID="" COORD3_PID=""
ARB_MEDIA_PID=""

# 异常退出时兜底清理（正常路径已逐级 TERM+wait，此处只补漏）
cleanup_tracked() {
    for p in "$HOST_MEDIA_PID" "$HOST_ARCHIVE_PID" "$HOST_EGRESS_PID" "$HOST_CORE_PID" "$HOST_INGRESS_PID" \
             "$SB_MEDIA_PID" "$SB_ARCHIVE_PID" "$SB_BRIDGE_PID" "$SB_CORE_PID" "$SB_EGRESS_PID" "$SB_INGRESS_PID" \
             "$HOST_SIDECAR_PID" "$SB_SIDECAR_PID" "$PROD_PID" \
             "$COORD1_PID" "$COORD2_PID" "$COORD3_PID" "$COORD_PID" "$ARB_MEDIA_PID"; do
        [ -n "$p" ] && kill -9 "$p" 2>/dev/null || true
    done
}
trap cleanup_tracked EXIT

# 仲裁层 Raft 模式：引擎心跳扇出到 3 个仲裁节点（各自 UDP listen 53101/53102/53103）
HA_ENDPOINTS="aeron:udp?endpoint=localhost:53101,aeron:udp?endpoint=localhost:53102,aeron:udp?endpoint=localhost:53103"

# ===== 主机五进程 =====
log_info "启动主机媒体驱动 (dir=$AERON_DIR)"
(cd "$OUT" && exec java -Xms256m -Xmx512m $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_DIR" -cp "$JAR" com.ocx.match.aeron.OcxMediaDriverMain) \
    > "$OUT/host-media.out" 2>&1 &
HOST_MEDIA_PID=$!
wait_ready media-driver "$OUT/host-media.out" 主机媒体驱动 "$AERON_DIR"

log_info "启动主机 Archive"
(cd "$OUT" && exec java -Xms512m -Xmx1g -XX:+UseG1GC $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_DIR" "-Docx.archive.dir=$ARCHIVE_DIR" \
    -cp "$JAR" com.ocx.egress.OcxArchiveMain) > "$OUT/host-archive.out" 2>&1 &
HOST_ARCHIVE_PID=$!
wait_ready archive "$OUT/host-archive.out" 主机Archive "$AERON_DIR"

log_info "启动主机 egress"
(cd "$OUT" && exec java -Xms512m -Xmx1g -XX:+UseG1GC $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_DIR" -cp "$JAR" com.ocx.egress.MatchEgressBootstrap) \
    > "$OUT/host-egress.out" 2>&1 &
HOST_EGRESS_PID=$!
wait_ready egress "$OUT/host-egress.out" 主机egress "$AERON_DIR"

# ===== 协调者（独立媒体驱动 aeron-arb：不随主机 kill 受影响；
#        先于引擎 core 就绪：primary 启动即有 ACK，避免启动期误自杀）=====
log_info "启动仲裁媒体驱动（dir=${AERON_ARB_DIR}）"
(cd "$OUT" && exec java -Xms256m -Xmx512m $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_ARB_DIR" -cp "$JAR" com.ocx.match.aeron.OcxMediaDriverMain) \
    > "$OUT/arb-media.out" 2>&1 &
ARB_MEDIA_PID=$!
wait_ready media-driver "$OUT/arb-media.out" 仲裁媒体驱动 "$AERON_ARB_DIR"

if [ "$RAFT" = "1" ]; then
    COORD_OUTS="$OUT/coordinator-arb1.out $OUT/coordinator-arb2.out $OUT/coordinator-arb3.out"
    log_info "启动仲裁层 Raft 协调组（3 节点：arb1/arb2/arb3，UDP 53101/53102/53103，Raft 6001/6002/6003）"
    start_raft_node() { # i：启动/重启仲裁节点 arbi（演练重启复用）
        local i="$1" PORT_UDP=$((53100 + i)) PORT_RAFT=$((6000 + i))
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
            "-Docx.ha.primary.ack.endpoint=aeron:udp?endpoint=localhost:53001" \
            "-Docx.ha.standby.ack.endpoints=aeron:udp?endpoint=localhost:53000" \
            "-Docx.ha.primary-id=1" "-Docx.ha.standby-ids=2" \
            "-Docx.ha.primary-timeout-ms=5000" \
            -cp "$JAR" com.ocx.match.aeron.HaRaftCoordinatorMain) \
            > "$OUT/coordinator-arb$i.out" 2>&1 &
        eval "COORD${i}_PID=\$!"
    }
    for i in 1 2 3; do
        start_raft_node "$i"
    done
    wait_ready coordinator-arb1 "$OUT/coordinator-arb1.out" 仲裁节点arb1 "$AERON_ARB_DIR"
    wait_ready coordinator-arb2 "$OUT/coordinator-arb2.out" 仲裁节点arb2 "$AERON_ARB_DIR"
    wait_ready coordinator-arb3 "$OUT/coordinator-arb3.out" 仲裁节点arb3 "$AERON_ARB_DIR"
else
    COORD_OUTS="$OUT/coordinator.out"
    # 协调者 listen 挪 53010（避开备机 bridge 的 53000；PROMOTE 仍发 53000）
    log_info "启动 HA 协调者（连 ${AERON_ARB_DIR}，心跳超时 5s 自动 PROMOTE）"
    (cd "$OUT" && exec java -Xms256m -Xmx1024m $JVM_OPENS \
        "-Docx.aeron.dir=$AERON_ARB_DIR" \
        "-Docx.ha.udp.listen=aeron:udp?endpoint=0.0.0.0:53010" \
        "-Docx.ha.udp.endpoint=aeron:udp?endpoint=localhost:53000" \
        "-Docx.ha.primary.ack.endpoint=aeron:udp?endpoint=localhost:53001" \
        "-Docx.ha.standby.ack.endpoint=aeron:udp?endpoint=localhost:53000" \
        "-Docx.ha.primary-id=1" "-Docx.ha.standby-id=2" \
        "-Docx.ha.primary-timeout-ms=5000" \
        -cp "$JAR" com.ocx.match.aeron.HaCoordinatorMain) > "$OUT/coordinator.out" 2>&1 &
    COORD_PID=$!
    wait_ready coordinator "$OUT/coordinator.out" HA协调者 "$AERON_ARB_DIR"
fi

log_info "启动主机 core (heap=$HEAP cap=$CAP)"
(cd "$OUT" && exec java "-Xms$HEAP" "-Xmx$HEAP" -Xmn1g -XX:+UseG1GC \
    -XX:MaxGCPauseMillis=20 -XX:+AlwaysPreTouch $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_DIR" \
    "-Docx.ha.udp.listen=aeron:udp?endpoint=0.0.0.0:53001" \
    $( [ "$RAFT" != "1" ] && echo "-Docx.ha.udp.endpoint=aeron:udp?endpoint=localhost:53010" ) \
    $( [ "$RAFT" = "1" ] && echo "-Docx.ha.udp.endpoints=$HA_ENDPOINTS" ) \
    -cp "$JAR" com.ocx.MatchCoreBootstrap) \
    > "$OUT/host-core.out" 2>&1 &
HOST_CORE_PID=$!
wait_ready core "$OUT/host-core.out" 主机core "$AERON_DIR"

log_info "启动主机 ingress"
(cd "$OUT" && exec java -Xms512m -Xmx1g -XX:+UseG1GC $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_DIR" -cp "$JAR" com.ocx.ingress.MatchIngressBootstrap) \
    > "$OUT/host-ingress.out" 2>&1 &
HOST_INGRESS_PID=$!
wait_ready ingress "$OUT/host-ingress.out" 主机ingress "$AERON_DIR"

# ===== producer 池子 =====
PRODUCER_EXTRA=""
[ "$FUND_USERS" -gt 0 ] && PRODUCER_EXTRA="--fund-users $FUND_USERS"
log_info "启动 producer 池子（${TPS} TPS × ${DURATION}s，池 ${POOL_SECS}s，fund-users=${FUND_USERS}）..."
env AERON_SOCKET_SO_SNDBUF=8388608 AERON_SOCKET_SO_RCVBUF=8388608 \
    AERON_RCV_INITIAL_WINDOW_LENGTH=4194304 \
    "$RUST_BIN" --pool-size "$((TPS * POOL_SECS))" --go-aeron-dir "$AERON_DIR" \
    --aeron-endpoint localhost:11000 --profile spot --symbol "$SYMBOL" \
    --instance-id 1 --tps "$TPS" --duration "$DURATION" \
    --price 70000 --spread 200 --depth exp --tick 0.01 --decay 10 --mid-vol 1.0 \
    --batch 500 --side 0 --order-lifetime-secs 2.0 --batch-cancel-ratio 0.000001 \
    --market-ratio 0.10 --stop-ratio 0.02 --iceberg-ratio 0.05 --large-ratio 0.02 --price-regime \
    $PRODUCER_EXTRA \
    > "$OUT/producer.out" 2>&1 &
PROD_PID=$!
for _ in $(seq 1 120); do
    rg -q "POOL_READY" "$OUT/producer.out" 2>/dev/null && break
    sleep 1
done
rg -q "POOL_READY" "$OUT/producer.out" || { log_error "producer 池子预生成超时"; tail -5 "$OUT/producer.out"; exit 1; }
log_info "✅ producer 池子就绪"

# ===== 备机 =====
log_info "启动备机媒体驱动 (dir=$AERON_SB_DIR)"
(cd "$OUT" && exec java -Xms256m -Xmx512m $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_SB_DIR" -cp "$JAR" com.ocx.match.aeron.OcxMediaDriverMain) \
    > "$OUT/sb-media.out" 2>&1 &
SB_MEDIA_PID=$!
wait_ready media-driver "$OUT/sb-media.out" 备机媒体驱动 "$AERON_SB_DIR"

log_info "启动备机 StandbyArchiveMain（复制主机入站 recording → 备机 Archive）"
(cd "$OUT" && exec java -Xms512m -Xmx1g -XX:+UseG1GC $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_SB_DIR" "-Docx.archive.dir=$ARCHIVE_SB_DIR" \
    "-Docx.standby.archive.control=$SB_CONTROL" \
    "-Docx.standby.archive.replication=$SB_REPLICATION" \
    "-Docx.standby.primary.control=aeron:udp?endpoint=localhost:8010" \
    -cp "$JAR" com.ocx.egress.StandbyArchiveMain) > "$OUT/sb-archive.out" 2>&1 &
SB_ARCHIVE_PID=$!
wait_ready standby-archive "$OUT/sb-archive.out" 备机Archive "$AERON_SB_DIR"

log_info "启动备机 ReplicationIngressBridge（live replay → 备机 core 入站环）"
(cd "$OUT" && exec java -Xms256m -Xmx512m $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_SB_DIR" \
    "-Docx.standby.archive.control=$SB_CONTROL" \
    "-Docx.ha.instance-id=2" \
    -cp "$JAR" com.ocx.egress.ReplicationIngressBridge) > "$OUT/sb-bridge.out" 2>&1 &
SB_BRIDGE_PID=$!
wait_ready bridge "$OUT/sb-bridge.out" 备机bridge "$AERON_SB_DIR"

log_info "启动备机 core（恢复连备机 Archive ${SB_CONTROL}）"
(cd "$OUT" && exec java "-Xms$HEAP" "-Xmx$HEAP" -Xmn1g -XX:+UseG1GC \
    -XX:MaxGCPauseMillis=20 -XX:+AlwaysPreTouch $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_SB_DIR" \
    "-Dmatch.wal.archive-control-channel=$SB_CONTROL" \
    "-Docx.standby.inbound-stream=true" \
    "-Docx.ha.instance-id=2" "-Docx.ha.role=standby" \
    $( [ "$RAFT" != "1" ] && echo "-Docx.ha.udp.endpoint=aeron:udp?endpoint=localhost:53010" ) \
    $( [ "$RAFT" = "1" ] && echo "-Docx.ha.udp.endpoints=$HA_ENDPOINTS" ) \
    -cp "$JAR" com.ocx.MatchCoreBootstrap) > "$OUT/sb-core.out" 2>&1 &
SB_CORE_PID=$!
wait_ready core "$OUT/sb-core.out" 备机core "$AERON_SB_DIR"

log_info "启动备机 egress（影子输出：写备机 Archive，接管前下游不消费）"
(cd "$OUT" && exec java -Xms512m -Xmx1g $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_SB_DIR" -cp "$JAR" com.ocx.egress.MatchEgressBootstrap) \
    > "$OUT/sb-egress.out" 2>&1 &
SB_EGRESS_PID=$!
wait_ready egress "$OUT/sb-egress.out" 备机egress "$AERON_SB_DIR"

log_info "启动备机 ingress（收单端点 0.0.0.0:11001，接管后为真实收单端点）"
(cd "$OUT" && exec java -Xms256m -Xmx512m $JVM_OPENS "${ENG_CFG[@]}" \
    "-Docx.aeron.dir=$AERON_SB_DIR" \
    "-Docx.ingress.aeron.udp.endpoint=0.0.0.0:11001" \
    "-Docx.standby.inbound-stream=true" \
    -cp "$JAR" com.ocx.ingress.MatchIngressBootstrap) > "$OUT/sb-ingress.out" 2>&1 &
SB_INGRESS_PID=$!
wait_ready ingress "$OUT/sb-ingress.out" 备机ingress "$AERON_SB_DIR"

# ===== sidecar（主备各一：9201/9202）=====
log_info "启动 sidecar（host :9201 / standby :9202）"
(cd "$OUT" && exec java $JVM_OPENS -jar "$SIDECAR_JAR" \
    "--metrics.aeron.dir=$AERON_DIR" "--metrics.symbols=$SYMBOL" \
    "--metrics.profile=spot" "--server.port=9201") > "$OUT/host-sidecar.out" 2>&1 &
HOST_SIDECAR_PID=$!
(cd "$OUT" && exec java $JVM_OPENS -jar "$SIDECAR_JAR" \
    "--metrics.aeron.dir=$AERON_SB_DIR" "--metrics.symbols=$SYMBOL" \
    "--metrics.profile=spot" "--server.port=9202") > "$OUT/sb-sidecar.out" 2>&1 &
SB_SIDECAR_PID=$!
for _ in $(seq 1 20); do
    curl -s -m 2 -o /dev/null "http://localhost:9201/metrics" && \
    curl -s -m 2 -o /dev/null "http://localhost:9202/metrics" && break
    sleep 1
done

# ===== GO =====
log_info "引擎就绪，发 go 信号 → producer 重放（${DURATION}s）"
java $JVM_OPENS "-Docx.aeron.dir=$AERON_DIR" -cp "$JAR" \
    com.ocx.match.aeron.StressControl --go || true
wait "$PROD_PID" || true
sleep 5

# ===== 主备指标对比 =====
echo ""
echo "━━━ 主备指标对比 ━━━"
metric() { # port metric
    curl -s "http://localhost:$1/metrics" | grep "^$2[ {]" | awk '{print $NF}' | tail -1
}
echo "match_trades_total        host=$(metric 9201 match_trades_total)  standby=$(metric 9202 match_trades_total)"
echo "match_order_book_size     host=$(metric 9201 match_order_book_size)  standby=$(metric 9202 match_order_book_size)"
echo "match_orders_rejected_total host=$(metric 9201 match_orders_rejected_total)  standby=$(metric 9202 match_orders_rejected_total)"
echo "match_engine_up           host=$(metric 9201 match_engine_up)  standby=$(metric 9202 match_engine_up)"
echo ""

# 主机段记录级对账（kill 前：主机 Archive/media driver 尚在运行，入站 recording 完整）
HOST_RECON_FILLS=""
if [ "$RECONCILE" = "1" ]; then
    RECON_EXTRA=()
    # 停主机 egress 让成交 recording 收尾（stopPosition 落定），再逐帧计数 + 对账
    log_info "停止主机 egress（recording 收尾）..."
    [ -n "$HOST_EGRESS_PID" ] && kill -TERM "$HOST_EGRESS_PID" 2>/dev/null || true
    [ -n "$HOST_EGRESS_PID" ] && wait "$HOST_EGRESS_PID" 2>/dev/null || true
    sleep 2
    run_recon "主机" "$AERON_DIR" "aeron:udp?endpoint=localhost:8010" "$OUT/host-recon.out"
    HOST_RECON_FILLS=$(rg -o "成交 fills=[0-9]+" "$OUT/host-recon.out" 2>/dev/null \
        | tail -1 | sed 's/成交 fills=//')
    HOST_RECON_PASS=$(rg -c "RECON_PASS" "$OUT/host-recon.out" 2>/dev/null || echo 0)
    echo "主机对账成交笔数: ${HOST_RECON_FILLS:-N/A}（RECON_PASS=${HOST_RECON_PASS:-0}）"
fi

# ===== 演练 A：仲裁双挂（kill 2/3，无多数派 → 主机 fail-stop、不提升）=====
if [ "$DRILL_ARB_QUORUM" = "1" ]; then
    echo "━━━ 演练 A：仲裁双挂（kill arb2+arb3，组失去多数派）━━━"
    HOST_T0=$(metric 9201 match_trades_total)
    SB_T0=$(metric 9202 match_trades_total)
    log_info "kill arb2 + arb3（保留 1 个节点，Raft 无多数派 → 无 leader → 无 ACK）..."
    [ -n "$COORD2_PID" ] && kill -9 "$COORD2_PID" 2>/dev/null || true
    [ -n "$COORD3_PID" ] && kill -9 "$COORD3_PID" 2>/dev/null || true
    # 主机收不到 LEASE_ACK → MAX_MISSED×1s 后 self-fence（fail-stop）
    HOST_FENCED=0
    for _ in $(seq 1 20); do
        if rg -q "SELF_FENCED" "$OUT/host-core.out" 2>/dev/null; then
            HOST_FENCED=1
            break
        fi
        sleep 1
    done
    sleep 3
    PROMO=$( (rg -c "PROMOTE_SENT" $COORD_OUTS 2>/dev/null || true) \
        | awk -F: '{s+=$1} END {print s+0}')
    HOST_T1=$(metric 9201 match_trades_total)
    SB_T1=$(metric 9202 match_trades_total)
    echo "host SELF_FENCED=${HOST_FENCED}  trades ${HOST_T0}→${HOST_T1}"
    echo "standby trades ${SB_T0}→${SB_T1}  PROMOTE_SENT=${PROMO:-0}"
    if [ "$HOST_FENCED" = "1" ] && [ "${PROMO:-0}" = "0" ] && [ "$SB_T1" = "$SB_T0" ]; then
        echo "✅ 仲裁双挂正确：主机 fail-stop、无提升、备机未接管（无双主风险）"
    else
        echo "❌ 仲裁双挂行为不符（预期：主机 SELF_FENCED、PROMOTE=0、备机平）"
    fi
    # 重启被杀仲裁节点 → 组恢复多数派 → 重新选举
    log_info "重启 arb2/arb3，验证仲裁组恢复..."
    start_raft_node 2
    start_raft_node 3
    REELECTED=0
    for _ in $(seq 1 25); do
        if rg -q "LEADER_ELECTED" $COORD_OUTS 2>/dev/null; then
            REELECTED=1
            break
        fi
        sleep 1
    done
    [ "$REELECTED" = "1" ] && echo "✅ 仲裁组已恢复并重新选举 leader" \
        || echo "❌ 仲裁组 25s 内未恢复（检查 coordinator-arb*.out）"
fi

# ===== 演练 B：网络分区（SIGSTOP 主机 core 模拟与仲裁断连；恢复后主机自熔断防双主）=====
if [ "$DRILL_PARTITION" = "1" ]; then
    echo "━━━ 演练 B：网络分区（SIGSTOP 主机 core → 协调者提升备机 → 恢复后主机自熔断）━━━"
    HOST_T0=$(metric 9201 match_trades_total)
    SB_T0=$(metric 9202 match_trades_total)
    log_info "SIGSTOP 主机 core（主机与仲裁断连/冻结，心跳停发）..."
    [ -n "$HOST_CORE_PID" ] && kill -STOP "$HOST_CORE_PID" || { log_error "无主机 core PID"; exit 1; }
    PROMOTED=0
    for _ in $(seq 1 25); do
        if rg -q "PROMOTE_SENT" $COORD_OUTS 2>/dev/null; then
            PROMOTED=1
            break
        fi
        sleep 1
    done
    sleep 3
    log_info "协调者提升（PROMOTED=${PROMOTED}），SIGCONT 主机 core → 应 self-fence..."
    [ -n "$HOST_CORE_PID" ] && kill -CONT "$HOST_CORE_PID" || true
    HOST_FENCED=0
    for _ in $(seq 1 15); do
        if rg -q "SELF_FENCED" "$OUT/host-core.out" 2>/dev/null; then
            HOST_FENCED=1
            break
        fi
        sleep 1
    done
    sleep 3
    HOST_T1=$(metric 9201 match_trades_total)
    SB_T1=$(metric 9202 match_trades_total)
    echo "host SELF_FENCED=${HOST_FENCED}  trades ${HOST_T0}→${HOST_T1}"
    echo "standby trades ${SB_T0}→${SB_T1}（提升后无新输入不增长属正常）"
    if [ "$HOST_FENCED" = "1" ] && [ "$PROMOTED" = "1" ] && [ "$HOST_T1" = "$HOST_T0" ]; then
        echo "✅ 分区正确：协调者提升备机、主机恢复后自熔断（不再输出，无双主）"
    else
        echo "❌ 分区演练行为不符（预期：PROMOTE=1、主机 SELF_FENCED、主机成交不再增长）"
    fi
fi

if [ "$FAILOVER" = "1" ]; then
    echo "━━━ 阶段 A：故障接管实验 ━━━"
    HOST_TRADES_BEFORE=$(metric 9201 match_trades_total)
    SB_TRADES_BEFORE=$(metric 9202 match_trades_total)
    echo "主机宕机前：host_trades=$HOST_TRADES_BEFORE standby_trades=$SB_TRADES_BEFORE"

    # 0. 仲裁层模式：先 kill 仲裁组 leader，验证仲裁组自愈且主机不被误 fence
    if [ "$RAFT" = "1" ]; then
        log_info "kill 仲裁组 leader，验证仲裁组自愈..."
        LEADER_FILE=""
        for _ in $(seq 1 15); do
            LEADER_FILE=$(rg -l "LEADER_ELECTED|LEADER id=" $COORD_OUTS 2>/dev/null | head -1 || true)
            [ -n "$LEADER_FILE" ] && break
            sleep 1
        done
        if [ -z "$LEADER_FILE" ]; then
            log_error "未发现仲裁 leader（检查 coordinator-arb*.out）"
            tail -5 $COORD_OUTS || true
            exit 1
        fi
        ARB_NUM=${LEADER_FILE##*coordinator-arb}; ARB_NUM=${ARB_NUM%.out}
        eval "LEADER_PID=\$COORD${ARB_NUM}_PID"
        log_info "仲裁 leader=arb${ARB_NUM} (pid=${LEADER_PID:-unknown})，kill -9 模拟仲裁节点宕机"
        [ -n "$LEADER_PID" ] && kill -9 "$LEADER_PID" 2>/dev/null || true

        NEW_LEADER=0
        for _ in $(seq 1 15); do
            for f in $COORD_OUTS; do
                [ "$f" = "$LEADER_FILE" ] && continue
                if rg -q "LEADER_ELECTED" "$f" 2>/dev/null; then
                    NEW_LEADER=1
                    break
                fi
            done
            [ "$NEW_LEADER" = "1" ] && break
            sleep 1
        done
        if [ "$NEW_LEADER" != "1" ]; then
            log_error "仲裁组未在 15s 内选出新 leader"
            tail -10 $COORD_OUTS || true
            exit 1
        fi
        sleep 3
        FENCED=$(rg -c "SELF_FENCED" "$OUT/host-core.out" 2>/dev/null || echo 0)
        if [ "$FENCED" != "0" ]; then
            log_error "仲裁 leader 切换期间主机被误 fence（fail-stop 误触发）"
            exit 1
        fi
        log_info "✅ 仲裁组自愈：新 leader 已当选，主机续租未中断（无 SELF_FENCED）"
    fi

    # 2. kill 主机链路（模拟整机宕机）
    log_info "kill 主机链路（core/ingress/egress/Archive/media）..."
    for p in "$HOST_INGRESS_PID" "$HOST_CORE_PID" "$HOST_EGRESS_PID" "$HOST_ARCHIVE_PID" "$HOST_MEDIA_PID"; do
        [ -n "$p" ] && kill -9 "$p" 2>/dev/null || true
    done
    sleep 3
    log_info "主机已宕机；等待协调者探测心跳超时并自动 PROMOTE（bridge 自动停止）..."
    PROMOTED=0
    for _ in $(seq 1 30); do
        if rg -q "PROMOTE_SENT" $COORD_OUTS 2>/dev/null; then
            PROMOTED=1
            break
        fi
        sleep 1
    done
    if [ "$PROMOTED" != "1" ]; then
        log_error "协调者未发出 PROMOTE"
        tail -10 $COORD_OUTS || true
        exit 1
    fi
    log_info "✅ 协调者已发出 PROMOTE；等待 bridge 停止转发（STANDBY_PROMOTED）..."
    for _ in $(seq 1 15); do
        if rg -q "STANDBY_PROMOTED" "$OUT/sb-bridge.out" 2>/dev/null; then
            break
        fi
        sleep 1
    done
    rg -q "STANDBY_PROMOTED" "$OUT/sb-bridge.out" || log_warn "bridge 未输出 STANDBY_PROMOTED（检查日志）"
    sleep 2

    # 4. 启动 producer2（--instance-id 2 → 备机 ingress 11001）
    log_info "启动 producer2（instance-id=2 → 备机 ingress 11001，${TPS} TPS × ${DURATION}s）..."
    env AERON_SOCKET_SO_SNDBUF=8388608 AERON_SOCKET_SO_RCVBUF=8388608 \
        AERON_RCV_INITIAL_WINDOW_LENGTH=4194304 \
        "$RUST_BIN" --pool-size "$((TPS * POOL_SECS))" --go-aeron-dir "$AERON_SB_DIR" \
        --aeron-endpoint localhost:11001 --profile spot --symbol "$SYMBOL" \
        --instance-id 2 --tps "$TPS" --duration "$DURATION" \
        --price 70000 --spread 200 --depth exp --tick 0.01 --decay 10 --mid-vol 1.0 \
        --batch 500 --side 0 --order-lifetime-secs 2.0 --batch-cancel-ratio 0.000001 \
        --market-ratio 0.10 --stop-ratio 0.02 --iceberg-ratio 0.05 --large-ratio 0.02 --price-regime \
        $PRODUCER_EXTRA \
        > "$OUT/producer2.out" 2>&1 &
    PROD2_PID=$!
    for _ in $(seq 1 120); do
        rg -q "POOL_READY" "$OUT/producer2.out" 2>/dev/null && break
        sleep 1
    done
    rg -q "POOL_READY" "$OUT/producer2.out" || { log_error "producer2 池子预生成超时"; exit 1; }
    java $JVM_OPENS "-Docx.aeron.dir=$AERON_SB_DIR" -cp "$JAR" \
        com.ocx.match.aeron.StressControl --go || true
    wait "$PROD2_PID" || true
    sleep 5

    # 5. 验证：备机接管后输出增长、无回退
    SB_TRADES_AFTER=$(metric 9202 match_trades_total)
    SB_BOOK_AFTER=$(metric 9202 match_order_book_size)
    SB_REJECTED_AFTER=$(metric 9202 match_orders_rejected_total)
    echo "━━━ 接管验证 ━━━"
    echo "standby trades: 接管前=$SB_TRADES_BEFORE → 接管后=$SB_TRADES_AFTER"
    echo "standby book:   接管后=$SB_BOOK_AFTER   rejected=$SB_REJECTED_AFTER"
    if [ -n "$SB_TRADES_BEFORE" ] && [ -n "$SB_TRADES_AFTER" ] \
            && [ "$SB_TRADES_AFTER" -ge "$SB_TRADES_BEFORE" ] 2>/dev/null \
            && [ "$SB_TRADES_AFTER" -gt "$SB_TRADES_BEFORE" ]; then
        echo "✅ 接管成功：备机成交在主机宕机后继续增长（无回退）"
    else
        echo "❌ 接管失败：备机成交未在接管后增长（before=$SB_TRADES_BEFORE after=${SB_TRADES_AFTER}）"
    fi
    echo ""
fi

# ===== 备机段记录级对账（停链前：备机 Archive/media driver 尚在运行）=====
if [ "$RECONCILE" = "1" ]; then
    RECON_EXTRA=("-Docx.standby.inbound-stream=true")
    # 停备机 egress 让影子成交 recording 收尾
    log_info "停止备机 egress（recording 收尾）..."
    [ -n "$SB_EGRESS_PID" ] && kill -TERM "$SB_EGRESS_PID" 2>/dev/null || true
    [ -n "$SB_EGRESS_PID" ] && wait "$SB_EGRESS_PID" 2>/dev/null || true
    sleep 2
    run_recon "备机" "$AERON_SB_DIR" "aeron:udp?endpoint=localhost:18010" "$OUT/sb-recon.out"
    SB_RECON_FILLS=$(rg -o "成交 fills=[0-9]+" "$OUT/sb-recon.out" 2>/dev/null \
        | tail -1 | sed 's/成交 fills=//')
    SB_RECON_PASS=$(rg -c "RECON_PASS" "$OUT/sb-recon.out" 2>/dev/null || echo 0)
    echo ""
    echo "━━━ 主备成交记录衔接 ━━━"
    echo "主机对账 fills=${HOST_RECON_FILLS:-N/A}（kill 前全部成交）"
    echo "备机对账 fills=${SB_RECON_FILLS:-N/A}"
    echo "主机 RECON_PASS=${HOST_RECON_PASS:-0}  备机 RECON_PASS=${SB_RECON_PASS:-0}"
    if [ "$FAILOVER" = "1" ] && [ -n "$HOST_RECON_FILLS" ] && [ -n "$SB_RECON_FILLS" ] \
            && [ -n "${SB_TRADES_BEFORE:-}" ] && [ -n "${SB_TRADES_AFTER:-}" ]; then
        echo "接管后新成交（sidecar 增量）=$((SB_TRADES_AFTER - SB_TRADES_BEFORE))"
        if [ "$SB_RECON_FILLS" -eq $((HOST_RECON_FILLS + SB_TRADES_AFTER - SB_TRADES_BEFORE)) ] 2>/dev/null; then
            echo "✅ 衔接一致：备机成交记录 = 主机成交记录 + 接管后新成交（无重复/无丢失）"
        else
            echo "❌ 衔接不一致（上述数值请人工核对）"
        fi
    fi
    echo ""
fi

# ===== 停链（倒序）=====
log_info "停止链路（倒序）..."
for p in "$SB_INGRESS_PID" "$SB_EGRESS_PID" "$SB_CORE_PID" "$SB_BRIDGE_PID" "$SB_ARCHIVE_PID" "$SB_MEDIA_PID" \
         "$HOST_INGRESS_PID" "$HOST_CORE_PID" "$HOST_EGRESS_PID" "$HOST_ARCHIVE_PID" "$HOST_MEDIA_PID"; do
    [ -n "$p" ] && kill -TERM "$p" 2>/dev/null || true
done
kill -TERM "$HOST_SIDECAR_PID" "$SB_SIDECAR_PID" 2>/dev/null || true
if [ "$RAFT" = "1" ]; then
    kill -TERM "$COORD1_PID" "$COORD2_PID" "$COORD3_PID" 2>/dev/null || true
else
    [ -n "$COORD_PID" ] && kill -TERM "$COORD_PID" 2>/dev/null || true
fi
for p in "$SB_INGRESS_PID" "$SB_EGRESS_PID" "$SB_CORE_PID" "$SB_BRIDGE_PID" "$SB_ARCHIVE_PID" "$SB_MEDIA_PID" \
         "$HOST_INGRESS_PID" "$HOST_CORE_PID" "$HOST_EGRESS_PID" "$HOST_ARCHIVE_PID" "$HOST_MEDIA_PID" \
         "$HOST_SIDECAR_PID" "$SB_SIDECAR_PID"; do
    [ -n "$p" ] && wait "$p" 2>/dev/null || true
done
if [ "$RAFT" = "1" ]; then
    for p in "$COORD1_PID" "$COORD2_PID" "$COORD3_PID"; do
        [ -n "$p" ] && wait "$p" 2>/dev/null || true
    done
else
    [ -n "$COORD_PID" ] && wait "$COORD_PID" 2>/dev/null || true
fi

echo ""
if [ "$RAFT" = "1" ]; then
    echo "━━━ 仲裁层日志摘要 ━━━"
    for f in $COORD_OUTS; do
        echo "--- $(basename "$f") ---"
        rg -o "ARB_READY|LEADER_ELECTED|RENEW_COMMITTED EPOCH=[0-9]+|PROMOTE_SENT[^\"]*|EPOCH=[0-9]+" "$f" 2>/dev/null | tail -4 || true
    done
    echo ""
fi
echo "产物目录: $OUT"
echo "━━━ 完成 ━━━"
