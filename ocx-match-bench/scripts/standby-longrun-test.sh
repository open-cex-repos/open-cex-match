#!/bin/bash
# 长时间运行稳定性验证（2026-09-01，P1）：一主一备 + 3 节点仲裁组，
# 低 TPS 持续灌单 + 定期小压测段，全程采样内存/磁盘/错误计数，结尾 kill 主机
# 验证故障切换在长时间运行后仍然正常。
#
# 用法: ./standby-longrun-test.sh [--minutes 60] [--tps 2000] [--burst-tps 20000]
#        [--burst-secs 60] [--burst-interval 600] [--heap 3g] [--sb-heap 2g]
#        [--out /tmp/perf-longrun]

set -euo pipefail

MINUTES=60; TPS=2000; BURST_TPS=20000; BURST_SECS=60; BURST_INTERVAL=600
HEAP=3g; SB_HEAP=2g; SYMBOL=BTCUSDT; OUT=/tmp/perf-longrun
while [ $# -gt 0 ]; do
    case "$1" in
        --minutes) MINUTES="$2"; shift ;;
        --tps) TPS="$2"; shift ;;
        --burst-tps) BURST_TPS="$2"; shift ;;
        --burst-secs) BURST_SECS="$2"; shift ;;
        --burst-interval) BURST_INTERVAL="$2"; shift ;;
        --heap) HEAP="$2"; shift ;;
        --sb-heap) SB_HEAP="$2"; shift ;;
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

log_info "清理残留进程与产物..."
pkill -9 -f "com.ocx.MatchCoreBootstrap|com.ocx.ingress.MatchIngressBootstrap|com.ocx.egress.MatchEgressBootstrap|com.ocx.egress.OcxArchiveMain|com.ocx.egress.StandbyArchiveMain|com.ocx.egress.ReplicationIngressBridge|com.ocx.match.aeron.OcxMediaDriverMain|com.ocx.match.aeron.HaRaftCoordinatorMain|ocx-stress-producer|ocx-metrics-exporter" 2>/dev/null || true
sleep 1
rm -rf "$OUT"
mkdir -p "$OUT"

log_info "构建引擎 jar..."
mvn -q -pl ocx-match-ingress -am package -DskipTests
[ -x "$RUST_BIN" ] || { log_error "缺少 producer 二进制: $RUST_BIN"; exit 1; }

AERON_DIR="$OUT/aeron"
AERON_ARB_DIR="$OUT/aeron-arb"
AERON_SB_DIR="$OUT/aeron-sb"
ARCHIVE_DIR="$OUT/archive"
ARCHIVE_SB_DIR="$OUT/archive-sb"
SB_CONTROL="aeron:udp?endpoint=localhost:18010"
SB_REPLICATION="aeron:udp?endpoint=localhost:18011"
HA_ENDPOINTS="aeron:udp?endpoint=localhost:53101,aeron:udp?endpoint=localhost:53102,aeron:udp?endpoint=localhost:53103"

ENG_CFG=(
    -Dspring.profiles.active=spot
    "-Dspring.match.symbols.$SYMBOL.coin-scale=8"
    "-Dspring.match.symbols.$SYMBOL.base-coin-scale=8"
    "-Dspring.match.max-open-orders=1000000"
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

wait_ready_ctl() {
    local name="$1" out="$2" proc="$3" dir="$4"
    for _ in $(seq 1 60); do
        java $JVM_OPENS "-Docx.aeron.dir=$dir" -cp "$JAR" \
            com.ocx.match.aeron.StressControl --wait-ready "$name" --timeout 5 \
            && return 0
        sleep 1
    done
    log_error "$proc 启动失败"; tail -20 "$out"; exit 1
}
wait_ready_grep() {
    local marker="$1" out="$2" proc="$3"
    for _ in $(seq 1 90); do
        rg -q "$marker" "$out" 2>/dev/null && return 0
        sleep 1
    done
    log_error "$proc 启动失败"; tail -20 "$out"; exit 1
}
metric() { curl -s "http://localhost:$1/metrics" | grep "^$2[ {]" | awk '{print $NF}' | tail -1; }

HOST_MEDIA_PID="" HOST_ARCHIVE_PID="" HOST_EGRESS_PID="" HOST_CORE_PID="" HOST_INGRESS_PID=""
SB_MEDIA_PID="" SB_ARCHIVE_PID="" SB_BRIDGE_PID="" SB_CORE_PID="" SB_EGRESS_PID="" SB_INGRESS_PID=""
COORD1_PID="" COORD2_PID="" COORD3_PID="" ARB_MEDIA_PID=""
HOST_SIDECAR_PID="" SB_SIDECAR_PID="" PROD_PID=""
cleanup_tracked() {
    for p in "$HOST_MEDIA_PID" "$HOST_ARCHIVE_PID" "$HOST_EGRESS_PID" "$HOST_CORE_PID" "$HOST_INGRESS_PID" \
             "$SB_MEDIA_PID" "$SB_ARCHIVE_PID" "$SB_BRIDGE_PID" "$SB_CORE_PID" "$SB_EGRESS_PID" "$SB_INGRESS_PID" \
             "$HOST_SIDECAR_PID" "$SB_SIDECAR_PID" "$PROD_PID" \
             "$COORD1_PID" "$COORD2_PID" "$COORD3_PID" "$ARB_MEDIA_PID"; do
        [ -n "$p" ] && kill -9 "$p" 2>/dev/null || true
    done
}
trap cleanup_tracked EXIT

start_host() {
    log_info "启动主机媒体驱动"
    (cd "$OUT" && exec java -Xms256m -Xmx512m $JVM_OPENS "${ENG_CFG[@]}" \
        "-Docx.aeron.dir=$AERON_DIR" -cp "$JAR" com.ocx.match.aeron.OcxMediaDriverMain) \
        > "$OUT/host-media.out" 2>&1 &
    HOST_MEDIA_PID=$!
    wait_ready_ctl media-driver "$OUT/host-media.out" 主机媒体驱动 "$AERON_DIR"
    log_info "启动主机 Archive/egress/core/ingress"
    (cd "$OUT" && exec java -Xms512m -Xmx1g -XX:+UseG1GC $JVM_OPENS "${ENG_CFG[@]}" \
        "-Docx.aeron.dir=$AERON_DIR" "-Docx.archive.dir=$ARCHIVE_DIR" \
        -cp "$JAR" com.ocx.egress.OcxArchiveMain) > "$OUT/host-archive.out" 2>&1 &
    HOST_ARCHIVE_PID=$!
    wait_ready_ctl archive "$OUT/host-archive.out" 主机Archive "$AERON_DIR"
    (cd "$OUT" && exec java -Xms512m -Xmx1g -XX:+UseG1GC $JVM_OPENS "${ENG_CFG[@]}" \
        "-Docx.aeron.dir=$AERON_DIR" -cp "$JAR" com.ocx.egress.MatchEgressBootstrap) \
        > "$OUT/host-egress.out" 2>&1 &
    HOST_EGRESS_PID=$!
    wait_ready_ctl egress "$OUT/host-egress.out" 主机egress "$AERON_DIR"
    (cd "$OUT" && exec java "-Xms$HEAP" "-Xmx$HEAP" -Xmn1g -XX:+UseG1GC \
        -XX:MaxGCPauseMillis=20 -XX:+AlwaysPreTouch $JVM_OPENS "${ENG_CFG[@]}" \
        "-Docx.aeron.dir=$AERON_DIR" \
        "-Docx.ha.udp.listen=aeron:udp?endpoint=0.0.0.0:53001" \
        "-Docx.ha.udp.endpoints=$HA_ENDPOINTS" \
        -cp "$JAR" com.ocx.MatchCoreBootstrap) > "$OUT/host-core.out" 2>&1 &
    HOST_CORE_PID=$!
    wait_ready_ctl core "$OUT/host-core.out" 主机core "$AERON_DIR"
    (cd "$OUT" && exec java -Xms512m -Xmx1g -XX:+UseG1GC $JVM_OPENS "${ENG_CFG[@]}" \
        "-Docx.aeron.dir=$AERON_DIR" -cp "$JAR" com.ocx.ingress.MatchIngressBootstrap) \
        > "$OUT/host-ingress.out" 2>&1 &
    HOST_INGRESS_PID=$!
    wait_ready_ctl ingress "$OUT/host-ingress.out" 主机ingress "$AERON_DIR"
}

start_arb() {
    log_info "启动仲裁媒体驱动与 3 节点仲裁组"
    (cd "$OUT" && exec java -Xms256m -Xmx512m $JVM_OPENS "${ENG_CFG[@]}" \
        "-Docx.aeron.dir=$AERON_ARB_DIR" -cp "$JAR" com.ocx.match.aeron.OcxMediaDriverMain) \
        > "$OUT/arb-media.out" 2>&1 &
    ARB_MEDIA_PID=$!
    wait_ready_ctl media-driver "$OUT/arb-media.out" 仲裁媒体驱动 "$AERON_ARB_DIR"
    for i in 1 2 3; do
        PORT_UDP=$((53100 + i))
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
    done
    wait_ready_ctl coordinator-arb1 "$OUT/coordinator-arb1.out" 仲裁节点arb1 "$AERON_ARB_DIR"
    wait_ready_ctl coordinator-arb2 "$OUT/coordinator-arb2.out" 仲裁节点arb2 "$AERON_ARB_DIR"
    wait_ready_ctl coordinator-arb3 "$OUT/coordinator-arb3.out" 仲裁节点arb3 "$AERON_ARB_DIR"
}

start_standby() {
    log_info "启动备机链路（media/archive/bridge/core/egress/ingress）"
    (cd "$OUT" && exec java -Xms256m -Xmx512m $JVM_OPENS "${ENG_CFG[@]}" \
        "-Docx.aeron.dir=$AERON_SB_DIR" -cp "$JAR" com.ocx.match.aeron.OcxMediaDriverMain) \
        > "$OUT/sb-media.out" 2>&1 &
    SB_MEDIA_PID=$!
    wait_ready_ctl media-driver "$OUT/sb-media.out" 备机媒体驱动 "$AERON_SB_DIR"
    (cd "$OUT" && exec java -Xms512m -Xmx1g -XX:+UseG1GC $JVM_OPENS "${ENG_CFG[@]}" \
        "-Docx.aeron.dir=$AERON_SB_DIR" "-Docx.archive.dir=$ARCHIVE_SB_DIR" \
        "-Docx.standby.archive.control=$SB_CONTROL" \
        "-Docx.standby.archive.replication=$SB_REPLICATION" \
        "-Docx.standby.primary.control=aeron:udp?endpoint=localhost:8010" \
        -cp "$JAR" com.ocx.egress.StandbyArchiveMain) > "$OUT/sb-archive.out" 2>&1 &
    SB_ARCHIVE_PID=$!
    wait_ready_ctl standby-archive "$OUT/sb-archive.out" 备机Archive "$AERON_SB_DIR"
    (cd "$OUT" && exec java -Xms256m -Xmx512m $JVM_OPENS "${ENG_CFG[@]}" \
        "-Docx.aeron.dir=$AERON_SB_DIR" \
        "-Docx.standby.archive.control=$SB_CONTROL" \
        "-Docx.ha.instance-id=2" \
        -cp "$JAR" com.ocx.egress.ReplicationIngressBridge) > "$OUT/sb-bridge.out" 2>&1 &
    SB_BRIDGE_PID=$!
    wait_ready_ctl bridge "$OUT/sb-bridge.out" 备机bridge "$AERON_SB_DIR"
    (cd "$OUT" && exec java "-Xms$SB_HEAP" "-Xmx$SB_HEAP" -Xmn1g -XX:+UseG1GC \
        -XX:MaxGCPauseMillis=20 -XX:+AlwaysPreTouch $JVM_OPENS "${ENG_CFG[@]}" \
        "-Docx.aeron.dir=$AERON_SB_DIR" \
        "-Dmatch.wal.archive-control-channel=$SB_CONTROL" \
        "-Docx.standby.inbound-stream=true" \
        "-Docx.ha.instance-id=2" "-Docx.ha.role=standby" \
        "-Docx.ha.udp.listen=aeron:udp?endpoint=0.0.0.0:53000" \
        "-Docx.ha.udp.endpoints=$HA_ENDPOINTS" \
        -cp "$JAR" com.ocx.MatchCoreBootstrap) > "$OUT/sb-core.out" 2>&1 &
    SB_CORE_PID=$!
    wait_ready_ctl core "$OUT/sb-core.out" 备机core "$AERON_SB_DIR"
    (cd "$OUT" && exec java -Xms512m -Xmx1g $JVM_OPENS "${ENG_CFG[@]}" \
        "-Docx.aeron.dir=$AERON_SB_DIR" -cp "$JAR" com.ocx.egress.MatchEgressBootstrap) \
        > "$OUT/sb-egress.out" 2>&1 &
    SB_EGRESS_PID=$!
    wait_ready_ctl egress "$OUT/sb-egress.out" 备机egress "$AERON_SB_DIR"
    (cd "$OUT" && exec java -Xms256m -Xmx512m $JVM_OPENS "${ENG_CFG[@]}" \
        "-Docx.aeron.dir=$AERON_SB_DIR" \
        "-Docx.ingress.aeron.udp.endpoint=0.0.0.0:11001" \
        "-Docx.standby.inbound-stream=true" \
        -cp "$JAR" com.ocx.ingress.MatchIngressBootstrap) > "$OUT/sb-ingress.out" 2>&1 &
    SB_INGRESS_PID=$!
    wait_ready_ctl ingress "$OUT/sb-ingress.out" 备机ingress "$AERON_SB_DIR"
}

run_burst() { # label tps secs target(host|standby)
    local label="$1" tps="$2" secs="$3" target="${4:-host}"
    local tdir tport tgo
    if [ "$target" = "standby" ]; then
        tdir="$AERON_SB_DIR"; tport=11001; tgo="$AERON_SB_DIR"
    else
        tdir="$AERON_DIR"; tport=11000; tgo="$AERON_DIR"
    fi
    log_info "压测段 ${label}：${tps} TPS × ${secs}s → ${target}"
    env AERON_SOCKET_SO_SNDBUF=8388608 AERON_SOCKET_SO_RCVBUF=8388608 \
        AERON_RCV_INITIAL_WINDOW_LENGTH=4194304 \
        "$RUST_BIN" --pool-size "$((tps * 5))" --go-aeron-dir "$tgo" \
        --aeron-endpoint "localhost:$tport" --profile spot --symbol "$SYMBOL" \
        --instance-id 1 --tps "$tps" --duration "$secs" \
        --price 70000 --spread 200 --depth exp --tick 0.01 --decay 10 --mid-vol 1.0 \
        --batch 500 --side 0 --order-lifetime-secs 2.0 --batch-cancel-ratio 0.000001 \
        --market-ratio 0.10 --stop-ratio 0.02 --iceberg-ratio 0.05 --large-ratio 0.02 --price-regime \
        > "$OUT/producer-$label.out" 2>&1 &
    local pid=$!
    for _ in $(seq 1 120); do
        rg -q "POOL_READY" "$OUT/producer-$label.out" 2>/dev/null && break
        sleep 1
    done
    java $JVM_OPENS "-Docx.aeron.dir=$tdir" -cp "$JAR" \
        com.ocx.match.aeron.StressControl --go || true
    wait "$pid" || true
    sleep 10   # 等 sidecar 指标收敛（备机回放消化尾部输入）
}

start_host
start_arb
start_standby

log_info "启动 sidecar（host :9201 / standby :9202）"
(cd "$OUT" && exec java $JVM_OPENS -jar "$SIDECAR_JAR" \
    "--metrics.aeron.dir=$AERON_DIR" "--metrics.symbols=$SYMBOL" \
    "--metrics.profile=spot" "--server.port=9201") > "$OUT/host-sidecar.out" 2>&1 &
HOST_SIDECAR_PID=$!
(cd "$OUT" && exec java $JVM_OPENS -jar "$SIDECAR_JAR" \
    "--metrics.aeron.dir=$AERON_SB_DIR" "--metrics.symbols=$SYMBOL" \
    "--metrics.profile=spot" "--server.port=9202") > "$OUT/sb-sidecar.out" 2>&1 &
SB_SIDECAR_PID=$!
for _ in $(seq 1 30); do
    curl -s -m 2 -o /dev/null "http://localhost:9201/metrics" && \
    curl -s -m 2 -o /dev/null "http://localhost:9202/metrics" && break
    sleep 1
done

# ===== 初始压测段（验证链路正常）=====
run_burst init "$TPS" 60

# ===== 长时间观察循环 =====
TOTAL_SECS=$((MINUTES * 60))
START_EPOCH=$(date +%s)
echo "━━━ 长时间运行观察（${MINUTES} 分钟，每 60s 采样）━━━" | tee "$OUT/longrun-report.txt"
echo "时间 | host_trades | sb_trades | host_rss_mb | sb_rss_mb | arb_rss_mb | archive_mb | raft_mb | arb_err | fenced" \
    | tee -a "$OUT/longrun-report.txt"

last_burst=0
while true; do
    NOW=$(date +%s)
    ELAPSED=$((NOW - START_EPOCH))
    if [ "$ELAPSED" -ge "$TOTAL_SECS" ]; then
        break
    fi
    HT=$(metric 9201 match_trades_total); ST=$(metric 9202 match_trades_total)
    HRSS=$(ps -o rss= -p "$HOST_CORE_PID" 2>/dev/null | tr -d ' ')
    SRSS=$(ps -o rss= -p "$SB_CORE_PID" 2>/dev/null | tr -d ' ')
    ARSS=$(ps -o rss= -p "$COORD1_PID" 2>/dev/null | tr -d ' ')
    ARCH=$(du -sm "$ARCHIVE_DIR" "$ARCHIVE_SB_DIR" 2>/dev/null | awk '{s+=$1} END {print s+0}')
    RAFT=$(du -sm "$OUT/raft" 2>/dev/null | awk '{print $1+0}')
    ARB_ERR=$(cat "$OUT"/coordinator-arb*.out 2>/dev/null | rg -c "transient error" || true)
    FENCED=$(cat "$OUT/host-core.out" "$OUT/sb-core.out" 2>/dev/null | rg -c "SELF_FENCED" || true)
    echo "$((ELAPSED / 60))m | $HT | $ST | $((HRSS / 1024)) | $((SRSS / 1024)) | $((ARSS / 1024)) | $ARCH | $RAFT | $ARB_ERR | $FENCED" \
        | tee -a "$OUT/longrun-report.txt"

    # 定期小压测段（验证链路在长时间保持后仍健康）
    if [ "$ELAPSED" -ge "$((last_burst + BURST_INTERVAL))" ]; then
        last_burst=$ELAPSED
        run_burst "burst-$((last_burst / 60))m" "$BURST_TPS" "$BURST_SECS" host
        BT=$(metric 9201 match_trades_total); BTS=$(metric 9202 match_trades_total)
        echo "  burst@$((last_burst / 60))m: host=$BT sb=$BTS $([ "$BT" = "$BTS" ] && echo '一致' || echo '⚠️不一致')" \
            | tee -a "$OUT/longrun-report.txt"
    fi
    sleep 60
done

# ===== 长时间运行后故障切换验证 =====
echo "━━━ 长时间运行后故障切换 ━━━"
HOST_BEFORE=$(metric 9201 match_trades_total)
SB_BEFORE=$(metric 9202 match_trades_total)
log_info "kill 主机链路..."
for p in "$HOST_INGRESS_PID" "$HOST_CORE_PID" "$HOST_EGRESS_PID" "$HOST_ARCHIVE_PID" "$HOST_MEDIA_PID"; do
    [ -n "$p" ] && kill -9 "$p" 2>/dev/null || true
done
sleep 3
PROMOTED=0
for _ in $(seq 1 40); do
    if grep -q "PROMOTE_SENT" "$OUT"/coordinator-arb*.out 2>/dev/null; then PROMOTED=1; break; fi
    sleep 1
done
if [ "$PROMOTED" != "1" ]; then
    log_error "长时间运行后仲裁组未发出 PROMOTE"
    exit 1
fi
run_burst post-failover "$BURST_TPS" 30 standby
SB_AFTER=$(metric 9202 match_trades_total)
echo "接管验证：standby trades $SB_BEFORE → $SB_AFTER"
if [ -n "$SB_BEFORE" ] && [ -n "$SB_AFTER" ] && [ "$SB_AFTER" -gt "$SB_BEFORE" ] 2>/dev/null; then
    echo "✅ 长时间运行后故障切换正常：备机继续撮合（无回退）"
else
    echo "❌ 故障切换失败（before=$SB_BEFORE after=${SB_AFTER}）"
    exit 1
fi

# ===== 汇总 =====
ARB_ERR_TOTAL=$(cat "$OUT"/coordinator-arb*.out 2>/dev/null | rg -c "transient error" || true)
FENCED_TOTAL=$(cat "$OUT/host-core.out" "$OUT/sb-core.out" 2>/dev/null | rg -c "SELF_FENCED" || true)
echo ""
echo "━━━ 长时间运行汇总（${MINUTES} 分钟）━━━"
echo "仲裁节点 transient error 总数: ${ARB_ERR_TOTAL:-0}"
echo "主备 SELF_FENCED 总数: ${FENCED_TOTAL:-0}"
echo "报告: $OUT/longrun-report.txt"
echo "━━━ 完成 ━━━"
