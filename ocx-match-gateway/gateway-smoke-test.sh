#!/bin/bash
# 网关换发端到端冒烟（2026-09-01）：起引擎链（media/archive/egress/core/ingress）+
# 网关，验证 HTTP 下单 → 三段式 orderId → 引擎撮合成交 → 幂等（重复返回同号）→
# 网关重启后映射恢复。
#
# 用法: ./gateway-smoke-test.sh [--out /tmp/gateway-smoke] [--symbol BTCUSDT]

set -euo pipefail

OUT=/tmp/gateway-smoke; SYMBOL=BTCUSDT
while [ $# -gt 0 ]; do
    case "$1" in
        --out) OUT="$2"; shift ;;
        --symbol) SYMBOL="$2"; shift ;;
        *) echo "未知参数: $1"; exit 1 ;;
    esac
    shift
done

GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
log_info() { echo -e "${GREEN}[INFO]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

BACKEND_DIR="$(cd "$(dirname "$0")/.." && pwd)"
JAR="$BACKEND_DIR/ocx-match-ingress/target/ocx-match-ingress-1.0.jar"
GW_JAR="$BACKEND_DIR/ocx-match-gateway/target/ocx-match-gateway-1.0.jar"
RUST_BIN="${OCX_PRODUCER_BIN:-$BACKEND_DIR/ocx-stress-producer/target/release/ocx-stress-producer}"
JVM_OPENS="--add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED"

log_info "清理残留进程与产物..."
pkill -9 -f "com.ocx.match.aeron.OcxMediaDriverMain|com.ocx.egress.OcxArchiveMain|com.ocx.egress.MatchEgressBootstrap|com.ocx.MatchCoreBootstrap|com.ocx.ingress.MatchIngressBootstrap|com.ocx.gateway.GatewayBootstrap" 2>/dev/null || true
sleep 1
rm -rf "$OUT"
mkdir -p "$OUT"

log_info "构建引擎 + 网关 jar..."
mvn -q -pl ocx-match-ingress -am package -DskipTests
mvn -q -pl ocx-match-gateway package -DskipTests

AERON_DIR="$OUT/aeron"; ARCHIVE_DIR="$OUT/archive"; WAL_DIR="$OUT/wal"; GW_STATE="$OUT/gw-state"
ENG_CFG=(
    -Dspring.profiles.active=spot
    "-Dspring.match.symbols.$SYMBOL.coin-scale=8"
    "-Dspring.match.symbols.$SYMBOL.base-coin-scale=8"
    "-Dspring.match.max-open-orders=1000000"
    "-Docx.ingress.aeron.udp.endpoint=0.0.0.0:11000"
    -Dmatch.wal.enabled=true
    "-Dmatch.wal.data-dir=$WAL_DIR"
)

wait_ready() {
    local name="$1" dir="$2" proc="$3"
    for _ in $(seq 1 60); do
        java $JVM_OPENS "-Docx.aeron.dir=$dir" -cp "$JAR" \
            com.ocx.match.aeron.StressControl --wait-ready "$name" --timeout 5 \
            && return 0
        sleep 1
    done
    log_error "$proc 启动失败"; exit 1
}

PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill -9 "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

log_info "启动引擎链..."
java -Xms256m -Xmx512m $JVM_OPENS "${ENG_CFG[@]}" "-Docx.aeron.dir=$AERON_DIR" \
    -cp "$JAR" com.ocx.match.aeron.OcxMediaDriverMain > "$OUT/media.out" 2>&1 &
PIDS+=($!); wait_ready media-driver "$AERON_DIR" 媒体驱动
java -Xms256m -Xmx512m $JVM_OPENS "${ENG_CFG[@]}" "-Docx.aeron.dir=$AERON_DIR" \
    "-Docx.archive.dir=$ARCHIVE_DIR" -cp "$JAR" com.ocx.egress.OcxArchiveMain > "$OUT/archive.out" 2>&1 &
PIDS+=($!); wait_ready archive "$AERON_DIR" Archive
java -Xms256m -Xmx512m $JVM_OPENS "${ENG_CFG[@]}" "-Docx.aeron.dir=$AERON_DIR" \
    -cp "$JAR" com.ocx.egress.MatchEgressBootstrap > "$OUT/egress.out" 2>&1 &
PIDS+=($!); wait_ready egress "$AERON_DIR" egress
java -Xms512m -Xmx1024m -XX:+UseG1GC $JVM_OPENS "${ENG_CFG[@]}" "-Docx.aeron.dir=$AERON_DIR" \
    -cp "$JAR" com.ocx.MatchCoreBootstrap > "$OUT/core.out" 2>&1 &
PIDS+=($!); wait_ready core "$AERON_DIR" core
java -Xms256m -Xmx512m $JVM_OPENS "${ENG_CFG[@]}" "-Docx.aeron.dir=$AERON_DIR" \
    -cp "$JAR" com.ocx.ingress.MatchIngressBootstrap > "$OUT/ingress.out" 2>&1 &
PIDS+=($!); wait_ready ingress "$AERON_DIR" ingress

log_info "启动网关（HTTP 8080，issuer=100，状态目录 ${GW_STATE}）..."
java -Xms128m -Xmx256m $JVM_OPENS "-Docx.aeron.dir=$AERON_DIR" \
    "-Docx.gateway.http.port=8080" \
    "-Docx.gateway.ingress.endpoint=localhost:11000" \
    "-Docx.gateway.issuer.id=100" \
    "-Docx.gateway.state.dir=$GW_STATE" \
    -cp "$GW_JAR" com.ocx.gateway.GatewayBootstrap > "$OUT/gateway.out" 2>&1 &
GW_PID=$!; PIDS+=($GW_PID)
for _ in $(seq 1 30); do
    rg -q "HTTP_READY" "$OUT/gateway.out" 2>/dev/null && break
    sleep 1
done

echo "━━━ 下单验证 ━━━"
BUY=$(curl -s -X POST "http://localhost:8080/orders?clientOrderId=SMOKE-001&symbol=$SYMBOL&side=0&type=1&price=70000&qty=0.5&userId=7")
SELL=$(curl -s -X POST "http://localhost:8080/orders?clientOrderId=SMOKE-002&symbol=$SYMBOL&side=1&type=1&price=70000&qty=0.5&userId=8")
echo "BUY : $BUY"
echo "SELL: $SELL"
echo "$BUY" | rg -q '"status":"accepted"' || { log_error "买单未 accepted"; exit 1; }
echo "$SELL" | rg -q '"status":"accepted"' || { log_error "卖单未 accepted"; exit 1; }

echo "━━━ 幂等验证（同 clientOrderId 重复提交）━━━"
DUP=$(curl -s -X POST "http://localhost:8080/orders?clientOrderId=SMOKE-001&symbol=$SYMBOL&side=0&price=70000&qty=0.5&userId=7")
echo "DUP: $DUP"
ORDER_ID=$(echo "$BUY" | rg -o '"orderId":[0-9]+' | cut -d: -f2)
[ "$DUP" = "{\"clientOrderId\":\"SMOKE-001\",\"orderId\":$ORDER_ID,\"status\":\"duplicate\"}" ] \
    || { log_error "幂等失败：重复提交未返回同一 orderId/duplicate"; exit 1; }
echo "✅ 幂等：重复提交返回同一 orderId=${ORDER_ID}（duplicate）"

echo "━━━ 撮合成交验证（等 3s 查 Archive）━━━"
sleep 5
echo "--- 小数价格对照单（exponent 与 producer 一致）---"
D3=$(curl -s -X POST "http://localhost:8080/orders?clientOrderId=SMOKE-003&symbol=$SYMBOL&side=0&type=1&price=70000.00000000&qty=0.50000000&userId=9")
D4=$(curl -s -X POST "http://localhost:8080/orders?clientOrderId=SMOKE-004&symbol=$SYMBOL&side=1&type=1&price=70000.00000000&qty=0.50000000&userId=10")
echo "D3: $D3"; echo "D4: $D4"
sleep 2
echo "--- 入站帧解码（recordingId=3，前 4 帧）---"
java $JVM_OPENS "-Docx.aeron.dir=$AERON_DIR" -cp "$JAR" \
    com.ocx.match.aeron.DumpInboundFrames "$AERON_DIR" \
    "aeron:udp?endpoint=localhost:8010" 3 4 > "$OUT/dump.out" 2>&1 || true
cat "$OUT/dump.out" || true
java $JVM_OPENS "-Docx.aeron.dir=$AERON_DIR" "-Docx.archive.control-channel=aeron:udp?endpoint=localhost:8010" \
    "-Docx.probe.symbol=$SYMBOL" "-Docx.probe.count-trades=true" \
    -cp "$JAR" com.ocx.egress.ArchiveProbe > "$OUT/probe-gw.out" 2>&1 || true
GW_TRADES=$(rg -o "trade ${SYMBOL} frames=[0-9]+" "$OUT/probe-gw.out" 2>/dev/null \
    | tail -1 | sed 's/.*frames=//' || true)
echo "网关两单后 Archive 成交帧数: ${GW_TRADES:-N/A}"
echo "--- 对照：producer 发同价单（验证环境链路）---"
env AERON_SOCKET_SO_SNDBUF=8388608 AERON_SOCKET_SO_RCVBUF=8388608 \
    "$RUST_BIN" --pool-size 2000 --go-aeron-dir "$AERON_DIR" \
    --aeron-endpoint localhost:11000 --profile spot --symbol "$SYMBOL" \
    --instance-id 1 --tps 2000 --duration 2 \
    --price 70000 --spread 200 --depth exp --tick 0.01 --decay 10 --mid-vol 1.0 \
    --batch 100 --side 0 --order-lifetime-secs 2.0 --batch-cancel-ratio 0.000001 \
    --market-ratio 0.10 --stop-ratio 0.02 --iceberg-ratio 0.05 --large-ratio 0.02 --price-regime \
    > "$OUT/producer.out" 2>&1 &
PROD_PID=$!
for _ in $(seq 1 60); do
    rg -q "POOL_READY" "$OUT/producer.out" 2>/dev/null && break
    sleep 1
done
java $JVM_OPENS "-Docx.aeron.dir=$AERON_DIR" -cp "$JAR" \
    com.ocx.match.aeron.StressControl --go || true
wait "$PROD_PID" || true
sleep 3
echo "--- producer 对照后入站帧解码（前 6 帧）---"
java $JVM_OPENS "-Docx.aeron.dir=$AERON_DIR" -cp "$JAR" \
    com.ocx.match.aeron.DumpInboundFrames "$AERON_DIR" \
    "aeron:udp?endpoint=localhost:8010" 3 6 > "$OUT/dump2.out" 2>&1 || true
cat "$OUT/dump2.out" || true
java $JVM_OPENS "-Docx.aeron.dir=$AERON_DIR" "-Docx.archive.control-channel=aeron:udp?endpoint=localhost:8010" \
    "-Docx.probe.symbol=$SYMBOL" "-Docx.probe.count-trades=true" \
    -cp "$JAR" com.ocx.egress.ArchiveProbe > "$OUT/probe.out" 2>&1 || true
TRADES=$(rg -o "trade ${SYMBOL} frames=[0-9]+" "$OUT/probe.out" 2>/dev/null \
    | tail -1 | sed 's/.*frames=//' || true)
echo "producer 对照后 Archive 成交帧数: ${TRADES:-N/A}"
[ -n "${TRADES:-}" ] && [ "$TRADES" -gt 0 ] 2>/dev/null \
    || { log_error "环境链路异常（producer 也未成交）"; cat "$OUT/probe.out"; exit 1; }
if [ -n "${GW_TRADES:-}" ] && [ "$GW_TRADES" -gt 0 ] 2>/dev/null; then
    echo "✅ 网关下单 → 引擎撮合成交（probe#1=${GW_TRADES} 帧，帧格式正确）"
else
    log_error "网关两单未成交（probe#1=0），producer 对照成交 ${TRADES} 帧——帧字段对比见 dump/dump2"
    diff <(rg "FRAME#" "$OUT/dump.out" 2>/dev/null) <(rg "FRAME#" "$OUT/dump2.out" 2>/dev/null) || true
    tail -20 "$OUT/ingress.out"; tail -20 "$OUT/core.out"
    exit 1
fi

echo "━━━ 网关重启映射恢复验证 ━━━"
kill -9 "$GW_PID" 2>/dev/null || true
sleep 1
java -Xms128m -Xmx256m $JVM_OPENS "-Docx.aeron.dir=$AERON_DIR" \
    "-Docx.gateway.http.port=8080" \
    "-Docx.gateway.ingress.endpoint=localhost:11000" \
    "-Docx.gateway.issuer.id=100" \
    "-Docx.gateway.state.dir=$GW_STATE" \
    -cp "$GW_JAR" com.ocx.gateway.GatewayBootstrap > "$OUT/gateway2.out" 2>&1 &
GW2_PID=$!; PIDS+=($GW2_PID)
for _ in $(seq 1 30); do
    rg -q "HTTP_READY" "$OUT/gateway2.out" 2>/dev/null && break
    sleep 1
done
GOT=$(curl -s "http://localhost:8080/orders/SMOKE-001")
echo "GET /orders/SMOKE-001: $GOT"
echo "$GOT" | rg -q "\"orderId\":$ORDER_ID" \
    || { log_error "重启后幂等键映射丢失"; exit 1; }
echo "✅ 网关重启后 SMOKE-001 → orderId=$ORDER_ID 映射恢复"

echo ""
echo "产物目录: $OUT"
echo "━━━ 网关冒烟通过 ━━━"
