#!/bin/bash
# 仲裁层 Raft 快速冒烟（2026-09-01）：媒体驱动 + 3 节点仲裁组，
# 验证 ARB_READY×3 / LEADER_ELECTED / REGISTERED 后自动清理。
# 用法: ./raft-arbitration-smoke.sh
set -euo pipefail

GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
log_info() { echo -e "${GREEN}[INFO]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# 编排脚本自 2026-09-02 统一在 ocx-match-bench/scripts/ 维护；
# backend 根（含 pom.xml 与各引擎模块）= SCRIPT_DIR/../..
BACKEND_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
JAR="$BACKEND_DIR/ocx-match-ingress/target/ocx-match-ingress-1.0.jar"
OUT="$(mktemp -d /tmp/perf-raft-smoke.XXXXXX)"

JVM_OPENS="--add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED"

log_info "构建引擎 jar..."
mvn -q -pl ocx-match-ingress -am package -DskipTests

PIDS=()
cleanup() {
    for p in "${PIDS[@]:-}"; do
        kill -9 "$p" 2>/dev/null || true
    done
}
trap cleanup EXIT

log_info "启动媒体驱动（dir=$OUT/aeron-sb）"
java -Xms256m -Xmx512m $JVM_OPENS "-Docx.aeron.dir=$OUT/aeron-sb" \
    -cp "$JAR" com.ocx.match.aeron.OcxMediaDriverMain > "$OUT/media.out" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 30); do
    java $JVM_OPENS "-Docx.aeron.dir=$OUT/aeron-sb" -cp "$JAR" \
        com.ocx.match.aeron.StressControl --wait-ready media-driver --timeout 3 \
        >/dev/null 2>&1 && break
    sleep 1
done

for i in 1 2 3; do
    log_info "启动仲裁节点 arb$i"
    java -Xms256m -Xmx1024m $JVM_OPENS "-Docx.aeron.dir=$OUT/aeron-sb" \
        "-Docx.ha.raft.id=arb$i" \
        "-Docx.ha.raft.peers=arb1:127.0.0.1:6001,arb2:127.0.0.1:6002,arb3:127.0.0.1:6003" \
        "-Docx.ha.raft.storage.dir=$OUT/raft/arb$i" \
        "-Docx.ha.metrics.port=$((9700 + i))" \
        "-Docx.ha.raft.lease-ttl-ms=5000" "-Docx.ha.raft.renew-interval-ms=2000" \
        "-Docx.ha.udp.listen=aeron:udp?endpoint=0.0.0.0:$((53100+i))" \
        "-Docx.ha.udp.endpoint=aeron:udp?endpoint=localhost:53000" \
        "-Docx.ha.primary.ack.endpoint=aeron:udp?endpoint=localhost:53001" \
        "-Docx.ha.standby.ack.endpoints=aeron:udp?endpoint=localhost:53000" \
        "-Docx.ha.primary-id=1" "-Docx.ha.standby-ids=2" \
        "-Docx.ha.primary-timeout-ms=5000" \
        -cp "$JAR" com.ocx.match.aeron.HaRaftCoordinatorMain > "$OUT/coordinator-arb$i.out" 2>&1 &
    PIDS+=($!)
done

OK=1
for t in $(seq 1 30); do
    READY=$(rg -l "ARB_READY" "$OUT"/coordinator-arb*.out 2>/dev/null | wc -l | tr -d ' ' || true)
    LEADER=$(rg -l "LEADER_ELECTED" "$OUT"/coordinator-arb*.out 2>/dev/null | wc -l | tr -d ' ' || true)
    REG=$(rg -l "REGISTERED" "$OUT"/coordinator-arb*.out 2>/dev/null | wc -l | tr -d ' ' || true)
    echo "[smoke] t=$t READY=$READY LEADER=$LEADER REG=$REG"
    if [ "$READY" -ge 3 ] && [ "$LEADER" -ge 1 ] && [ "$REG" -ge 1 ]; then
        OK=0
        break
    fi
    sleep 1
done

echo "=== 冒烟结果: ARB_READY=$READY LEADER_ELECTED=$LEADER REGISTERED=$REG ==="
if [ "$OK" != "0" ]; then
    log_error "仲裁组冒烟失败"
    for f in "$OUT"/coordinator-arb*.out; do
        echo "--- $(basename "$f") ---"
        tail -8 "$f" || true
    done
    exit 1
fi

LEADER_FILE=$(rg -l "LEADER_ELECTED" "$OUT"/coordinator-arb*.out | head -1)
echo "--- 当前 leader 日志 ---"
tail -5 "$LEADER_FILE"
log_info "仲裁组冒烟通过，产物目录=$OUT"
