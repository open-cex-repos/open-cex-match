#!/bin/bash
# 撮合引擎压测编排脚本
# 用法: ./stress-test.sh [--local|--remote] [--quick|--full] [--profile spot|futures] [--symbol BTCUSDT] [--port N]
#       ./stress-test.sh --big [--tps 500000] [--duration 600] [--prefill-secs 5] [--pool-secs 10] [--heap 3g] [--cap 5000000]
#           [--wal-dir /Volumes/ocx-wal-ram/wal]  # 快照目录覆盖（{match.wal.data-dir}/snapshots/）
#           [--wal-recover-check]                # 停链前用 RecoveryMain 回放入站验证恢复
#           [--producer-n N]                     # 并行 producer 数量（各独立 socket，突破单 UDP 发送上限 ≈4.3M；
#                                                每进程自动分配 --instance-id 1..N，orderId 三段式发号互不重叠）
#           [--order-lifetime-secs 2.0]          # 限价单平均寿命（秒，指数分布）；真实市场撤单模型（一单一撤）；
#                                                池子窗口 --pool-secs 须 ≥ 3×寿命
#           [--prop -Dkey=value]                 # 追加引擎系统属性（可重复；对照实验如 -Docx.archive.record-inbound=false）
#           [--driver-low-latency]               # 媒体驱动 LowLatency 配置（NoOp idle，空闲也占核；对照 sender 上限用）
#           [--driver-pin-base N]                # 媒体驱动钉核起点（conductor=N, sender=N+1, receiver=N+2；避开撮合线程核）
#           [--producer-host felix@192.168.0.121] [--producer-pass 03053311]
#           [--ingress-endpoint 192.168.0.120:11000]
#           [--no-prom]                          # 不依赖外部 Prometheus：跳过预检/清序列/指标查询（本地压测用）
#           [--no-sidecar]                       # 不启动 metrics sidecar（引擎不依赖它，指标侧无人消费）
#           [--core-only]                        # 只标定 match-core：不起 egress/sidecar，出站环由 OutboundDrainMain 哑消费
#           [--bare-core]                        # 更裸：连 ingress 也不起，producer 直连入站 IPC 环（隐含 --core-only）
#
# 场景阶梯:
#   smoke   1K TPS × 30s  冒烟
#   load    10K TPS × 60s  常规负载
#   stress  50K TPS × 60s  压力测试
#   spike   100K TPS × 30s 极限压测
#
# --big 模式：500K 完整撮合编排（单 Producer 进程预生成订单池 + 四进程引擎自管理
#            + 灌簿→撮合连续注入 + 入站录制/快照，真实 exp 价格模型），本地 120 运行；见 §15.1。
# 撮合链路（2026-08-28 起）已完全脱离 Kafka：
#   收单 Producer → Aeron UDP → ingress；出站 egress → Aeron Archive 持久化日志。
# 四进程形态：媒体驱动(OcxMediaDriverMain) + ingress + core + egress + Archive(OcxArchiveMain)
#            —— 不再使用单 JVM 装配根（避免单进程下把多线程合计误判为引擎能力）。
# 压测发单（2026-08-28 起）：Rust 版 ocx-stress-producer（rusteron C 绑定，零分配定长帧，
#   单进程 + 订单池预生成，AERON_SOCKET_SO_* 环境变量调参；Java 版已删除）。

set -euo pipefail

OCX_HOST_105="${OCX_HOST_105:-bot01@192.168.0.105}"
HOST_105="${OCX_HOST_105}"
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=5 -F /dev/null \
          -o ControlMaster=auto -o ControlPersist=300 -o ControlPath=/tmp/ocx-stress-105.sock"
MATCH_PORT=""
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# 编排脚本自 2026-09-02 统一在 ocx-match-bench/scripts/ 维护；
# backend 根（含 pom.xml 与各引擎模块）= SCRIPT_DIR/../..
BACKEND_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# 目标引擎端口映射：{profile}:{symbol} -> port
port_for() {
    case "$1:$2" in
        spot:BTCUSDT)      echo 8092 ;;
        spot:ETHUSDT)      echo 8095 ;;
        futures:BTCUSDT)   echo 8094 ;;
        futures:ETHUSDT)   echo 8093 ;;
        *)                 echo 8085 ;;
    esac
}

MODE="remote"; LEVEL="quick"; PROFILE="spot"; SYMBOL="BTCUSDT"; PORT=""
BIG=0; TARGET_TPS=500000; DURATION=600; PREFILL_SECS=5; POOL_SECS=10; HEAP="3g"; CAP=5000000; OUT_ROOT="/tmp/perf-big"
SKIP_PROM=0
SKIP_SIDECAR=0
NO_JFR=0
WAL_DIR_OVERRIDE=""
ORDER_LIFETIME_SECS=2.0
BATCH_CANCEL_RATIO=0.000001
FUND_USERS=0
AFFINITY_ARG=""
IDLE_STRATEGY_ARG=""
PRODUCER_HOST=""; PRODUCER_SSH_PASS=""
INGRESS_ENDPOINT=""
ARCHIVE_COUNT_TRADES=0
WAL_RECOVER_CHECK=0
REBUILD_PRODUCER=0
CORE_ONLY=0
BARE_CORE=0
PRODUCER_N=1
METRICS_MODE=""
EXTRA_PROPS=()
DRIVER_LOW_LATENCY=0
DRIVER_PIN_BASE=""
while [ $# -gt 0 ]; do
    case "$1" in
        --local)   MODE="local" ;;
        --remote)  MODE="remote" ;;
        --quick)   LEVEL="quick" ;;
        --full)    LEVEL="full" ;;
        --big)     BIG=1 ;;
        --profile) PROFILE="$2"; shift ;;
        --symbol)  SYMBOL="$2"; shift ;;
        --port)    PORT="$2"; shift ;;
        --tps)     TARGET_TPS="$2"; shift ;;
        --duration) DURATION="$2"; shift ;;
        --prefill-secs) PREFILL_SECS="$2"; shift ;;
        --pool-secs) POOL_SECS="$2"; shift ;;
        --order-lifetime-secs) ORDER_LIFETIME_SECS="$2"; shift ;;
        --batch-cancel-ratio) BATCH_CANCEL_RATIO="$2"; shift ;;
        --fund-users) FUND_USERS="$2"; shift ;;
        --no-affinity) AFFINITY_ARG="-Docx.match.affinity=false" ;;
        --idle-strategy) IDLE_STRATEGY_ARG="-Docx.match.idle-strategy=$2"; shift ;;
        --heap)    HEAP="$2"; shift ;;
        --cap)     CAP="$2"; shift ;;
        --wal-dir) WAL_DIR_OVERRIDE="$2"; shift ;;
        --wal-recover-check) WAL_RECOVER_CHECK=1 ;;
        --out)     OUT_ROOT="$2"; shift ;;
        --ingress-endpoint) INGRESS_ENDPOINT="$2"; shift ;;
        --producer-host) PRODUCER_HOST="$2"; shift ;;
        --producer-pass) PRODUCER_SSH_PASS="$2"; shift ;;
        --archive-count-trades) ARCHIVE_COUNT_TRADES=1 ;;
        --rebuild-producer) REBUILD_PRODUCER=1 ;;
        --no-prom) SKIP_PROM=1 ;;
        --no-sidecar) SKIP_SIDECAR=1 ;;
        --no-jfr) NO_JFR=1 ;;
        --core-only) CORE_ONLY=1 ;;
        --bare-core) BARE_CORE=1 ;;
        --metrics-mode) METRICS_MODE="$2"; shift ;;
        --producer-n) PRODUCER_N="$2"; shift ;;
        --prop) EXTRA_PROPS+=("$2"); shift ;;
        --driver-low-latency) DRIVER_LOW_LATENCY=1 ;;
        --driver-pin-base) DRIVER_PIN_BASE="$2"; shift ;;
        *) echo "未知参数: $1"; exit 2 ;;
    esac
    shift
done
SYMBOL=$(echo "$SYMBOL" | tr '[:lower:]' '[:upper:]')
MATCH_PORT="${PORT:-$(port_for "$PROFILE" "$SYMBOL")}"

# 成交对账（2026-08-31 起支持快照锚点）：TradeRecordReconciliationMain 自动选择——
# 有有效快照 → 快照预填 + 锚点后增量回放入站（快照开形态，磁盘水位受控）；
# 无快照 → 全量回放入站（旧行为，需完整入站 recording）。不再强制关快照：
# 大压力长跑建议保持快照开（入站 recording 后段回收，避免磁盘爆满）。
if [ "$FUND_USERS" -gt 0 ]; then
    echo "[INFO] --fund-users=$FUND_USERS: 对账快照锚点自动模式（快照开→锚点增量；快照关→全量入站）"
fi

case "$PROFILE:$SYMBOL" in
    spot:BTCUSDT)    PRICE=70000; SPREAD=200 ;;
    spot:ETHUSDT)    PRICE=3500;  SPREAD=50  ;;
    futures:BTCUSDT) PRICE=70000; SPREAD=200 ;;
    futures:ETHUSDT) PRICE=3500;  SPREAD=50  ;;
    *)               PRICE=3000;  SPREAD=50  ;;
esac

if [ "$LEVEL" = "quick" ]; then
    SCENARIOS=("smoke:1000:30" "load:10000:60" "stress:50000:60")
else
    SCENARIOS=("smoke:1000:30" "load:10000:60" "stress:50000:60" "spike:100000:30")
fi

# 生产端 → ingress 的 UDP 收单端点：本地 producer 走 localhost；远程 producer 需 120 的局域网地址。
if [ -z "$INGRESS_ENDPOINT" ]; then
    if [ -n "$PRODUCER_HOST" ]; then
        INGRESS_ENDPOINT="192.168.0.120:11000"
    else
        INGRESS_ENDPOINT="localhost:11000"
    fi
fi
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log_info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

prom_q() {
    [ "${SKIP_PROM:-0}" = "1" ] && { echo "N/A"; return 0; }
    curl -s -m 4 -G "$PROM_URL/api/v1/query" --data-urlencode "query=$1" 2>/dev/null \
        | python3 -c "import json,sys
try:
  r=json.load(sys.stdin)['data']['result']
  print(r[0]['value'][1] if r else 'N/A')
except Exception: print('ERR')"
}

# ====== --big：500K 完整撮合编排（单 Producer 池子预生成 + 引擎自管理 + 入站录制/快照）======
run_big_stress() {
    local JAR="$BACKEND_DIR/ocx-match-ingress/target/ocx-match-ingress-1.0.jar"
    local SIDECAR_JAR="$BACKEND_DIR/ocx-metrics-exporter/target/ocx-metrics-exporter-1.0.jar"
    local RUST_PROD_DIR="$BACKEND_DIR/ocx-stress-producer"
    # OCX_PRODUCER_BIN 可覆盖为任意 producer 可执行（A/B 对照等场景）
    local RUST_BIN="${OCX_PRODUCER_BIN:-$RUST_PROD_DIR/target/release/ocx-stress-producer}"
    local OUT="$OUT_ROOT" METRICS_DIR="$OUT_ROOT/metrics" WAL_DIR="${WAL_DIR_OVERRIDE:-$OUT_ROOT/wal}"
    local SIDECAR_PORT=9201 PROM_URL="${PROM_URL:-http://192.168.0.105:9090}"
    local SSH_PASS_121="${SSH_PASS_121:-03053311}"
    local PRODUCER_PASS="${PRODUCER_SSH_PASS:-$SSH_PASS_121}"
    # --no-jfr：零诊断写盘（JFR/GC 日志都是 mmap 落盘，探测纯计算上限时禁用）
    gc_log_arg() { [ "$NO_JFR" = "1" ] && return; echo "-Xlog:gc:file=$OUT/$1-gc.log:time,uptime,level,tags:filecount=5,filesize=50m"; }
    jfr_arg() { [ "$NO_JFR" = "1" ] && return; echo "-XX:StartFlightRecording=filename=$OUT/$1.jfr,settings=profile,disk=true,maxsize=$2"; }
    mkdir -p "$METRICS_DIR" "$WAL_DIR"

    if [ "$BARE_CORE" = "1" ]; then
        log_warn "--bare-core：跳过 ingress（producer 直连入站 IPC 环，264B 入站行），隐含 --core-only"
        CORE_ONLY=1
    fi
    if [ "$CORE_ONLY" = "1" ]; then
        log_warn "--core-only：跳过 sidecar 与 Prometheus（无 egress 写 metrics 队列，指标侧无数据）"
        SKIP_SIDECAR=1
        SKIP_PROM=1
    fi
    if [ "$SKIP_PROM" = "1" ]; then
        log_warn "已跳过 Prometheus 预检/清理/查询（--no-prom），压测照常、指标输出 N/A"
    else
        log_info "预检: Prometheus($PROM_URL) / 引擎 jar / sidecar jar"
        if ! curl -s -m 4 -o /dev/null "$PROM_URL/-/healthy"; then
            log_warn "Prometheus 不可达（$PROM_URL），自动降级为 --no-prom（压测照常、指标输出 N/A）"
            SKIP_PROM=1
        fi
    fi
    [ -f "$JAR" ] || { log_error "引擎 jar 不存在: $JAR"; exit 1; }
    if [ "$SKIP_SIDECAR" != "1" ]; then
        [ -f "$SIDECAR_JAR" ] || { log_error "sidecar jar 不存在: $SIDECAR_JAR"; exit 1; }
    fi
    log_info "✅ 预检通过（单 producer，目标 ${TARGET_TPS} TPS）"
    if [ -n "$PRODUCER_HOST" ]; then
        log_info "远程拓扑: producer-host=${PRODUCER_HOST}"
    fi
    local DRIVER_ARGS=()
    [ "$DRIVER_LOW_LATENCY" = "1" ] && DRIVER_ARGS+=("-Docx.driver.low-latency=true")
    [ -n "$DRIVER_PIN_BASE" ] && DRIVER_ARGS+=("-Docx.driver.pin-base=$DRIVER_PIN_BASE")
    if [ "${#DRIVER_ARGS[@]}" -gt 0 ]; then
        log_warn "媒体驱动高性能开关: ${DRIVER_ARGS[*]}（空闲也占核，对照实验用）"
    fi

    log_info "杀掉遗留压测 java 进程..."
    for pat in "ocx-match-ingress-1.0.jar" "ocx-stress-producer" \
               "ocx-metrics-exporter-1.0.jar" "match-engine-perf-full" \
               "com.ocx.MatchCoreBootstrap" "com.ocx.ingress.MatchIngressBootstrap" \
               "com.ocx.egress.MatchEgressBootstrap" "com.ocx.egress.OcxArchiveMain" \
               "com.ocx.aeron.OcxMediaDriverMain"; do
        pkill -f "$pat" 2>/dev/null || true
    done
    sleep 2
    if pgrep -f "ocx-match-ingress-1.0.jar|ocx-stress-producer|ocx-metrics-exporter-1.0.jar|com.ocx.MatchCoreBootstrap|com.ocx.ingress.MatchIngressBootstrap|com.ocx.egress.MatchEgressBootstrap|com.ocx.egress.OcxArchiveMain|com.ocx.aeron.OcxMediaDriverMain" >/dev/null 2>&1; then
        log_warn "残留 java 进程未退出，强制终止..."
        pkill -9 -f "ocx-match-ingress-1.0.jar|ocx-stress-producer|ocx-metrics-exporter-1.0.jar|com.ocx.MatchCoreBootstrap|com.ocx.ingress.MatchIngressBootstrap|com.ocx.egress.MatchEgressBootstrap|com.ocx.egress.OcxArchiveMain|com.ocx.aeron.OcxMediaDriverMain" 2>/dev/null || true
        sleep 1
    fi
    pgrep -f "ocx-match-ingress-1.0.jar|ocx-stress-producer|ocx-metrics-exporter-1.0.jar|com.ocx.MatchCoreBootstrap|com.ocx.ingress.MatchIngressBootstrap|com.ocx.egress.MatchEgressBootstrap|com.ocx.egress.OcxArchiveMain|com.ocx.aeron.OcxMediaDriverMain" >/dev/null 2>&1 \
        && log_warn "⚠️ 仍有压测 java 进程残留（请人工确认）" || log_info "✅ 无残留压测 java 进程"

    # 1) 清快照/Archive：旧目录移入回收站后重建（避免新轮恢复旧订单/旧 recording）
    local TRASH="$HOME/.Trash/perf-cleanup-$(date +%s)"
    mkdir -p "$TRASH"
    # 1.0) 清 JFR：上一轮 JFR 录制/崩溃转储显式移入回收站（core/ingress/egress.jfr / hotspot-pid / hs_err）
    for f in "$OUT"/core.jfr "$OUT"/ingress.jfr "$OUT"/egress.jfr \
             "$OUT"/engine.jfr "$OUT"/hotspot-pid-*.jfr "$OUT"/hs_err_pid*.log; do
        [ -e "$f" ] && mv "$f" "$TRASH/" 2>/dev/null || true
    done
    [ -d "$OUT" ] && mv "$OUT" "$TRASH/out" 2>/dev/null || true
    [ -d "$METRICS_DIR" ] && mv "$METRICS_DIR" "$TRASH/metrics" 2>/dev/null || true
    if [ -n "$WAL_DIR_OVERRIDE" ]; then
        # RAM disk / 独立盘上的 WAL：直接清空（易失介质，无回收价值），避免跨卷 mv 复制
        [ -d "$WAL_DIR" ] && rm -rf "${WAL_DIR:?}" 2>/dev/null || true
    else
        [ -d "$WAL_DIR" ] && mv "$WAL_DIR" "$TRASH/wal" 2>/dev/null || true
    fi
    [ -d "$OUT/archive" ] && mv "$OUT/archive" "$TRASH/archive" 2>/dev/null || true
    mkdir -p "$OUT" "$METRICS_DIR" "$WAL_DIR"

    if [ "$SKIP_PROM" != "1" ]; then
        # 2) 清 Prometheus 垃圾数据：删除上一轮 match-engine job 全部历史序列
        log_info "清 Prometheus 垃圾数据（delete_series job=match-engine）..."
        local HTTP
        HTTP=$(curl -s -m 15 -X POST "$PROM_URL/api/v1/admin/tsdb/delete_series?match[]=%7Bjob%3D%22match-engine%22%7D" \
            -o /dev/null -w "%{http_code}" 2>/dev/null) || HTTP="000"
        if [ "$HTTP" = "204" ]; then
            HTTP=$(curl -s -m 60 -X POST "$PROM_URL/api/v1/admin/tsdb/clean_tombstones" \
                -o /dev/null -w "%{http_code}" 2>/dev/null) || HTTP="000"
            log_info "✅ Prometheus 旧序列已清除（clean_tombstones http=${HTTP}）"
        else
            log_warn "⚠️ Prometheus delete_series http=$HTTP（需 105 开启 --web.enable-admin-api；本次未清除历史序列）"
        fi
    fi

    log_info "✅ 环境清理完成（Archive/WAL / Prometheus 旧序列均清，旧产物已移入 ${TRASH}）"

    local JVM_OPENS="--add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED"
    if [ "$SKIP_SIDECAR" = "1" ]; then
        log_warn "已跳过 sidecar 启动（--no-sidecar），指标侧无人消费（引擎不受影响）"
        local SIDECAR_PID=""
    else
        log_info "启动 sidecar :$SIDECAR_PORT <- metrics 流 (aeron dir=$OUT/aeron)"
        (cd "$OUT" && exec java $JVM_OPENS -jar "$SIDECAR_JAR" \
            "--metrics.aeron.dir=$OUT/aeron" "--metrics.symbols=$SYMBOL" \
            "--metrics.profile=$PROFILE" "--server.port=$SIDECAR_PORT" \
            ) > "$OUT/sidecar.out" 2>&1 &
        local SIDECAR_PID=$!
        for i in $(seq 1 20); do
            curl -s -m 2 -o /dev/null "http://localhost:$SIDECAR_PORT/metrics" && break
            sleep 1
        done
        curl -s -m 2 -o /dev/null "http://localhost:$SIDECAR_PORT/metrics" \
            || { log_error "sidecar 启动失败"; tail -10 "$OUT/sidecar.out"; exit 1; }
        log_info "✅ sidecar 启动 (pid=$SIDECAR_PID)"
    fi

    JVM_OPENS="--add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED"
    local AERON_DIR="$OUT/aeron"
    local MEDIA_PID="" ARCHIVE_PID="" EGRESS_PID="" CORE_PID="" INGRESS_PID=""
    local ARCHIVE_DIR="$OUT/archive"

    # 四进程共享同一套引擎配置 + 独立 Aeron 媒体驱动目录（与单 JVM 装配根参数完全对齐）
    local ENG_CFG=(
        -Dspring.profiles.active=spot
        "-Dspring.match.symbols.$SYMBOL.coin-scale=8"
        "-Dspring.match.symbols.$SYMBOL.base-coin-scale=8"
        "-Dspring.match.max-open-orders=$CAP"
        "-Docx.ingress.aeron.udp.endpoint=0.0.0.0:11000"
        # 单实例性能压测默认关 HA 心跳（协调者不起则 primary 会被自熔断 fail-stop；
        # 需要 HA 场景时用 --prop -Docx.ha.enabled=true 覆盖）
        "-Docx.ha.enabled=false"
        # Aeron UDP 收单链路调参（600K pps 实测：默认 128KB 接收缓冲/流控窗口秒满 → 丢包重传坍塌）
        "-Daeron.socket.so_rcvbuf=8388608"
        "-Daeron.socket.so_sndbuf=8388608"
        "-Daeron.rcv.initial.window.length=4194304"
        # Archive 录制订阅为 untethered：录制端停滞超过默认 5s 窗口限制超时会被
        # 媒体驱动吊销发布端（Publications Revoked），录制会话永久终止、引擎停摆
        # （2026-09-02 D10/D11 实测）。桥/环已吸收 0.7-2.8s 级停滞，调大到 60s 兜底
        "-Daeron.untethered.window.limit.timeout=60000000000"
        -Dmatch.wal.enabled=true
        "-Dmatch.wal.data-dir=$WAL_DIR"
        # WAL 持久化由 Aeron Archive recording 承担（core 直发 → Archive 记录），
        # 不再需要 Chronicle 滚动/fsync/屏障等调参；-Docx.archive.dir 见 Archive 启动。
        "-Dmatch.metrics.data-dir=$METRICS_DIR"
        -Dmatch.metrics.lag-trigger=50000000
        -Dmatch.metrics.lag-recover=20000000
        "-Docx.aeron.dir=$AERON_DIR"
    )
    # 录制解耦（2026-09-01）：全形态默认 journal bridge（ingress→feed→live/journal）；
    # bare/core-only 无 ingress，回退直接录 live 流（Archive/恢复流选择跟随本属性）
    if [ "$BARE_CORE" = "1" ] || [ "$CORE_ONLY" = "1" ]; then
        ENG_CFG+=("-Docx.journal-bridge.enabled=false")
    else
        ENG_CFG+=("-Docx.journal-bridge.enabled=true")
    fi
    [ -n "$METRICS_MODE" ] && ENG_CFG+=("-Docx.match.metrics.mode=$METRICS_MODE")
    for ep in ${EXTRA_PROPS[@]+"${EXTRA_PROPS[@]}"}; do
        ENG_CFG+=("$ep")
    done

    # 控制流 READY 等待（2026-08-30，替代 .out 文件 grep）：各引擎进程启动完成时
    # 向 CONTROL_STREAM 发布 READY；媒体驱动未起时 StressControl 自带建连重试。
    wait_ready_aeron() {
        local name="$1" out="$2" proc="$3" timeout_s="${4:-60}"
        java $JVM_OPENS "-Docx.aeron.dir=$AERON_DIR" -cp "$JAR" \
            com.ocx.aeron.StressControl --wait-ready "$name" --timeout "$timeout_s" \
            && return 0
        log_error "$proc 启动失败（控制流未收到 READY）"
        tail -20 "$out"
        exit 1
    }

    # ── Rust producer：先构建/同步，再预生成订单池（引擎未启动，可用满资源）──
    if [ -z "${OCX_PRODUCER_BIN:-}" ] && { [ "$REBUILD_PRODUCER" = "1" ] || [ ! -x "$RUST_BIN" ]; }; then
        log_info "构建 Rust producer（cargo build --release）..."
        (cd "$RUST_PROD_DIR" && cargo build --release) || { log_error "Rust producer 构建失败"; exit 1; }
    fi
    if [ -n "$PRODUCER_HOST" ]; then
        log_info "同步 Rust producer 到 ${PRODUCER_HOST}..."
        sshpass -p "$PRODUCER_PASS" ssh $SSH_OPTS "$PRODUCER_HOST" \
            "mkdir -p ~/stress" 2>/dev/null
        sshpass -p "$PRODUCER_PASS" rsync -az -e "ssh $SSH_OPTS" \
            "$RUST_BIN" "$PRODUCER_HOST:stress/ocx-stress-producer" 2>/dev/null
        log_info "✅ 产物同步完成"
    fi

    # 池子预生成：PRODUCER_N 个 producer 并行预生成（各占独立 UDP socket，突破单 socket
    # 发送上限 ≈4.3M pps），打印 POOL_READY 后原地等待各自 go 文件（引擎就绪才重放）
    [ "$PRODUCER_N" -lt 1 ] && PRODUCER_N=1
    local POOL_TOTAL=$((TARGET_TPS * POOL_SECS))
    local PER_TPS=$((TARGET_TPS / PRODUCER_N))
    local PER_POOL=$((POOL_TOTAL / PRODUCER_N))
    [ "$PER_TPS" -lt 1 ] && PER_TPS=1
    [ "$PER_POOL" -lt 1 ] && PER_POOL=1
    local PROD_PIDS=()
    local PROD_ENV="AERON_SOCKET_SO_SNDBUF=8388608 AERON_SOCKET_SO_RCVBUF=8388608 AERON_RCV_INITIAL_WINDOW_LENGTH=4194304"
    log_info "启动 ${PRODUCER_N} 个 producer（各 ${PER_TPS} TPS、池子 ${PER_POOL} 帧，总 ${POOL_TOTAL} 帧，引擎未启动）..."
    local i
    for i in $(seq 1 "$PRODUCER_N"); do
        local PROD_ARGS="--aeron-endpoint $INGRESS_ENDPOINT --profile spot --symbol $SYMBOL \
            --instance-id $i \
            --tps $PER_TPS --duration $((PREFILL_SECS + DURATION)) \
            --price $PRICE --spread $SPREAD \
            --depth exp --tick 0.01 --decay 10 --mid-vol 1.0 \
            --batch 500 --side 0 --order-lifetime-secs $ORDER_LIFETIME_SECS \
            --batch-cancel-ratio $BATCH_CANCEL_RATIO \
            --market-ratio 0.10 --stop-ratio 0.02 --iceberg-ratio 0.05 --large-ratio 0.02 --price-regime"
        if [ "$BARE_CORE" = "1" ]; then
            PROD_ARGS="$PROD_ARGS --aeron-ipc --aeron-dir $AERON_DIR"
        fi
        if [ "$FUND_USERS" -gt 0 ]; then
            PROD_ARGS="$PROD_ARGS --fund-users $FUND_USERS"
        fi
        if [ -n "$PRODUCER_HOST" ]; then
            sshpass -p "$PRODUCER_PASS" ssh $SSH_OPTS "$PRODUCER_HOST" \
                "bash -lc '$PROD_ENV \$HOME/stress/ocx-stress-producer --pool-size $PER_POOL --go-file \$HOME/stress/go-$SYMBOL-$i $PROD_ARGS'" \
                > "$OUT/match-$i.out" 2>&1 &
        else
            # 本地 producer：Aeron 控制流 GO（替代 go 文件握手）；共享媒体驱动 dir 由脚本传入
            env $PROD_ENV "$RUST_BIN" --pool-size "$PER_POOL" --go-aeron-dir "$AERON_DIR" $PROD_ARGS \
                > "$OUT/match-$i.out" 2>&1 &
        fi
        PROD_PIDS+=("$!")
    done
    local ALL_READY=0
    for _ in $(seq 1 180); do
        ALL_READY=1
        for i in $(seq 1 "$PRODUCER_N"); do
            grep -q "POOL_READY" "$OUT/match-$i.out" 2>/dev/null || { ALL_READY=0; break; }
        done
        [ "$ALL_READY" = "1" ] && break
        sleep 1
    done
    if [ "$ALL_READY" -ne 1 ]; then
        log_error "producer 池子预生成超时"
        for i in $(seq 1 "$PRODUCER_N"); do tail -5 "$OUT/match-$i.out"; done
        exit 1
    fi
    log_info "✅ ${PRODUCER_N} 个 producer 池子就绪（各 ${PER_POOL} 帧，等待引擎就绪后 go）"

    log_info "启动媒体驱动 (aeron dir=$AERON_DIR)"
    (cd "$OUT" && exec java -Xms256m -Xmx512m $JVM_OPENS \
        "${ENG_CFG[@]}" "${DRIVER_ARGS[@]+"${DRIVER_ARGS[@]}"}" \
        -cp "$JAR" com.ocx.aeron.OcxMediaDriverMain) \
        > "$OUT/media-driver.out" 2>&1 &
    MEDIA_PID=$!
    wait_ready_aeron "media-driver" "$OUT/media-driver.out" "媒体驱动"
    log_info "✅ 媒体驱动启动 (pid=$MEDIA_PID)"

    log_info "启动 Archive (Aeron 持久化日志: 成交/深度/DLT/WAL, dir=$ARCHIVE_DIR)"
    (cd "$OUT" && exec java -Xms512m -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=20 \
        $JVM_OPENS \
        $(gc_log_arg archive) \
        "${ENG_CFG[@]}" "-Docx.archive.dir=$ARCHIVE_DIR" \
        -cp "$JAR" com.ocx.egress.OcxArchiveMain) \
        > "$OUT/archive.out" 2>&1 &
    ARCHIVE_PID=$!
    wait_ready_aeron "archive" "$OUT/archive.out" "Archive"
    log_info "✅ Archive 启动 (pid=$ARCHIVE_PID)"

    if [ "$CORE_ONLY" = "1" ]; then
        log_info "启动出站哑消费 (OutboundDrainMain：只读出站环丢帧，零编码/零持久化)"
        (cd "$OUT" && exec java -Xms256m -Xmx512m $JVM_OPENS \
            "${ENG_CFG[@]}" -cp "$JAR" com.ocx.aeron.OutboundDrainMain) \
            > "$OUT/drain.out" 2>&1 &
        EGRESS_PID=$!
        wait_ready_aeron "drain" "$OUT/drain.out" "出站哑消费"
        log_info "✅ 出站哑消费启动 (pid=$EGRESS_PID)"
    else
        log_info "启动 egress (WAL/Aeron Archive 出站/metrics 队列)"
        (cd "$OUT" && exec java -Xms512m -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=20 \
            $JVM_OPENS \
            $(gc_log_arg egress) $(jfr_arg egress 2g) \
            "${ENG_CFG[@]}" -cp "$JAR" com.ocx.egress.MatchEgressBootstrap) \
            > "$OUT/egress.out" 2>&1 &
        EGRESS_PID=$!
        wait_ready_aeron "egress" "$OUT/egress.out" "egress"
        log_info "✅ egress 启动 (pid=$EGRESS_PID)"
    fi

    log_info "启动 core (纵表撮合: ${HEAP} 堆 / cap=${CAP} / WAL=$WAL_DIR)"
    # 固定堆 + AlwaysPreTouch：启动时把 $HEAP 物理内存全部 pre-touch（mmap 填零），
    # 杜绝撮合过程中纵表容量扩张触发动态物理页分配（微秒级抖动，LMAX 机械同理心）。
    # -Xmn1g：16GB 测试机上 young gen 大部分是瞬态垃圾，2g→1g 降低 GC 换页压力。
    (cd "$OUT" && exec java "-Xms$HEAP" "-Xmx$HEAP" "-Xmn1g" -XX:+UseG1GC -XX:MaxGCPauseMillis=20 \
        -XX:+AlwaysPreTouch \
        $JVM_OPENS \
        $AFFINITY_ARG \
        $IDLE_STRATEGY_ARG \
        $(gc_log_arg core) $(jfr_arg core 3g) \
        "${ENG_CFG[@]}" -cp "$JAR" com.ocx.MatchCoreBootstrap) \
        > "$OUT/core.out" 2>&1 &
    CORE_PID=$!
    wait_ready_aeron "core" "$OUT/core.out" "core"
    log_info "✅ core 启动 (pid=$CORE_PID)"

    if [ "$BARE_CORE" != "1" ]; then
        log_info "启动 ingress (Aeron UDP 收单/校验/SBE 解码 → 入站环, endpoint=0.0.0.0:11000)"
        (cd "$OUT" && exec java -Xms512m -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=20 \
            $JVM_OPENS \
            $(gc_log_arg ingress) $(jfr_arg ingress 2g) \
            "${ENG_CFG[@]}" -cp "$JAR" com.ocx.ingress.MatchIngressBootstrap) \
            > "$OUT/ingress.out" 2>&1 &
        INGRESS_PID=$!
        wait_ready_aeron "ingress" "$OUT/ingress.out" "ingress"
        log_info "✅ ingress 启动 (pid=$INGRESS_PID)"
    else
        log_info "（--bare-core：不起 ingress，producer 直连入站 IPC 环）"
    fi

    # 引擎就绪 → 发 go 信号，producer 开始重放（预灌 ${PREFILL_SECS}s + 撮合 ${DURATION}s 连续注入）
    log_info "引擎就绪，发 go 信号 → producer 重放（预灌 ${PREFILL_SECS}s + 撮合 ${DURATION}s）"
    local i
    local GO_SENT=0
    for i in $(seq 1 "$PRODUCER_N"); do
        if [ -n "$PRODUCER_HOST" ]; then
            sshpass -p "$PRODUCER_PASS" ssh $SSH_OPTS "$PRODUCER_HOST" \
                "touch ~/stress/go-$SYMBOL-$i" 2>/dev/null || true
        elif [ "$GO_SENT" = "0" ]; then
            # 本地：单条 GO 控制消息唤醒全部 producer（全局控制流）
            java $JVM_OPENS "-Docx.aeron.dir=$AERON_DIR" \
                -cp "$JAR" com.ocx.aeron.StressControl --go || true
            GO_SENT=1
        fi
    done

    if [ "$CORE_ONLY" = "1" ]; then
        echo "（core-only：无 Prometheus 指标，进度仅显示 GC 采样）"
    fi
    for m in $(seq 1 $((DURATION / 60))); do
        sleep 60
        book=$(prom_q "match_order_book_size{symbol=\"$SYMBOL\"}")
        trades=$(prom_q "sum(match_trades_total{symbol=\"$SYMBOL\"})")
        rejected=$(prom_q "sum(match_orders_rejected_total{symbol=\"$SYMBOL\"})")
        overloaded=$(prom_q "match_engine_overloaded{symbol=\"$SYMBOL\"}")
        lag=$(prom_q "match_consumer_lag{symbol=\"$SYMBOL\"}")
        p99=$(prom_q "match_duration_percentile{symbol=\"$SYMBOL\",type=\"limit\",percentile=\"p99\"}")
        young=$(grep -c "Pause Young" "$OUT/core-gc.log" 2>/dev/null || echo 0)
        full=$(grep -c "Pause Full" "$OUT/core-gc.log" 2>/dev/null || echo 0)
        echo "[$((m*60))s] book=$book trades=$trades rejected=$rejected overloaded=$overloaded lag=$lag p99=$p99 gc_young=$young gc_full=$full"
    done

    local p
    for p in "${PROD_PIDS[@]}"; do
        [ -n "$p" ] && wait "$p" 2>/dev/null || true
    done
    sleep 10
    # 先停 egress/drain：结束出站发布，让 Archive 完成 recording 收尾（stopPosition 落定）
    if [ "$CORE_ONLY" = "1" ]; then
        log_info "停止出站哑消费（等待 Archive recording 收尾）"
    else
        log_info "停止 egress（等待 Archive recording 收尾）"
    fi
    [ -n "$EGRESS_PID" ] && kill -TERM "$EGRESS_PID" 2>/dev/null || true
    [ -n "$EGRESS_PID" ] && wait "$EGRESS_PID" 2>/dev/null || true
    sleep 3

    # 出站落盘验证：ArchiveProbe 列出 recording（stream/字节数/位置），对照成交总量
    # （须在媒体驱动与 Archive 仍在运行时执行：replay/listRecordings 走 archive 控制通道）
    log_info "Archive 落盘验证 (ArchiveProbe)..."
    local PROBE_ARGS=("-Docx.aeron.dir=$AERON_DIR" "-Docx.probe.symbol=$SYMBOL")
    if [ "$ARCHIVE_COUNT_TRADES" = "1" ] || [ "$FUND_USERS" -gt 0 ]; then
        PROBE_ARGS+=("-Docx.probe.count-trades=true")
    fi
    java $JVM_OPENS "${PROBE_ARGS[@]}" -cp "$JAR" com.ocx.egress.ArchiveProbe \
        > "$OUT/archive-probe.out" 2>&1 || true
    cat "$OUT/archive-probe.out" || true
    local TRADES_PROM
    TRADES_PROM=$(prom_q "sum(match_trades_total{symbol=\"$SYMBOL\"})")
    echo "Prometheus match_trades_total: ${TRADES_PROM:-N/A}"
    # 成交 recording 逐帧计数（fund-check 时用，作为对账 U8 的期望；比 Prometheus 抓取更准）
    local TRADES_RECON
    TRADES_RECON=$(grep -o "trade ${SYMBOL} frames=[0-9]+" "$OUT/archive-probe.out" 2>/dev/null \
        | tail -1 | sed 's/.*frames=//')

    # WAL 恢复验证（可选）：core 已停（撮合停止、WAL 不再增长），媒体驱动与 Archive
    # 仍在运行——RecoveryMain 从 Archive recording 回放 WAL，验证崩溃恢复路径。
    if [ "$WAL_RECOVER_CHECK" = "1" ]; then
        log_info "WAL 恢复验证 (RecoveryMain)..."
        java $JVM_OPENS "${ENG_CFG[@]}" -cp "$JAR" com.ocx.match.RecoveryMain \
            > "$OUT/wal-recovery.out" 2>&1 || true
        cat "$OUT/wal-recovery.out" || true
    fi

    # 成交记录对账（可选，--fund-users N）：回放入站+成交 recording，独立校验
    # 成交记录自洽（金额/累计成交额/自成交/批撤/全局守恒），输出每专用用户账本。
    if [ "$FUND_USERS" -gt 0 ]; then
        log_info "成交记录对账 (TradeRecordReconciliationMain, fund-users=$FUND_USERS)..."
        # 大压力量级（5.5M×300s ≈ 4 亿帧/1 亿成交）：在簿订单实例 + BigDecimal 账本
        # 需 4–8g 堆；默认 ParallelGC 会用满全核做 GC，饿死同机媒体驱动
        # （DriverTimeout），改 G1 + 限 4 GC 线程（2 线程下 4 亿事件分配后
        # full GC 超 10s，饿死 Aeron 客户端 conductor）+ 驱动超时容忍 60s。
        java $JVM_OPENS -Xms4g -Xmx10g -XX:+UseG1GC \
            -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2 \
            -Xlog:gc:file=$OUT/recon-gc.log \
            -Daeron.driver.timeout=60000 "${ENG_CFG[@]}" \
            "-Docx.probe.symbol=$SYMBOL" "-Docx.fund.users=$FUND_USERS" \
            "-Docx.fund.expected-trades=${TRADES_RECON:-${TRADES_PROM:--1}}" \
            -cp "$JAR" com.ocx.match.TradeRecordReconciliationMain \
            > "$OUT/trade-recon.out" 2>&1 || true
        cat "$OUT/trade-recon.out" || true
    fi

    log_info "停止剩余链路（倒序: ingress → core → Archive → 媒体驱动）"
    for p in "$INGRESS_PID" "$CORE_PID" "$ARCHIVE_PID" "$MEDIA_PID"; do
        [ -n "$p" ] && kill -TERM "$p" 2>/dev/null || true
    done
    [ -n "$SIDECAR_PID" ] && kill -TERM "$SIDECAR_PID" 2>/dev/null || true
    for p in "$INGRESS_PID" "$CORE_PID" "$ARCHIVE_PID" "$MEDIA_PID"; do
        [ -n "$p" ] && wait "$p" 2>/dev/null || true
    done
    [ -n "$SIDECAR_PID" ] && wait "$SIDECAR_PID" 2>/dev/null || true

    echo ""
    echo "━━━ 汇总 ━━━"
    local total_tps=0 total_sent=0
    for i in $(seq 1 "$PRODUCER_N"); do
        s=$(grep "总发送" "$OUT/match-$i.out" 2>/dev/null | tail -1 \
            | sed -E 's/.*总发送:[[:space:]]*([0-9,]+).*/\1/' | tr -d ',' || echo 0)
        t=$(grep "实际 TPS" "$OUT/match-$i.out" 2>/dev/null | tail -1 \
            | sed -E 's/.*实际 TPS: *([0-9,]+).*/\1/' | tr -d ',' || echo 0)
        total_sent=$((total_sent + ${s:-0}))
        total_tps=$((total_tps + ${t:-0}))
    done
    echo "注入 TPS: ${total_tps}（${PRODUCER_N} 个 producer 合计，总发送 ${total_sent} 条）"
    echo "产物目录: $OUT"
    echo "━━━ 完成 ━━━"
}

if [ "$BIG" = "1" ]; then
    run_big_stress
    exit $?
fi

# ====== 指标抓取 ======
get_prom_metric() {
    local prom_data="$1" metric_name="$2"
    echo "$prom_data" | grep "^${metric_name}[ {]" | awk '{print $NF}' | tail -1
}
fetch_prometheus() {
    if [ "${1:-}" = "local" ]; then
        curl -s "http://localhost:9101/metrics" 2>/dev/null
    else
        local out=""
        for try in 1 2 3; do
            out=$(sshpass -p '001024' ssh $SSH_OPTS "$HOST_105" \
                "curl -s http://localhost:9101/metrics" 2>/dev/null) || true
            [ -n "$out" ] && break
            sleep 2
        done
        echo "$out"
    fi
}

series_value() {
    local prom_data="$1" metric="$2" labels="$3"
    echo "$prom_data" | grep -F "${metric}{${labels}}" | awk '{print $NF}' | tail -1
}

collect_snapshot() {
    local prom_data="$1" label="$2"
    local s="symbol=\"$SYMBOL\",profile=\"$PROFILE\""
    echo ""
    echo "=== $label ==="
    printf "  %-50s %s\n" "match_consumer_lag"                    "$(series_value "$prom_data" "match_consumer_lag" "$s")"
    printf "  %-50s %s\n" "match_throughput_current (TPS)"      "$(series_value "$prom_data" "match_throughput_current" "$s")"
    printf "  %-50s %s\n" "match_orders_total (limit)"           "$(series_value "$prom_data" "match_orders_total" "$s,type=\"limit\"")"
    printf "  %-50s %s\n" "match_trades_total"                  "$(series_value "$prom_data" "match_trades_total" "$s")"
    printf "  %-50s %s\n" "match_orders_rejected_total"         "$(series_value "$prom_data" "match_orders_rejected_total" "$s")"
    printf "  %-50s %s\n" "match_ringbuffer_remaining"          "$(series_value "$prom_data" "match_ringbuffer_remaining" "$s")"
    printf "  %-50s %s\n" "match_duration P99 (ms)"             "$(series_value "$prom_data" "match_duration_percentile" "$s,type=\"limit\",percentile=\"p99\"")"
    echo ""
}

# ====== 预检 ======
echo ""
echo "━━━ 压测预检 ━━━"
MONITOR_HOST="$([ "$MODE" = "local" ] && echo "local" || echo "remote")"
log_info "模式: $MONITOR_HOST | 目标: ${PROFILE}/${SYMBOL} (端口 $MATCH_PORT)"
log_info "场景: ${#SCENARIOS[@]} 个阶梯 | 价格基准: $PRICE ±$SPREAD"

PROM=$(fetch_prometheus "$MONITOR_HOST")
if [ -z "$PROM" ]; then
    log_error "❌ match-engine 不可达"
    exit 1
fi
if [ "$(echo "$PROM" | wc -l)" -lt 10 ]; then
    log_error "❌ Prometheus endpoint 不可用"
    exit 1
fi
log_info "✅ match-engine 可访问 (Prometheus $(echo "$PROM" | wc -l) 行)"

# ====== 构建 Rust producer（ocx-stress-producer；Java 版已删除） ======
RUST_PROD_DIR="$BACKEND_DIR/ocx-stress-producer"
RUST_BIN="${OCX_PRODUCER_BIN:-$RUST_PROD_DIR/target/release/ocx-stress-producer}"
if [ -z "${OCX_PRODUCER_BIN:-}" ] && { [ "$REBUILD_PRODUCER" = "1" ] || [ ! -x "$RUST_BIN" ]; }; then
    log_info "构建 Rust producer（cargo build --release）..."
    (cd "$RUST_PROD_DIR" && cargo build --release) || { log_error "Rust producer 构建失败"; exit 1; }
fi
log_info "Rust producer 就绪: $RUST_BIN"

# ====== 压测 ======
REPORT_FILE="/tmp/match-engine-stress-$(date +%Y%m%d-%H%M%S).txt"
{
    echo "撮合引擎压测报告 — $(date)"
    echo "模式: $MODE | 级别: $LEVEL | 符号: $SYMBOL"
    echo "================================================="
} > "$REPORT_FILE"

FIRST=true
for scenario_def in "${SCENARIOS[@]}"; do
    IFS=':' read -r name tps duration <<< "$scenario_def"

    echo ""
    echo "━━━ 场景: $name — ${tps} TPS × ${duration}s ━━━"

    PROM_BEFORE=$(fetch_prometheus "$MONITOR_HOST" || true)
    collect_snapshot "$PROM_BEFORE" "压测前 ($name)"

    START_TS=$(date +%s)

    log_info "启动 Producer ($tps TPS)..."
    env AERON_SOCKET_SO_SNDBUF=8388608 AERON_SOCKET_SO_RCVBUF=8388608 \
        AERON_RCV_INITIAL_WINDOW_LENGTH=4194304 \
        "$RUST_BIN" \
        --aeron-endpoint "$INGRESS_ENDPOINT" \
        --profile "$PROFILE" \
        --symbol "$SYMBOL" \
        --tps "$tps" \
        --duration "$duration" \
        --price "$PRICE" \
        --spread "$SPREAD" \
        --depth exp \
        --tick 0.01 \
        2>&1 | grep -E "^\[|^===" || true

    ELAPSED=$(($(date +%s) - START_TS))

    PROM_AFTER=$(fetch_prometheus "$MONITOR_HOST" || true)
    {
        collect_snapshot "$PROM_AFTER" "压测后 ($name, 耗时${ELAPSED}s)"
        echo ""
    } | tee -a "$REPORT_FILE"

    RING=$(series_value "$PROM_AFTER" "match_ringbuffer_remaining" "symbol=\"$SYMBOL\",profile=\"$PROFILE\"")
    # Aeron 模式无进程内环形缓冲：-1 表示无信号（真实背压看 lag/overloaded），视为健康
    if [ -n "$RING" ] && [ "$RING" != "-1" ] && [ "$(echo "$RING < 1000" | bc 2>/dev/null || echo 0)" = "1" ]; then
        log_warn "⚠️  RingBuffer 剩余=$RING — 接近瓶颈！"
    else
        log_info "✅ RingBuffer 剩余=$RING — 健康"
    fi
done

echo ""
echo "━━━ 完成 ━━━"
echo "报告: $REPORT_FILE"
echo ""
cat "$REPORT_FILE"
