#!/bin/bash
# JMH 微基准启动器（ocx-match-bench），默认携带 -prof jfr 与 JFR 配合观测。
#
# 用法:
#   ./bench.sh -l                             # 列出全部 benchmark
#   ./bench.sh OrderBookBench                 # 跑某个类（JFR 录制到 target/jfr/）
#   ./bench.sh OrderBookBench.cancelReadd     # 精确到方法
#   ./bench.sh 'OrderBookBench.*' -f 3 -i 10  # 追加 JMH 参数（fork/迭代）
#   ./bench.sh -prof gc OrderBookBench        # 叠加 GC 分配指标
#
# 产物: target/jfr/*.jfr 每 benchmark 一份，用 jfr print / JMC 分析；
#       JFR 采样与 JMH 吞吐/延迟指标正交，可互相印证热点。
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
JAR="$DIR/target/benchmarks.jar"
if [ ! -f "$JAR" ]; then
    echo "未找到 $JAR，先构建："
    echo "  mvn -Pbench -pl ocx-match-bench -am package -DskipTests"
    exit 1
fi
mkdir -p "$DIR/target/jfr"
# fork JVM 与生产一致：agrona/aeron 需要反射访问 jdk.internal.misc 等
JVM_ARGS="--add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED"
exec java -jar "$JAR" -jvmArgsAppend "$JVM_ARGS" -prof jfr:dir="$DIR/target/jfr" "$@"
