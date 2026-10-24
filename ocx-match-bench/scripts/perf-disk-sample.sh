#!/bin/bash
# 全形态压测磁盘/Archive 采样（2026-09-01）：每 1s 一行 TSV：
#   epoch_secs  KB/t  tps  MB/s  archive_rec_files  archive_bytes
# 用法: ./perf-disk-sample.sh <out-tsv> <archive-dir> <duration-secs>
# iostat -c 1 在非 TTY 下可能秒回（不真正等待），故每轮显式 sleep 1 节拍；
# 时间戳用秒.纳秒，避免整秒内多行同刻。
set -uo pipefail
OUT="$1"; ARCHIVE_DIR="$2"; DURATION="${3:-900}"
: > "$OUT"
for i in $(seq 1 "$DURATION"); do
    line=$(iostat -d -w 1 -c 1 disk0 2>/dev/null | tail -1)
    kb_t=$(echo "$line" | awk '{print $1}')
    tps=$(echo "$line" | awk '{print $2}')
    mb_s=$(echo "$line" | awk '{print $3}')
    if [ -d "$ARCHIVE_DIR" ]; then
        recs=( "$ARCHIVE_DIR"/*.rec )
        segs=${#recs[@]}
        bytes=$(stat -f%z "$ARCHIVE_DIR"/*.rec 2>/dev/null | awk '{s+=$1} END {print s+0}')
    else
        segs=0; bytes=0
    fi
    echo "$(date +%s.%N) ${kb_t:-0} ${tps:-0} ${mb_s:-0} ${segs:-0} ${bytes:-0}" >> "$OUT"
    sleep 1
done
