#!/usr/bin/env python3
"""全形态稳态平线分析工具（2026-09-01，录制端解耦专项）。

输入 Rust producer 的每 2s 采样日志（match-1.out），输出：
  - 每间隔 TPS 序列的 mean/std/p10/p50/p90/min/max
  - 自相关峰（间隔→秒，定位锯齿周期）
  - 背压退避（retries）与 TPS 的相关系数
可选叠加磁盘采样 TSV（perf-disk-sample.sh 产物）：
  - TPS 与磁盘写 MB/s 在 -5..+5s 各滞后的相关系数（定位录制落盘与吞吐的相位关系）
"""

import re
import sys
import statistics
from collections import deque


LINE_RE = re.compile(
    r"\[\s*(\d+)s\] sent=([\d,]+) errors=([\d,]+) retries=([\d,]+) avg_tps=([\d,]+)"
)


def parse_producer(paths):
    """返回 (t[], tps[], retries[])；t 为间隔中点秒，tps 为该间隔注入速率。"""
    ts, tps, retries = [], [], []
    prev = None
    for p in paths:
        with open(p, encoding="utf-8", errors="replace") as f:
            for line in f:
                m = LINE_RE.search(line)
                if not m:
                    continue
                t = int(m.group(1))
                sent = int(m.group(2).replace(",", ""))
                err = int(m.group(3).replace(",", ""))
                ret = int(m.group(4).replace(",", ""))
                if prev is None:
                    prev = (t, sent, err, ret)
                    continue
                pt, ps, pe, pr = prev
                dt = t - pt
                if dt <= 0 or sent < ps or ret < pr:
                    prev = (t, sent, err, ret)
                    continue
                ts.append((pt + t) / 2.0)
                tps.append((sent - ps) / dt)
                retries.append((ret - pr) / dt)
                prev = (t, sent, err, ret)
    return ts, tps, retries


def autocorr_peaks(x, max_lag):
    """返回前 max_lag 个自相关系数（lag 从 1 起）。"""
    n = len(x)
    if n < 3:
        return []
    mean = statistics.mean(x)
    var = sum((v - mean) ** 2 for v in x) / n
    if var == 0:
        return []
    out = []
    for lag in range(1, max_lag + 1):
        num = sum((x[i] - mean) * (x[i + lag] - mean) for i in range(n - lag))
        out.append(num / (n - lag) / var)
    return out


def corr(a, b):
    n = min(len(a), len(b))
    if n < 3:
        return float("nan")
    a, b = a[:n], b[:n]
    ma, mb = statistics.mean(a), statistics.mean(b)
    va = sum((v - ma) ** 2 for v in a)
    vb = sum((v - mb) ** 2 for v in b)
    if va == 0 or vb == 0:
        return 0.0
    return sum((a[i] - ma) * (b[i] - mb) for i in range(n)) / (va * vb) ** 0.5


def parse_disk_tsv(path):
    """返回 (epoch[], mb_s[], seg_count[], seg_bytes[])。"""
    ep, mb, seg, by = [], [], [], []
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = line.split()
            if len(parts) < 5 or not parts[0].replace(".", "", 1).isdigit():
                continue
            ep.append(float(parts[0]))
            mb.append(float(parts[3]))
            seg.append(float(parts[4]))
            by.append(float(parts[5]))
    return ep, mb, seg, by


def disk_2s_delta(dep, dbytes):
    """把 1s 采样聚成 2s 桶并求字节增量；返回 (t[], delta_bytes[])，t 为桶中点（相对 dep[0]）。"""
    base = dep[0]
    buckets = {}
    for t, v in zip(dep, dbytes):
        b = int((t - base) / 2.0)
        buckets[b] = v
    bs = sorted(buckets)
    out = []
    for i in range(1, len(bs)):
        out.append(((bs[i] + bs[i - 1]) / 2.0 * 2.0, buckets[bs[i]] - buckets[bs[i - 1]]))
    return out


def best_alignment_offset(ts, tps, bd):
    """交叉相关搜索 producer 与磁盘序列的最优对齐偏移（秒，正=磁盘领先）。"""
    best = (0, -1.0)
    # 采样器可能晚于 producer 启动数百秒，搜索窗放宽到 ±400s
    for off in range(-400, 401):
        pairs = []
        for t, v in zip(ts, tps):
            want = t + off
            idx = min(range(len(bd)), key=lambda k: abs(bd[k][0] - want))
            pairs.append((v, bd[idx][1]))
        va = [x[0] for x in pairs]
        vb = [x[1] for x in pairs]
        ma = statistics.mean(va)
        mb = statistics.mean(vb)
        na = sum((x - ma) ** 2 for x in va)
        nb = sum((x - mb) ** 2 for x in vb)
        if na == 0 or nb == 0:
            continue
        r = sum((va[i] - ma) * (vb[i] - mb) for i in range(len(va))) / (na * nb) ** 0.5
        if r > best[1]:
            best = (off, r)
    return best


def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        sys.exit(1)
    producer_logs = []
    disk_tsv = None
    steady_intervals = 180
    skip_intervals = None
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--disk":
            i += 1
            disk_tsv = args[i]
        elif a == "--steady-intervals":
            i += 1
            steady_intervals = int(args[i])
        elif a == "--skip-intervals":
            i += 1
            skip_intervals = int(args[i])
        else:
            producer_logs.append(a)
        i += 1
    if not producer_logs:
        print("no producer logs", file=sys.stderr)
        sys.exit(1)

    ts, tps, retries = parse_producer(producer_logs)
    if len(tps) < 4:
        print("producer 采样不足（<4 个间隔）", file=sys.stderr)
        sys.exit(1)

    # 稳态窗：默认取最后 180 间隔（360s）；可显式指定
    if skip_intervals is None:
        warm = max(0, len(tps) - steady_intervals)
    else:
        warm = skip_intervals
    st = tps[warm:]
    sr = retries[warm:]
    print("=== 稳态窗（跳过前 %d 间隔，窗宽 %d 间隔 = %.0fs）===" % (warm, len(st), len(st) * 2))
    print("TPS: n=%d mean=%.0f std=%.0f p10=%.0f p50=%.0f p90=%.0f min=%.0f max=%.0f"
          % (len(st), statistics.mean(st), statistics.stdev(st),
             sorted(st)[int(len(st) * 0.10)], statistics.median(st),
             sorted(st)[int(len(st) * 0.90) - 1], min(st), max(st)))
    print("retries/2s: mean=%.1f std=%.1f" % (statistics.mean(sr), statistics.stdev(sr)))
    ac = autocorr_peaks(st, 30)
    peaks = sorted(range(len(ac)), key=lambda k: ac[k], reverse=True)[:5]
    print("自相关峰（lag 间隔=2s → 周期秒）:")
    for k in sorted(peaks):
        print("  lag=%2d (%.1fs) r=%.3f" % (k + 1, (k + 1) * 2, ac[k]))
    print("TPS × retries 同期相关 r=%.3f" % corr(st, sr))

    if disk_tsv:
        dep, dmb, dseg, dby = parse_disk_tsv(disk_tsv)
        if len(dep) >= 3:
            print("=== 与磁盘采样相关（TPS ↔ 写MB/s，正滞后=磁盘领先）===")
            bd = disk_2s_delta(dep, dby)
            off, r0 = best_alignment_offset(ts[warm:], st, bd)
            print("  对齐偏移=%+ds（磁盘领先），基准相关 r=%.3f" % (off, r0))
            for lag_s in range(-6, 7, 2):
                pairs = []
                for t, v in zip(ts[warm:], st):
                    want = t + off + lag_s
                    idx = min(range(len(bd)), key=lambda k: abs(bd[k][0] - want))
                    pairs.append((v, bd[idx][1]))
                print("  lag=%+ds r=%.3f" % (lag_s, corr(*zip(*pairs)) if pairs else float("nan")))


if __name__ == "__main__":
    main()
