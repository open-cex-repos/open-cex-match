package com.ocx.metrics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 撮合引擎 metrics exporter 独立进程。
 *
 * <p>订阅各 symbol 的 metrics(symbol) Aeron 流（core 的 METRICS/EVENT 行 + egress 的
 * OUTBOUND_STATS 行），聚合后以 Prometheus 文本格式暴露 /metrics。撮合引擎本身
 * headless（不监听 HTTP），可观测性由本进程承担（2026-08-30 起不再读 Chronicle 队列）。</p>
 */
@SpringBootApplication
public class MetricsExporterApplication {

    public static void main(String[] args) {
        SpringApplication.run(MetricsExporterApplication.class, args);
    }
}
