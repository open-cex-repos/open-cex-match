package com.ocx.metrics;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** 暴露 Prometheus 抓取的 /metrics 文本端点。 */
@RestController
public class MetricsController {

    private final MetricsAeronSource source;

    public MetricsController(MetricsAeronSource source) {
        this.source = source;
    }

    @GetMapping(value = "/metrics", produces = "text/plain; version=0.0.4; charset=utf-8")
    public String metrics() {
        return source.render();
    }
}
