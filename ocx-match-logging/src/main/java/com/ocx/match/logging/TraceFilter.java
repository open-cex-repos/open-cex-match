package com.ocx.match.logging;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;

import org.slf4j.MDC;

import java.io.IOException;

/**
 * 请求追踪过滤器 — 从 HTTP Header 提取或生成 traceId，注入 SLF4J MDC。
 * <p>
 * 优先级：X-Trace-Id > X-Request-Id > 自动生成 UUID。
 * 下游异步消息透传同一 traceId 实现全链路追踪。
 */
public class TraceFilter implements Filter {

    private static final String TRACE_ID = "traceId";
    private static final String HEADER_TRACE = "X-Trace-Id";
    private static final String HEADER_REQUEST = "X-Request-Id";

    /** 健康检查/监控端点 — 不需要 traceId 追踪 */
    private static final String[] SKIP_PREFIXES = {
        "/actuator", "/health", "/metrics", "/favicon.ico"
    };

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest httpReq = (HttpServletRequest) request;
        String path = httpReq.getRequestURI();

        // 跳过基础设施端点
        for (String prefix : SKIP_PREFIXES) {
            if (path.startsWith(prefix)) {
                chain.doFilter(request, response);
                return;
            }
        }

        String traceId = httpReq.getHeader(HEADER_TRACE);
        if (traceId == null || traceId.isEmpty()) {
            traceId = httpReq.getHeader(HEADER_REQUEST);
        }
        if (traceId == null || traceId.isEmpty()) {
            // 生产环境优先用上游 traceId；本地生成时用更轻量的 id 避免 SecureRandom 锁争用
            traceId = Long.toHexString(System.nanoTime());
        }

        MDC.put(TRACE_ID, traceId);
        // 回写响应头，便于调用方/前端拿到 traceId 关联排障
        if (response instanceof jakarta.servlet.http.HttpServletResponse httpResp) {
            httpResp.setHeader(HEADER_TRACE, traceId);
        }
        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(TRACE_ID);
        }
    }
}
