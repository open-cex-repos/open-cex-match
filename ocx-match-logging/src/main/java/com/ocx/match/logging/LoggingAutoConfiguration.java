package com.ocx.match.logging;

import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

/**
 * 自动注册 TraceFilter，注入 traceId 到 MDC。
 *
 * <p>仅在 Servlet 环境加载。WebFlux 服务（如 ocx-gateway）无 servlet 容器，
 * 加载 FilterRegistrationBean 会因缺少 {@code jakarta.servlet.Filter} 崩溃
 * （NoClassDefFoundError）——2026-08-11 实测 gateway 启动循环。
 */
@Configuration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
public class LoggingAutoConfiguration {

    @Bean
    public FilterRegistrationBean<TraceFilter> traceFilterRegistration() {
        FilterRegistrationBean<TraceFilter> reg = new FilterRegistrationBean<>();
        reg.setFilter(new TraceFilter());
        reg.addUrlPatterns("/*");
        reg.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return reg;
    }
}
