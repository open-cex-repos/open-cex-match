package com.ocx.match.logging;

import ch.qos.logback.classic.pattern.ClassicConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 纳秒精度时间戳转换器 — 用于审计日志（audit.log）。
 *
 * <p>logback 默认 {@code %d} 的时间源是 {@code System.currentTimeMillis()}，
 * 只有毫秒精度。撮合引擎审计日志需要微秒/纳秒级排序精度（同一毫秒内多条
 * 事件需要区分先后），因此提供 {@code %nanoTime} 转换字。
 *
 * <p>输出格式：{@code yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS+08:00}（9 位纳秒）。
 * 使用时在 logback-spring.xml 注册：
 * <pre>
 *   &lt;conversionRule conversionWord="nanoTime"
 *       converterClass="com.ocx.logging.NanoTimestampConverter"/&gt;
 * </pre>
 */
public class NanoTimestampConverter extends ClassicConverter {

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX")
                    .withZone(ZoneId.systemDefault());

    @Override
    public String convert(ILoggingEvent event) {
        return FORMATTER.format(Instant.ofEpochMilli(event.getTimeStamp()));
    }
}
