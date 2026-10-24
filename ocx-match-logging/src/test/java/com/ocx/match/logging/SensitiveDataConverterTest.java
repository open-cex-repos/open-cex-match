package com.ocx.match.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;

import org.junit.jupiter.api.Test;

class SensitiveDataConverterTest {

    private final SensitiveDataConverter converter = new SensitiveDataConverter();

    private String mask(String msg) {
        LoggerContext ctx = new LoggerContext();
        Logger logger = ctx.getLogger("test");
        LoggingEvent event = new LoggingEvent("fqcn", logger, Level.INFO, msg, null, null);
        return converter.convert(event);
    }

    @Test
    void masksMobile() {
        assertEquals("138****5678", mask("13812345678"));
    }

    @Test
    void masksPasswordJson() {
        assertEquals("{\"password\":\"****\"}", mask("{\"password\":\"abc123\"}"));
    }

    @Test
    void masksToken() {
        String raw = "token=eyJhbGciOiJIUzI1NiJ9abc";
        String out = mask(raw);
        assertFalse(out.contains("eyJhbGciOiJIUzI1NiJ9abc"));
    }

    @Test
    void keepsPlainLogs() {
        assertEquals("order 1001 matched", mask("order 1001 matched"));
    }
}
