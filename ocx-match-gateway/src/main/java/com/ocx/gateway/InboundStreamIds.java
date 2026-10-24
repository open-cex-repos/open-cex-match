package com.ocx.gateway;

/**
 * 引擎入站 stream id 计算（2026-09-01）：与 ocx-match-core
 * {@code StreamIds.inbound(symbol)} 保持一致（INBOUND_BASE=2000、SPAN=30000、
 * FNV-1a 32 位）。网关按 symbol 计算发布目标流；交叉校验见单测
 * {@code InboundStreamIdsTest}（依赖 core 类对比）。
 */
public final class InboundStreamIds {

    private static final int INBOUND_BASE = 2_000;
    private static final int SPAN = 30_000;

    private InboundStreamIds() {}

    public static int inbound(String symbol) {
        return INBOUND_BASE + fnv1a(symbol) % SPAN;
    }

    private static int fnv1a(String s) {
        int hash = 0x811c9dc5;
        for (int i = 0; i < s.length(); i++) {
            hash ^= s.charAt(i);
            hash *= 0x01000193;
        }
        return hash & 0x7fffffff;
    }
}
