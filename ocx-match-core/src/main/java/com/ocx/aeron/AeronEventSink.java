package com.ocx.aeron;

import com.ocx.outbound.MatchEventSink;

import io.aeron.Publication;

import org.agrona.concurrent.UnsafeBuffer;

import java.util.Map;

/**
 * 关键事件信号的 Aeron 实现（core 侧）：EVENT 行经出站环交给 egress 落 metrics 队列。
 * 事件为可观测性信号，单次 offer、失败即弃（上游已有节流），绝不阻塞撮合线程。
 *
 * <p>调用方不止撮合线程：开发态下 ingress DLT 发送失败与出站发送失败
 * （egress 线程）也会触发，多 symbol 时多个撮合线程共享实例——编码缓冲必须线程局部，
 * 否则并发 emit 会互相覆写行内容。</p>
 */
public final class AeronEventSink implements MatchEventSink {

    private final Map<String, Publication> publications;
    private static final byte[] EMPTY_SYMBOL = AeronWire.symbolBytes(null);

    /** symbol → 16 字节预编码（事件低频，仍避免热路径 getBytes）。 */
    private final Map<String, byte[]> symbolBytes;

    /** 每线程独立编码缓冲：零竞争、稳态零分配（事件低频，每线程一个 256B 缓冲可忽略）。 */
    private final ThreadLocal<UnsafeBuffer> buffer =
            ThreadLocal.withInitial(() -> new UnsafeBuffer(new byte[256]));

    public AeronEventSink(Map<String, Publication> publications) {
        this.publications = publications;
        Map<String, byte[]> bytes = new java.util.HashMap<>(publications.size());
        for (String s : publications.keySet()) {
            bytes.put(s, AeronWire.symbolBytes(s));
        }
        this.symbolBytes = bytes;
    }

    @Override
    public void emit(String symbol, String event, String detail) {
        Publication pub = publications.get(symbol);
        if (pub == null) {
            return;
        }
        UnsafeBuffer buf = buffer.get();
        byte[] s16 = symbolBytes.getOrDefault(symbol, EMPTY_SYMBOL);
        pub.offer(buf, 0, AeronWire.encodeEvent(buf, s16, event, detail));
    }

    @Override
    public void emitAll(String event, String detail) {
        UnsafeBuffer buf = buffer.get();
        int len = AeronWire.encodeEvent(buf, EMPTY_SYMBOL, event, detail);
        for (Publication pub : publications.values()) {
            pub.offer(buf, 0, len);
        }
    }
}
