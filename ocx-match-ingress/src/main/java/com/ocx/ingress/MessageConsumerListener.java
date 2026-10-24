package com.ocx.ingress;

import com.ocx.codec.IoCodec;
import com.ocx.codec.sbe.OrderInDecoder;
import com.ocx.metrics.MatchMetrics;
import com.ocx.outbound.DeadLetterSink;
import com.ocx.outbound.MatchEventSink;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 入站唯一入口：Aeron UDP 收单线程把 OCX2/SBE OrderIn 帧校验/路由后**原样透传**进撮合核心。
 *
 * <p>只做三件事：魔数/templateId/长度校验 → 读定长 symbol 路由 → 原始帧透传发布
 * （+ 收单时刻）。不再反序列化、不再字段解析、不再重新编码——字段解析
 * 全部下沉到撮合线程按偏移直读（{@code IoCodec.readOrderIn}）。</p>
 *
 * <p><b>⚠️ 帧内 {@code id} 必须满足设计文档 02-data-structure.md §1.3 发号规则</b>（Snowflake 化纯业务号：
 * {@code orderId = (seg << 42) | (发号方ID << 34) | counter}；引擎内部
 * orderId→slot 映射落位，ID 不内嵌槽位）。
 * 生产链路由网关换发（{@code clientOrderId → orderId}，见 ocx-match-gateway）；
 * 新接入方不得把任意 clientOrderId 直接写入 {@code OrderIn.id}，否则同槽冲突率飙升。</p>
 */
public class MessageConsumerListener {

    private final OrderInPublisher orderInPublisher;
    private final MatchMetrics matchMetrics;
    private final DeadLetterSink dltSink;
    private final com.ocx.config.MatchEngineProperties matchEngineProperties;

    /** 关键事件出口（组合根注入；测试/降级场景为 null 时静默）。 */
    private final MatchEventSink eventSink;

    /** 滑动窗口 TPS 计数 — 每秒重置 */
    private final AtomicLong windowCounter = new AtomicLong(0);

    /** 入站环积压探测器（startSampler 时注入共享媒体驱动计数器）。 */
    private volatile AeronIntakeLagProbe lagProbe;

    /** symbol 路由表：配置的每个交易对 → 定长 16 字节 ASCII 键。 */
    private volatile List<SymbolRoute> symbolRoutes = new ArrayList<>();

    /** 路由比较探针（消费线程单线程复用，16 字节，零分配）。 */
    private final byte[] symbolProbe = new byte[IoCodec.ORDER_IN_SYMBOL_LENGTH];

    /** 建 symbol 路由表（bootstrap 启动前调用一次）。 */
    public void initRoutes() {
        initSymbolRoutes();
    }

    public MessageConsumerListener(
            OrderInPublisher orderInPublisher,
            MatchMetrics matchMetrics,
            DeadLetterSink dltSink,
            com.ocx.config.MatchEngineProperties matchEngineProperties,
            MatchEventSink eventSink) {
        this.orderInPublisher = orderInPublisher;
        this.matchMetrics = matchMetrics;
        this.dltSink = dltSink;
        this.matchEngineProperties = matchEngineProperties;
        this.eventSink = eventSink;
    }

    /** 启动后台采样线程：TPS 每秒 + 入站环积压每 5 秒（counters 来自共享媒体驱动）。 */
    public void startSampler(CountersReader counters) {
        List<String> symbols = new ArrayList<>(symbolRoutes.size());
        for (SymbolRoute r : symbolRoutes) {
            symbols.add(r.symbol);
        }
        lagProbe = counters == null ? null : new AeronIntakeLagProbe(counters, symbols);
        Thread tpsSampler =
                new Thread(
                        () -> {
                            long tick = 0;
                            while (!Thread.currentThread().isInterrupted()) {
                                try {
                                    Thread.sleep(1000);
                                    tick++;
                                    double tps = windowCounter.getAndSet(0);
                                    matchMetrics.setCurrentTps(tps);
                                    if (tick % 5 == 0) {
                                        AeronIntakeLagProbe probe = lagProbe;
                                        if (probe != null) {
                                            matchMetrics.setConsumerLag(probe.totalBacklog());
                                        } else {
                                            matchMetrics.setConsumerLag(-1);
                                        }
                                    }
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    break;
                                }
                            }
                        },
                        "tps-sampler");
        tpsSampler.setDaemon(true);
        tpsSampler.start();
    }

    private void initSymbolRoutes() {
        java.util.Map<String, com.ocx.config.MatchEngineProperties.CoinScale> symbols =
                matchEngineProperties == null ? null : matchEngineProperties.getSymbols();
        if (symbols == null || symbols.isEmpty()) {
            // 启动期配置兜底提示走 stderr（metrics 队列此时尚无 symbol 可写）。
            System.err.println("[MessageConsumerListener] 未配置任何 symbol，入站订单将全部被拒绝");
            return;
        }
        List<SymbolRoute> routes = new ArrayList<>(symbols.size());
        for (String symbol : symbols.keySet()) {
            byte[] key = new byte[IoCodec.ORDER_IN_SYMBOL_LENGTH];
            byte[] src = symbol.getBytes(StandardCharsets.US_ASCII);
            if (src.length > IoCodec.ORDER_IN_SYMBOL_LENGTH) {
                throw new IllegalStateException("symbol 超过 16 字节，无法编码为定长路由键: " + symbol);
            }
            System.arraycopy(src, 0, key, 0, src.length);
            routes.add(new SymbolRoute(symbol, key));
        }
        this.symbolRoutes = routes;
    }

    /** 按定长 symbol 键路由：命中返回配置索引（与发布器槽位数组一一对应），未命中返回 -1。 */
    private int routeIndex(DirectBuffer payload, int offset) {
        IoCodec.readOrderInSymbol(payload, offset, symbolProbe);
        List<SymbolRoute> routes = symbolRoutes;
        for (int i = 0; i < routes.size(); i++) {
            SymbolRoute r = routes.get(i);
            if (r.matches(symbolProbe)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 入站唯一入口：只接受 SBE（OCX2）二进制订单，零拷贝直读。
     * 校验失败（非 SBE/坏帧/未知 symbol）记指标 + 进 DLT，不阻塞队列。
     */
    public void onOrderIn(byte[] payload) {
        if (payload == null) {
            return;
        }
        onOrderIn(new UnsafeBuffer(payload), 0, payload.length);
    }

    public void onOrderIn(DirectBuffer payload, int offset, int length) {
        if (payload == null || length <= 0) {
            return;
        }
        if (length < IoCodec.MIN_ORDER_IN_FRAME_LENGTH) {
            matchMetrics.recordRejectedOrder("unknown", "frame_too_short");
            sendToDlt(payload, offset, length, "frame_too_short");
            return;
        }
        if (!IoCodec.isSbe(payload, offset, length)) {
            matchMetrics.recordRejectedOrder("unknown", "deserialization_failure");
            sendToDlt(payload, offset, length, "deserialization_failure");
            return;
        }
        // SBE header 内 templateId（MAGIC 之后第 6 字节起，uint16 小端）。
        // 显式传 OrderInDecoder.BYTE_ORDER：agrona 无 order 参数的重载是 native
        // 字节序（x86/ARM 碰巧小端），不是 SBE 语义，跨平台不可依赖。
        if (payload.getShort(offset + IoCodec.MAGIC_LEN + 2, OrderInDecoder.BYTE_ORDER)
                != OrderInDecoder.TEMPLATE_ID) {
            matchMetrics.recordRejectedOrder("unknown", "deserialization_failure");
            sendToDlt(payload, offset, length, "deserialization_failure");
            return;
        }
        // SBE header 版本/块长断言：core 热路径用常量 BLOCK_LENGTH/SCHEMA_VERSION wrap
        // （跳过 header 读取省 ~10% 采样），此处必须校验——schema 升级时旧消费者立即
        // 拒绝而非错位解码（DLL 兜底，不静默）
        if (payload.getShort(offset + IoCodec.MAGIC_LEN, OrderInDecoder.BYTE_ORDER)
                        != OrderInDecoder.BLOCK_LENGTH
                || payload.getShort(offset + IoCodec.MAGIC_LEN + 4, OrderInDecoder.BYTE_ORDER)
                        != OrderInDecoder.SCHEMA_ID
                || payload.getShort(offset + IoCodec.MAGIC_LEN + 6, OrderInDecoder.BYTE_ORDER)
                        != OrderInDecoder.SCHEMA_VERSION) {
            matchMetrics.recordRejectedOrder("unknown", "sbe_header_mismatch");
            sendToDlt(payload, offset, length, "sbe_header_mismatch");
            return;
        }
        int symbolIndex = routeIndex(payload, offset);
        if (symbolIndex < 0) {
            matchMetrics.recordRejectedOrder("unknown", "symbol_unknown");
            sendToDlt(payload, offset, length, "symbol_unknown");
            return;
        }
        orderInPublisher.onData(payload, offset, length, symbolIndex);
        windowCounter.incrementAndGet();
    }

    /** 异常/脏数据 → Archive DLT 死信流，best-effort，发送失败只记事件不影响主流程 */
    private void sendToDlt(DirectBuffer payload, int offset, int length, String reason) {
        if (dltSink == null) return;
        try {
            byte[] tmp = new byte[length];
            payload.getBytes(offset, tmp, 0, length);
            dltSink.send(IoCodec.encodeDltRecord(reason, truncate(hex(tmp))));
        } catch (Exception e) {
            if (eventSink != null) {
                eventSink.emitAll("dlt_send_failed", "reason=" + reason + " err=" + e.getMessage());
            }
        }
    }

    private static String truncate(String s) {
        return s == null ? "null" : (s.length() > 500 ? s.substring(0, 500) + "..." : s);
    }

    private static String hex(byte[] payload) {
        if (payload == null) {
            return "null";
        }
        return java.util.HexFormat.of().formatHex(payload);
    }

    /** 交易对路由键：配置 symbol → 定长 16 字节 ASCII 表示。 */
    private static final class SymbolRoute {
        private final String symbol;
        private final byte[] key;

        /** 16 字节定长路由键的 2×long 大端表示：匹配从逐字节循环降为 2 次 long 比较。 */
        private final long keyHi;

        private final long keyLo;

        SymbolRoute(String symbol, byte[] key) {
            this.symbol = symbol;
            this.key = key;
            this.keyHi = toLong(key, 0);
            this.keyLo = toLong(key, 8);
        }

        boolean matches(byte[] probe) {
            return keyHi == toLong(probe, 0) && keyLo == toLong(probe, 8);
        }

        /** 大端 8 字节合并（SBE symbol16 按 ASCII 大端存储）。 */
        private static long toLong(byte[] b, int off) {
            return ((long) (b[off] & 0xFF) << 56)
                    | ((long) (b[off + 1] & 0xFF) << 48)
                    | ((long) (b[off + 2] & 0xFF) << 40)
                    | ((long) (b[off + 3] & 0xFF) << 32)
                    | ((long) (b[off + 4] & 0xFF) << 24)
                    | ((long) (b[off + 5] & 0xFF) << 16)
                    | ((long) (b[off + 6] & 0xFF) << 8)
                    | (b[off + 7] & 0xFF);
        }
    }
}
