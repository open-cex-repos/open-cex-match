package com.ocx.metrics;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

/**
 * 撮合引擎指标。不使用 Micrometer，直接维护原始计数器与 HdrHistogram，
 * 快照经 metrics 流直达 sidecar（2026-08-30，Chronicle metrics 队列已删）。
 */
public class MatchMetrics {

    /**
     * 观测模式（-Docx.match.metrics.mode=full|batch|off）：
     * <ul>
     *   <li>FULL：逐单 nanoTime + HdrHistogram 记录（原始语义，观测税最高）；</li>
     *   <li>BATCH：撮合线程只累加批级计数，批末一次提交（每批 1 次时钟 + 1-2 次直方图记录，
     *       观测税降到 ~1/256）；</li>
     *   <li>OFF：完全跳过逐单时钟与指标，只保留最终可从 producer 侧对账的吞吐。</li>
     * </ul>
     */
    public enum Mode {
        FULL,
        BATCH,
        OFF
    }

    private final Mode mode;

    /**
     * 每 symbol 预计算的指标键缓存 — 热路径 recordOrder/recordDuration 每单都做
     * 字符串拼接（JFR 40K×30min 实测占撮合线程分配 ~20%），改为构造时/首次使用时
     * 一次性拼接，运行期只做数组索引。
     */
    private static final class SymbolKeys {
        /** 热路径订单计数（[limit, market]）— 替代全局 ConcurrentHashMap<String, LongAdder> 查找。 */
        final LongAdder[] orderCounters = {new LongAdder(), new LongAdder()};

        /** 热路径成交计数（symbol 级，无 type 维度）。 */
        final LongAdder tradeCounter = new LongAdder();

        /** 热路径 HdrHistogram（[limit, market]）— 替代全局 map 查找。 */
        final Recorder[] latencyRecorders = {new Recorder(60_000L, 3), new Recorder(60_000L, 3)};

        /**
         * getIntervalHistogram 回收目标：无参版每次 new Histogram（大数组分配，
         * JFR 卡点 Recorder.getIntervalHistogram）；快照线程单写复用，轮换后更新引用。
         */
        Histogram intervalRecycle;

        /** 单批最大事件数（替代全局 map）。 */
        final LongAccumulator batchSizeMax = new LongAccumulator(Long::max, 0L);

        /** 入站校验推导的 tickSize/stepSize（首次使用时计算并缓存，避免每单 Decimal.parse）。 */
        volatile com.ocx.math.Decimal tickSize;

        volatile com.ocx.math.Decimal stepSize;

        SymbolKeys() {}
    }

    private static final String TYPE_LIMIT = "limit";
    private static final String TYPE_MARKET = "market";

    private final ConcurrentHashMap<String, SymbolKeys> symbolKeysCache = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, com.ocx.book.OrderBook> orderBooksRefs =
            new ConcurrentHashMap<>();

    private final JvmMetricsSampler jvmMetricsSampler = new JvmMetricsSampler();
    private final AtomicLong rejectedTotal = new AtomicLong();

    /** 订单簿挂单上限拒绝计数（独立于总拒绝数，告警口径专用）。 */
    private final AtomicLong orderBookCapRejected = new AtomicLong();

    /** 出站 offer 背压累计计数（AeronFillSink 负返回时记录）。 */
    private final java.util.concurrent.atomic.LongAdder outboundBackpressure =
            new java.util.concurrent.atomic.LongAdder();

    /** 入站环积压（消息数，AeronMetricsPublisher 按 symbol 采样注入）。 */
    private final Map<String, Long> inboundBacklog = new ConcurrentHashMap<>();

    private volatile long lastOrderTs = 0;
    private volatile double currentTps = 0.0;
    private volatile long consumerLag = 0;

    public MatchMetrics() {
        this(Mode.FULL);
    }

    public MatchMetrics(Mode mode) {
        this.mode = mode == null ? Mode.FULL : mode;
    }

    public Mode mode() {
        return mode;
    }

    /** 从系统属性解析模式（非法值回退 BATCH；生产默认批级聚合，FULL 需显式指定）。 */
    public static Mode modeFromProperty() {
        String v = System.getProperty("ocx.match.metrics.mode", "batch");
        try {
            return Mode.valueOf(v.trim().toUpperCase(java.util.Locale.ROOT));
        } catch (Exception e) {
            return Mode.BATCH;
        }
    }

    /** type 字符串 → 固定下标（LIMIT=0, MARKET=1）；未知类型返回 -1，走冷路径拼接。 */
    private static int typeIndex(String type) {
        if (TYPE_LIMIT.equals(type)) return 0;
        if (TYPE_MARKET.equals(type)) return 1;
        return -1;
    }

    private SymbolKeys symbolKeys(String symbol) {
        SymbolKeys keys = symbolKeysCache.get(symbol);
        if (keys == null) {
            SymbolKeys created = new SymbolKeys();
            SymbolKeys prev = symbolKeysCache.putIfAbsent(symbol, created);
            keys = prev != null ? prev : created;
        }
        return keys;
    }

    /**
     * 入站校验用的 tickSize（按 symbol 缓存）— 消费线程每单校验都做
     * Decimal.parse 是纯浪费，值在配置层恒定，首次计算后复用。
     * scale 为 null 或配置为空串时返回 null，语义与调用方现有分支一致。
     */
    public com.ocx.math.Decimal cachedTick(
            String symbol, com.ocx.config.MatchEngineProperties.CoinScale scale) {
        if (scale == null) return null;
        SymbolKeys keys = symbolKeys(symbol);
        com.ocx.math.Decimal tick = keys.tickSize;
        if (tick == null) {
            synchronized (keys) {
                tick = keys.tickSize;
                if (tick == null) {
                    String tickText = scale.getTickSize().orElse(null);
                    com.ocx.math.Decimal configured =
                            tickText != null && !tickText.isBlank()
                                    ? com.ocx.math.Decimal.parse(tickText)
                                    : null;
                    tick =
                            configured != null
                                            && configured.compareTo(com.ocx.math.Decimal.ZERO) > 0
                                    ? configured
                                    : com.ocx.math.Decimal.of(1L, -scale.getCoinScale());
                    keys.tickSize = tick;
                }
            }
        }
        return tick;
    }

    /** 同 {@link #cachedTick}，推导 stepSize。 */
    public com.ocx.math.Decimal cachedStep(
            String symbol, com.ocx.config.MatchEngineProperties.CoinScale scale) {
        if (scale == null) return null;
        SymbolKeys keys = symbolKeys(symbol);
        com.ocx.math.Decimal step = keys.stepSize;
        if (step == null) {
            synchronized (keys) {
                step = keys.stepSize;
                if (step == null) {
                    String stepText = scale.getStepSize().orElse(null);
                    com.ocx.math.Decimal configured =
                            stepText != null && !stepText.isBlank()
                                    ? com.ocx.math.Decimal.parse(stepText)
                                    : null;
                    step =
                            configured != null
                                            && configured.compareTo(com.ocx.math.Decimal.ZERO) > 0
                                    ? configured
                                    : com.ocx.math.Decimal.of(1L, -scale.getBaseCoinScale());
                    keys.stepSize = step;
                }
            }
        }
        return step;
    }

    public void recordOrder(String symbol, String type) {
        int idx = typeIndex(type);
        if (idx >= 0) {
            // 热路径：per-symbol 数组下标，零 String HashMap 查找（P2）
            symbolKeys(symbol).orderCounters[idx].increment();
        }
        lastOrderTs = System.currentTimeMillis();
    }

    public void recordBatchSize(String symbol, long batchSize) {
        if (batchSize <= 0) return;
        symbolKeys(symbol).batchSizeMax.accumulate(batchSize);
    }

    public void recordTrades(String symbol, int count) {
        if (count == 0) return;
        symbolKeys(symbol).tradeCounter.add(count);
    }

    public void recordDuration(String symbol, String type, long elapsedMs) {
        int idx = typeIndex(type);
        if (idx >= 0) {
            // 热路径：per-symbol 数组下标（P2）
            symbolKeys(symbol).latencyRecorders[idx].recordValue(Math.max(0L, elapsedMs));
        }
    }

    /**
     * 撮合热路径合并指标：一次 {@code symbolKeys} 查找同时完成订单计数、成交计数与
     * 延迟记录——替代 {@link #recordOrder} + {@link #recordTrades} + {@link #recordDuration}
     * 三次查找（每单两个 ConcurrentHashMap.get）。
     */
    public void recordMatch(String symbol, String type, int tradeCount, long elapsedMs) {
        if (mode == Mode.OFF) {
            return;
        }
        SymbolKeys keys = symbolKeys(symbol);
        int idx = typeIndex(type);
        if (idx >= 0) {
            keys.orderCounters[idx].increment();
            keys.latencyRecorders[idx].recordValue(Math.max(0L, elapsedMs));
        }
        if (tradeCount > 0) {
            keys.tradeCounter.add(tradeCount);
        }
        lastOrderTs = System.currentTimeMillis();
    }

    /**
     * BATCH 模式批末提交：撮合线程批级累加一次入账（LongAdder.add + 每型 1 次直方图记录），
     * 延迟记录为批均耗时（ms；4.9M TPS 下逐单耗时 <1ms，FULL 模式本就多为 0）。
     */
    public void recordBatchMatch(
            String symbol, long limitCount, long marketCount, long trades, long elapsedNanos) {
        if (mode == Mode.OFF || (limitCount == 0L && marketCount == 0L)) {
            return;
        }
        SymbolKeys keys = symbolKeys(symbol);
        if (limitCount > 0L) {
            keys.orderCounters[0].add(limitCount);
            keys.latencyRecorders[0].recordValue(
                    Math.max(0L, elapsedNanos / limitCount / 1_000_000L));
        }
        if (marketCount > 0L) {
            keys.orderCounters[1].add(marketCount);
            keys.latencyRecorders[1].recordValue(
                    Math.max(0L, elapsedNanos / marketCount / 1_000_000L));
        }
        if (trades > 0L) {
            keys.tradeCounter.add(trades);
        }
        lastOrderTs = System.currentTimeMillis();
    }

    public void recordRejectedOrder(String symbol, String reason) {
        if (mode == Mode.OFF) {
            return;
        }
        rejectedTotal.incrementAndGet();
    }

    /** 订单簿挂单数达上限导致的拒绝（reason=order_book_cap）。
     *  只加明细计数；rejectedTotal 由 recordRejectedOrder 统一累加，避免双计。 */
    public void recordOrderBookCapReject() {
        if (mode == Mode.OFF) {
            return;
        }
        orderBookCapRejected.incrementAndGet();
    }

    public void setCurrentTps(double tps) {
        this.currentTps = tps;
    }

    public double getCurrentTps() {
        return currentTps;
    }

    public void setConsumerLag(long lag) {
        this.consumerLag = lag;
    }

    public long getConsumerLag() {
        return consumerLag;
    }

    /** 全 symbol 成交总数（HA 心跳等低频通道用，热路径零开销）。 */
    public long totalTrades() {
        long sum = 0L;
        for (SymbolKeys keys : symbolKeysCache.values()) {
            sum += keys.tradeCounter.sum();
        }
        return sum;
    }

    public long getRejectedTotal() {
        return rejectedTotal.get();
    }

    /** 合并 ingress 进程的拒绝增量（三进程模式下入站侧校验拒绝需计入总量）。 */
    public void addRejectedDelta(long delta) {
        if (delta > 0) {
            rejectedTotal.addAndGet(delta);
        }
    }

    /**
     * 快照采集安全门：仅在低负载时允许 Disruptor 线程执行全量快照复制。
     * 压测/高负载期间会跳过快照，避免热路径产生大对象分配和长停顿。
     */
    public boolean isSnapshotSafe() {
        return currentTps < 1_000.0 && consumerLag < 1_000L;
    }

    public void registerOrderBook(com.ocx.book.OrderBook orderBook) {
        if (orderBook != null && orderBook.symbol() != null) {
            orderBooksRefs.put(orderBook.symbol(), orderBook);
        }
    }

    public void recordOutboundBackpressure() {
        outboundBackpressure.increment();
    }

    public long outboundBackpressureTotal() {
        return outboundBackpressure.sum();
    }

    public void setInboundBacklog(String symbol, long messages) {
        inboundBacklog.put(symbol, messages);
    }

    public MetricsSnapshot snapshot(String symbol, boolean overloaded) {
        SymbolKeys keys = symbolKeys(symbol);
        double[] p = latencyPercentiles(keys, keys.latencyRecorders[0]);
        com.ocx.book.OrderBook ob = orderBooksRefs.get(symbol);
        return new MetricsSnapshot(
                symbol,
                System.currentTimeMillis(),
                lastOrderTs,
                overloaded,
                consumerLag,
                inboundBacklog.getOrDefault(symbol, 0L),
                currentTps,
                keys.orderCounters[0].sum(),
                keys.orderCounters[1].sum(),
                keys.tradeCounter.sum(),
                rejectedTotal.get(),
                orderBookCapRejected.get(),
                ob == null ? 0L : ob.openOrderCount(),
                ob == null ? 0L : ob.getMaxOpenOrders(),
                depth(symbol, true),
                depth(symbol, false),
                p[0],
                p[1],
                p[2],
                p[3],
                p[4],
                keys.batchSizeMax.getThenReset(),
                jvmMetricsSampler.sample(),
                ob == null ? 0L : ob.slotsUsed(),
                outboundBackpressure.sum(),
                ob == null ? 0L : ob.buyOpenCount(),
                ob == null ? 0L : ob.sellOpenCount());
    }

    private int depth(String symbol, boolean bid) {
        com.ocx.book.OrderBook ob = orderBooksRefs.get(symbol);
        if (ob == null) return 0;
        return ob.priceLevelSize(bid ? 0 : 1);
    }

    private double[] latencyPercentiles(SymbolKeys keys, Recorder recorder) {
        double[] v = new double[5];
        if (recorder == null) {
            return v;
        }
        // 复用回收目标（容量足够就地复用，避免每次快照 new 大数组）；返回实例可能轮换，需回写
        Histogram interval = recorder.getIntervalHistogram(keys.intervalRecycle);
        keys.intervalRecycle = interval;
        v[0] = interval.getValueAtPercentile(50.0);
        v[1] = interval.getValueAtPercentile(95.0);
        v[2] = interval.getValueAtPercentile(99.0);
        v[3] = interval.getValueAtPercentile(99.9);
        v[4] = interval.getMaxValue();
        return v;
    }
}
