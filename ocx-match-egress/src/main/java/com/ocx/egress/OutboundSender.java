package com.ocx.egress;

import com.ocx.aeron.OcxAeron;
import com.ocx.book.MatchFill;
import com.ocx.codec.IoCodec;
import com.ocx.codec.IoCodec.CodecContext;
import com.ocx.outbound.FillSink;
import com.ocx.outbound.MatchEventSink;
import com.ocx.outbound.OutboundDepth;

import io.aeron.Publication;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.ManyToManyConcurrentArrayQueue;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

/**
 * 纵表链路出站发送管线（机械同理心：值容器池化、无锁队列、批量编码、发送线程单写）。
 *
 * <p>撮合线程（多 symbol）把本批 {@link MatchFill} 值拷贝入队后立即返回（零编码零网络）；
 * 独立发送线程批量消费，用 IoCodec 字段级 SBE 编码（零 ExchangeTrade 对象）拼批量帧
 * 一次 Aeron offer 到 Archive 记录流，offer 完成后归还 fill 池。深度走 {@link OutboundDepth}
 * 池化值容器。Bitvavo cold-log 模式：撮合链路只写 Archive 持久化日志，不再触碰 Kafka。</p>
 *
 * <p>撮合线程侧 {@link #submitFills(List)} 零分配（从池取）；发送线程空闲 park ~1us。
 * Archive 落盘由独立 Archive 进程异步完成；本侧仅对共享内存环 offer，offer 失败自旋
 * 重试（背压不丢数据），同时累积 backpressure 计数供 sidecar 抓取。</p>
 */
public final class OutboundSender implements FillSink {

    private static final int TRADE_QUEUE_CAPACITY = 1 << 16;
    private static final int DEPTH_QUEUE_CAPACITY = 1 << 10;
    private static final int FILL_POOL_CAPACITY = 1 << 14;
    private static final int DEPTH_POOL_CAPACITY = 1 << 5;
    private static final int TRADE_BATCH_MAX = 512;

    /** 出站背压退避：每 1024 次 spin 后 park 1ms（防纯自旋烧核）。 */
    private static final long BACKPRESSURE_PARK_NANOS = 1_000_000L;

    /** 无订阅者丢弃警告（仅 sender 线程访问，一次性）。 */
    private boolean noSubscriberWarned;

    private static final int TRADE_BATCH_BUF_SIZE =
            TRADE_BATCH_MAX * (IoCodec.FRAME_LENGTH_PREFIX + IoCodec.MAX_TRADE_FRAME_LENGTH);
    private static final long IDLE_PARK_NANOS = 1_000L;

    private final Map<String, Publication> tradePubs;
    private final Map<String, Publication> depthPubs;

    /** 出站录制解耦（2026-09-02）：发送线程写环、drain 线程按录制端节奏发布。 */
    private final Map<String, OutboundJournalRing> tradeRings = new HashMap<>();

    private final Map<String, OutboundJournalRing> depthRings = new HashMap<>();
    private final boolean journalBridgeEnabled =
            Boolean.parseBoolean(System.getProperty("ocx.journal-bridge.enabled", "false"));

    /** symbol → 定长 16 字节 ASCII 键（预编码，发送热路径免 String→UTF-8）。 */
    private final Map<String, byte[]> symbolKeys = new HashMap<>();

    private final MatchEventSink eventSink;
    private final OutboundStats stats;

    /** 撮合线程（多生产者）→ 发送线程（单消费者）。 */
    private final ManyToOneConcurrentArrayQueue<MatchFill> tradeQueue =
            new ManyToOneConcurrentArrayQueue<>(TRADE_QUEUE_CAPACITY);

    private final ManyToManyConcurrentArrayQueue<MatchFill> fillPool =
            new ManyToManyConcurrentArrayQueue<>(FILL_POOL_CAPACITY);
    private final ManyToOneConcurrentArrayQueue<OutboundDepth> depthQueue =
            new ManyToOneConcurrentArrayQueue<>(DEPTH_QUEUE_CAPACITY);
    private final ManyToManyConcurrentArrayQueue<OutboundDepth> depthPool =
            new ManyToManyConcurrentArrayQueue<>(DEPTH_POOL_CAPACITY);

    /** 发送线程独占。 */
    private final Map<String, List<MatchFill>> batchesBySymbol = new HashMap<>();

    private final byte[] batchBuf = new byte[TRADE_BATCH_BUF_SIZE];
    private final UnsafeBuffer batchWrap = new UnsafeBuffer(batchBuf);
    private final byte[] depthScratch = new byte[IoCodec.MAX_DEPTH_FRAME_LENGTH];
    private final UnsafeBuffer depthWrap = new UnsafeBuffer(depthScratch);

    private volatile boolean running = false;
    private Thread senderThread;

    public OutboundSender(
            OcxAeron aeron,
            Collection<String> symbols,
            MatchEventSink eventSink,
            OutboundStats stats) {
        this(
                buildTradePubs(aeron, symbols),
                buildDepthPubs(aeron, symbols),
                eventSink,
                stats,
                symbols);
    }

    OutboundSender(
            Map<String, Publication> tradePubs,
            Map<String, Publication> depthPubs,
            MatchEventSink eventSink,
            OutboundStats stats,
            Collection<String> symbols) {
        this.tradePubs = tradePubs;
        this.depthPubs = depthPubs;
        this.eventSink = eventSink;
        this.stats = stats;
        for (String s : tradePubs.keySet()) {
            symbolKeys.put(s, IoCodec.symbolKey(s));
        }
        for (int i = 0; i < FILL_POOL_CAPACITY; i++) {
            fillPool.offer(new MatchFill());
        }
        for (int i = 0; i < DEPTH_POOL_CAPACITY; i++) {
            depthPool.offer(new OutboundDepth());
        }
        if (journalBridgeEnabled) {
            int tradeRingSize = Integer.getInteger("ocx.outbound.trade.ring-size", 1 << 29);
            int depthRingSize = Integer.getInteger("ocx.outbound.depth.ring-size", 1 << 22);
            for (String s : symbols) {
                tradeRings.put(
                        s,
                        new OutboundJournalRing(
                                tradePubs.get(s), s, tradeRingSize, TRADE_BATCH_BUF_SIZE));
                depthRings.put(
                        s,
                        new OutboundJournalRing(
                                depthPubs.get(s),
                                s,
                                depthRingSize,
                                IoCodec.MAX_DEPTH_FRAME_LENGTH));
            }
        }
    }

    /** 测试/内嵌兼容：符号集从 tradePubs 键推导。 */
    OutboundSender(
            Map<String, Publication> tradePubs,
            Map<String, Publication> depthPubs,
            MatchEventSink eventSink,
            OutboundStats stats) {
        this(tradePubs, depthPubs, eventSink, stats, tradePubs.keySet());
    }

    private static Map<String, Publication> buildTradePubs(
            OcxAeron aeron, Collection<String> symbols) {
        Map<String, Publication> pubs = new HashMap<>();
        for (String s : symbols) {
            pubs.put(s, aeron.addArchiveTradePublication(s));
        }
        return pubs;
    }

    private static Map<String, Publication> buildDepthPubs(
            OcxAeron aeron, Collection<String> symbols) {
        Map<String, Publication> pubs = new HashMap<>();
        for (String s : symbols) {
            pubs.put(s, aeron.addArchiveDepthPublication(s));
        }
        return pubs;
    }

    public void start() {
        if (running) {
            return;
        }
        running = true;
        senderThread = new Thread(this::runLoop, "Outbound-Sender");
        senderThread.setDaemon(true);
        senderThread.start();
        for (OutboundJournalRing ring : tradeRings.values()) {
            ring.start();
        }
        for (OutboundJournalRing ring : depthRings.values()) {
            ring.start();
        }
    }

    public void stop() {
        running = false;
        for (OutboundJournalRing ring : tradeRings.values()) {
            ring.close();
        }
        for (OutboundJournalRing ring : depthRings.values()) {
            ring.close();
        }
        if (senderThread != null) {
            senderThread.interrupt();
            try {
                senderThread.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 撮合线程：本批成交值拷贝入队（从池取空容器，零分配；发送线程编码后归还）。
     * 队满自旋（Archive 背压，不丢数据）。
     */
    public void submitFills(List<MatchFill> fills) {
        for (int i = 0; i < fills.size(); i++) {
            MatchFill src = fills.get(i);
            MatchFill dst = fillPool.poll();
            if (dst == null) {
                dst = new MatchFill();
            }
            dst.direction = src.direction;
            dst.symbol = src.symbol;
            dst.takerOrderId = src.takerOrderId;
            dst.makerOrderId = src.makerOrderId;
            dst.price = src.price;
            dst.priceExponent = src.priceExponent;
            dst.qty = src.qty;
            dst.qtyExponent = src.qtyExponent;
            dst.takerCumQuote = src.takerCumQuote;
            dst.makerCumQuote = src.makerCumQuote;
            dst.turnOverExponent = src.turnOverExponent;
            dst.time = src.time;
            while (!tradeQueue.offer(dst)) {
                Thread.onSpinWait();
            }
        }
    }

    /** 撮合线程：领取深度值容器（池空回退堆分配）。 */
    public OutboundDepth acquireDepth(String symbol, int priceScale, int volumeScale) {
        OutboundDepth depth = depthPool.poll();
        if (depth == null) {
            depth = new OutboundDepth();
        }
        depth.symbol = symbol;
        depth.priceScale = priceScale;
        depth.volumeScale = volumeScale;
        depth.askCount = 0;
        depth.bidCount = 0;
        depth.ts = 0L;
        return depth;
    }

    public void submitDepth(OutboundDepth depth) {
        while (!depthQueue.offer(depth)) {
            Thread.onSpinWait();
        }
    }

    private void runLoop() {
        while (running) {
            try {
                if (drainOnce() == 0) {
                    LockSupport.parkNanos(IDLE_PARK_NANOS);
                }
            } catch (Throwable t) {
                emitAll("outbound_thread_error", t.getMessage());
            }
        }
        try {
            drainOnce();
        } catch (Throwable t) {
            emitAll("outbound_drain_error", t.getMessage());
        }
    }

    /** 批量消费并发送；返回处理条数（测试同步驱动用）。 */
    long drainOnce() {
        long n = 0;
        n += drainTrades();
        n += drainDepths();
        return n;
    }

    private long drainTrades() {
        long n = 0;
        Map<String, List<MatchFill>> pending = batchesBySymbol;
        // 发送线程每轮取一次编码上下文（此前每笔成交 3 次 ThreadLocal.get）
        CodecContext ctx = IoCodec.context();
        MatchFill fill;
        while ((fill = tradeQueue.poll()) != null) {
            List<MatchFill> batch =
                    pending.computeIfAbsent(fill.symbol, s -> new ArrayList<>(TRADE_BATCH_MAX));
            batch.add(fill);
            n++;
            if (batch.size() >= TRADE_BATCH_MAX) {
                sendTradeBatch(ctx, batch);
                batch.clear();
            }
        }
        for (List<MatchFill> batch : pending.values()) {
            if (!batch.isEmpty()) {
                sendTradeBatch(ctx, batch);
                batch.clear();
            }
        }
        return n;
    }

    /** 字段级批量编码（[len][frame] 拼接）→ 一次 Aeron offer 到 Archive → 归还 fill 池。 */
    private void sendTradeBatch(CodecContext ctx, List<MatchFill> batch) {
        if (batch.isEmpty()) {
            return;
        }
        String symbol = batch.get(0).symbol;
        Publication pub = tradePubs.get(symbol);
        try {
            if (pub == null) {
                emitAll("outbound_trade_dropped", "symbol=" + symbol + " no publication");
                return;
            }
            int pos = 0;
            for (MatchFill f : batch) {
                boolean takerBuy = f.direction == 0;
                int len =
                        IoCodec.encodeTradeFields(
                                ctx,
                                symbolKeys.get(f.symbol),
                                f.direction,
                                f.price,
                                f.priceExponent,
                                f.qty,
                                f.qtyExponent,
                                takerBuy ? f.takerOrderId : f.makerOrderId,
                                takerBuy ? f.makerOrderId : f.takerOrderId,
                                takerBuy ? f.takerCumQuote : f.makerCumQuote,
                                f.turnOverExponent,
                                takerBuy ? f.makerCumQuote : f.takerCumQuote,
                                f.turnOverExponent,
                                f.time,
                                batchBuf,
                                pos + IoCodec.FRAME_LENGTH_PREFIX);
                writeIntBE(batchBuf, pos, len);
                pos += IoCodec.FRAME_LENGTH_PREFIX + len;
            }
            OutboundJournalRing ring = tradeRings.get(symbol);
            if (ring != null) {
                ring.offer(batchWrap, 0, pos);
            } else {
                offerWithBackpressure(pub, batchWrap, 0, pos, symbol);
            }
            if (stats != null) {
                stats.recordPublished(symbol, pos);
            }
        } catch (Exception ex) {
            emitAll(
                    "outbound_trade_send_failed",
                    "symbol=" + symbol + " size=" + batch.size() + " err=" + ex.getMessage());
        } finally {
            for (MatchFill f : batch) {
                fillPool.offer(f);
            }
        }
    }

    private long drainDepths() {
        long n = 0;
        OutboundDepth depth;
        while ((depth = depthQueue.poll()) != null) {
            try {
                Publication pub = depthPubs.get(depth.symbol);
                if (pub == null) {
                    emitEvent(depth.symbol, "outbound_depth_dropped", "no publication");
                    continue;
                }
                int len =
                        IoCodec.encodeDepthSnapshot(
                                depth.symbol,
                                depth.ts,
                                depth.priceScale,
                                depth.volumeScale,
                                depth.askPrices,
                                depth.askVolumes,
                                depth.askCount,
                                depth.bidPrices,
                                depth.bidVolumes,
                                depth.bidCount,
                                depthScratch);
                OutboundJournalRing ring = depthRings.get(depth.symbol);
                if (ring != null) {
                    ring.offer(depthWrap, 0, len);
                } else {
                    offerWithBackpressure(pub, depthWrap, 0, len, depth.symbol);
                }
                if (stats != null) {
                    stats.recordPublished(depth.symbol, len);
                }
            } catch (Exception ex) {
                emitEvent(depth.symbol, "outbound_depth_send_failed", ex.getMessage());
            } finally {
                depthPool.offer(depth);
            }
            n++;
        }
        return n;
    }

    /** 背压语义：offer 失败自旋重试（不丢数据），周期上报事件 + 计数。 */
    private long offerWithBackpressure(
            Publication pub, DirectBuffer buf, int offset, int length, String symbol) {
        long result;
        int spins = 0;
        while ((result = pub.offer(buf, offset, length)) < 0) {
            // 无订阅者（零落盘形态 record-outbound=false：Archive 不 recording，
            // 出站流无消费者）：丢弃本帧继续，避免无限重试把出站队列/环堵死，
            // 回压到撮合线程、饿死入站链路（2026-08-31 复现：producer 卡 ~100 万帧）。
            if (!pub.isConnected()) {
                if (!noSubscriberWarned) {
                    noSubscriberWarned = true;
                    emitAll(
                            "egress_archive_no_subscriber",
                            "symbol=" + symbol + " offer=" + result + "（无订阅者：出站帧丢弃，零落盘形态）");
                }
                return 0L;
            }
            spins++;
            if (stats != null) {
                stats.recordBackpressure(symbol);
            }
            if ((spins & 1023) == 0) {
                emitAll(
                        "egress_archive_backpressure",
                        "symbol=" + symbol + " offer=" + result + " spins=" + spins);
                // 退避让出 CPU：正常背压（Archive recording 消费慢）与零落盘形态
                // （record-outbound=false，无订阅者 → term 填满后 offer 永久负值）
                // 都不能纯自旋——否则占满 P 核、饿死同机媒体驱动收单线程
                // （2026-08-31 零落盘 600s 复现：Outbound-Sender 满核 183s，
                // producer 入站 UDP 背压停发）。
                LockSupport.parkNanos(BACKPRESSURE_PARK_NANOS);
            } else {
                Thread.onSpinWait();
            }
        }
        return result;
    }

    private static void writeIntBE(byte[] buf, int offset, int value) {
        buf[offset] = (byte) (value >>> 24);
        buf[offset + 1] = (byte) (value >>> 16);
        buf[offset + 2] = (byte) (value >>> 8);
        buf[offset + 3] = (byte) value;
    }

    private void emitAll(String event, String detail) {
        if (eventSink != null) {
            eventSink.emitAll(event, detail);
        }
    }

    private void emitEvent(String symbol, String event, String detail) {
        if (eventSink != null) {
            eventSink.emit(symbol, event, detail);
        }
    }
}
