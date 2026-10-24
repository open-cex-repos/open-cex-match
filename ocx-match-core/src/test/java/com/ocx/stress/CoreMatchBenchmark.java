package com.ocx.stress;

import com.ocx.book.MatchFill;
import com.ocx.book.OrderBook;
import com.ocx.book.TriggerSlots;
import com.ocx.codec.domain.OrderInFields;
import com.ocx.engine.MatchService;
import com.ocx.handler.OrderEvent;
import com.ocx.handler.OrderEventHandler;
import com.ocx.metrics.MatchMetrics;
import com.ocx.outbound.FillSink;
import com.ocx.outbound.OutboundDepth;

import java.util.List;
import java.util.Random;

/**
 * 纯撮合核心基准（绕过 ingress/egress/Aeron IPC/WAL）：
 * 直接构造 OrderInFields 喂给 OrderEventHandler.onEvent 全流程，测订单簿+撮合能力上限。
 *
 * <p>对比完整链路（producer→ingress→IPC→core→IPC→egress→Archive）的稳态，
 * 量化链路成本占比。订单模型与 ocx-stress-producer（Rust，已替代 Java 版）一致
 * （exp 价格 / 真实市场撤单模型：一单一撤 + 指数寿命 / 市价+条件单+冰山+大单混合），
 * 保证可比。2026-08-31 起撤单从"近窗随机重复选目标"改为"每笔限价单登记一次
 * 到期撤单"（对齐 Rust producer，撤单命中率从 ~2% 提到接近真实）。</p>
 *
 * <p>用法：java -cp test-classes:classes:deps com.ocx.stress.CoreMatchBenchmark [--metrics true|false]
 * 默认 metrics=false（纯撮合）；--metrics true 带上 MatchMetrics 热路径（接近真实）。
 * 默认运行时生成订单（Random/log 占 ~48% 采样，测得是含生成的上限）；--pregen 预生成
 * 1000 万订单流回放，消除生成开销，探测纯撮合真实上限。</p>
 */
public final class CoreMatchBenchmark {

    private static final String SYMBOL = "BTCUSDT";
    private static final int CAP = 5_000_000;
    private static final int SCALE = 8;
    private static final int WARMUP_SECS = 10;
    private static final int RUN_SECS = 90;
    private static final int BATCH = 500; // 每批订单数（对齐 ocx-stress-producer）
    private static final int BATCH_END_EVERY = 256; // onBatchEnd 频率（对齐 Aeron FRAGMENT_LIMIT）
    private static final int PREGEN_N = 10_000_000; // 预生成订单流长度（覆盖 2×5M 簿）

    // 订单混合（对齐 --big 压测）
    private static final double MARKET_RATIO = 0.10;
    private static final double STOP_RATIO = 0.02;
    private static final double ICEBERG_RATIO = 0.05;
    private static final double LARGE_RATIO = 0.02;

    /** 限价单指数寿命均值（帧）：引擎纯撮合 ~8M TPS 下 800 万帧 ≈ 1s（GDAX 2s 量级，
     * 折衷 pending 队列内存）。一单一撤：每笔限价单登记一次到期撤单。 */
    private static final long ORDER_LIFETIME_FRAMES = 8_000_000L;

    private static final double BASE_PRICE = 70000;
    private static final double SPREAD = 200;
    private static final double DECAY = 10;
    private static final double MID_VOL = 1.0;
    private static final double TICK = 0.01;
    private static final double MEAN_REVERSION = 0.002;

    private CoreMatchBenchmark() {}

    /** 到期撤单小顶堆（原始 long 数组，零装箱）：expiry 帧 → 目标 orderId。 */
    private static final class ExpiryHeap {
        private long[] expiry = new long[1 << 20];
        private long[] ids = new long[1 << 20];
        private int size;

        boolean isEmpty() {
            return size == 0;
        }

        long peekExpiry() {
            return expiry[0];
        }

        void push(long e, long id) {
            if (size == expiry.length) {
                int n = size << 1;
                expiry = java.util.Arrays.copyOf(expiry, n);
                ids = java.util.Arrays.copyOf(ids, n);
            }
            int i = size++;
            expiry[i] = e;
            ids[i] = id;
            while (i > 0) {
                int p = (i - 1) >>> 1;
                if (expiry[p] <= expiry[i]) {
                    break;
                }
                swap(p, i);
                i = p;
            }
        }

        /** 弹出堆顶并返回目标 id（调用方先 peekExpiry 判到期）。 */
        long pollId() {
            long id = ids[0];
            size--;
            expiry[0] = expiry[size];
            ids[0] = ids[size];
            int i = 0;
            while (true) {
                int l = (i << 1) + 1;
                if (l >= size) {
                    break;
                }
                int r = l + 1;
                int m = r < size && expiry[r] < expiry[l] ? r : l;
                if (expiry[m] >= expiry[i]) {
                    break;
                }
                swap(m, i);
                i = m;
            }
            return id;
        }

        private void swap(int a, int b) {
            long te = expiry[a];
            expiry[a] = expiry[b];
            expiry[b] = te;
            long ti = ids[a];
            ids[a] = ids[b];
            ids[b] = ti;
        }
    }

    public static void main(String[] args) throws Exception {
        boolean withMetrics = false;
        boolean pregen = false;
        for (int i = 0; i < args.length; i++) {
            if ("--metrics".equals(args[i])) {
                withMetrics = Boolean.parseBoolean(args[++i]);
            } else if ("--pregen".equals(args[i])) {
                pregen = true;
            }
        }

        // ── 装配（绕过 WAL/Aeron/Kafka/egress）──
        int levelCapacity = (int) Math.min(Math.max(CAP / 16, 1024), 1 << 20);
        OrderBook book = new OrderBook(SYMBOL, SCALE, SCALE, CAP, levelCapacity, CAP);
        TriggerSlots triggers =
                new TriggerSlots((int) Math.min(Math.max(CAP / 4, 1024), 1 << 20), 1 << 16);
        MatchMetrics metrics = withMetrics ? new MatchMetrics() : null;
        if (metrics != null) {
            metrics.registerOrderBook(book);
        }
        CountingFillSink fillSink = new CountingFillSink();
        OrderEventHandler handler =
                new OrderEventHandler(
                        book, triggers, new MatchService(false), fillSink, metrics, null);

        if (pregen) {
            runPregen(handler, book, fillSink);
            return;
        }

        // ── 订单流（复刻 ocx-stress-producer exp 模型）──
        Random rng = new Random(42);
        double mid = BASE_PRICE;
        long orderIdBase = System.nanoTime();
        ExpiryHeap pending = new ExpiryHeap();
        OrderEvent event = new OrderEvent();
        event.setSbeLength(1); // 对齐 AeronOrderInSource.onFragment（onEvent 入口检查）
        event.setSnapshotRequest(false);
        OrderInFields f = event.getFields();

        long total = 0, cancelCnt = 0, lastSent = 0;
        long start = System.currentTimeMillis();
        long phaseStart = start;
        long reportTs = start;

        while (true) {
            long now = System.currentTimeMillis();
            if (now - start >= WARMUP_SECS * 1000L && phaseStart == start) {
                phaseStart = now;
                lastSent = total;
            }
            if (now - start >= (WARMUP_SECS + RUN_SECS) * 1000L) {
                break;
            }
            if (now - reportTs >= 10_000) {
                long elapsed = (now - phaseStart) / 1000;
                long sent = total - lastSent;
                System.out.printf(
                        "[%ds] orders=%,d cancels=%,d fills=%,d rejects=%,d tps=%,.0f%n",
                        (now - start) / 1000,
                        sent,
                        cancelCnt,
                        fillSink.fills,
                        fillSink.rejects,
                        elapsed > 0 ? (double) sent / elapsed : 0);
                reportTs = now;
            }

            for (int i = 0; i < BATCH; i++) {
                mid += rng.nextGaussian() * MID_VOL + (BASE_PRICE - mid) * MEAN_REVERSION;
                // 到期撤单优先（真实市场：一单一撤，目标 = 寿命窗口内自己刚发的限价单）
                if (!pending.isEmpty() && pending.peekExpiry() <= total) {
                    long cancelId = pending.pollId();
                    f.id = cancelId;
                    f.userId = 1L;
                    f.status = 2; // 撤单
                    f.type = 1;
                    f.side = 0;
                    f.timeInForce = 0;
                    f.priceMantissa = 0;
                    f.priceExponent = -SCALE;
                    f.origQtyMantissa = 0;
                    f.origQtyExponent = -SCALE;
                    f.executedQtyMantissa = 0L;
                    f.executedQtyExponent = -SCALE;
                    f.mergeLowPriceMantissa = 0;
                    f.mergeLowPriceExponent = -SCALE;
                    f.visibleQtyMantissa = 0L;
                    f.visibleQtyExponent = -SCALE;
                    f.createTimeMillis = now;
                    handler.onEvent(event, 0L, false);
                    cancelCnt++;
                    total++;
                    if ((total & (BATCH_END_EVERY - 1)) == 0) {
                        handler.onBatchEnd();
                    }
                    continue;
                }

                int orderSide = rng.nextBoolean() ? 0 : 1;
                double roll = rng.nextDouble();
                int orderType =
                        roll < MARKET_RATIO
                                ? 2
                                : roll < MARKET_RATIO + STOP_RATIO ? 3 + rng.nextInt(4) : 1;
                boolean large = rng.nextDouble() < LARGE_RATIO;
                double qty = large ? 1.0 + rng.nextDouble() * 2.0 : 0.01 + rng.nextDouble() * 0.1;

                double price = 0;
                if (orderType != 2 && orderType != 4 && orderType != 6) {
                    double d = -DECAY * Math.log(1.0 - rng.nextDouble());
                    if (d > SPREAD) {
                        d = SPREAD;
                    }
                    price = orderSide == 0 ? mid - d : mid + d;
                    price = Math.round(price / TICK) * TICK;
                }

                f.id = orderIdBase + total;
                f.userId = rng.nextInt(100000);
                f.status = 0;
                f.type = (byte) orderType;
                f.side = (byte) orderSide;
                f.timeInForce = 0;
                f.priceMantissa =
                        orderType == 2 || orderType == 4 || orderType == 6
                                ? 0L
                                : Math.round(price * Math.pow(10, SCALE));
                f.priceExponent = -SCALE;
                f.origQtyMantissa = Math.round(qty * Math.pow(10, SCALE));
                f.origQtyExponent = -SCALE;
                f.executedQtyMantissa = 0L;
                f.executedQtyExponent = -SCALE;
                f.createTimeMillis = now;
                f.visibleQtyMantissa = 0L;
                f.visibleQtyExponent = -SCALE;
                if (orderType >= 3) {
                    double dist = (0.0005 + rng.nextDouble() * 0.002) * mid;
                    double trigger = mid + (rng.nextBoolean() ? dist : -dist);
                    f.mergeLowPriceMantissa = Math.round(trigger * Math.pow(10, SCALE));
                    f.mergeLowPriceExponent = -SCALE;
                } else {
                    f.mergeLowPriceMantissa = 0L;
                    f.mergeLowPriceExponent = -SCALE;
                }
                if (orderType == 1 && rng.nextDouble() < ICEBERG_RATIO) {
                    f.visibleQtyMantissa =
                            Math.round(qty * (0.2 + rng.nextDouble() * 0.4) * Math.pow(10, SCALE));
                }

                handler.onEvent(event, 0L, false);
                total++;
                if ((total & (BATCH_END_EVERY - 1)) == 0) {
                    handler.onBatchEnd(); // 批末：flushFills + 深度（本地 sink 只计数）
                }
                if (orderType == 1) {
                    // 一单一撤：限价单登记一次到期撤单，寿命指数分布
                    long lifetime =
                            (long) (-Math.log(1.0 - rng.nextDouble()) * ORDER_LIFETIME_FRAMES) + 1;
                    pending.push(total + lifetime, f.id);
                }
            }
        }
        handler.onBatchEnd();

        long elapsedSec = (System.currentTimeMillis() - phaseStart) / 1000;
        long phaseOrders = total - lastSent;
        System.out.printf("=== 纯撮合基准完成 ===%n");
        System.out.printf(
                "  模式:       %s%n", withMetrics ? "带 MatchMetrics（接近真实）" : "纯撮合（metrics=null）");
        System.out.printf("  有效时长:   %d 秒%n", elapsedSec);
        System.out.printf(
                "  订单(含撤单): %,d  →  %,d TPS%n", phaseOrders, phaseOrders / Math.max(1, elapsedSec));
        System.out.printf("  撤单:      %,d（%.1f%%）%n", cancelCnt, 100.0 * cancelCnt / phaseOrders);
        System.out.printf("  成交:      %,d 笔%n", fillSink.fills);
        System.out.printf("  拒绝:      %,d%n", fillSink.rejects);
        System.out.printf(
                "  在簿:      %,d / %,d（slots %,d）%n",
                book.openOrderCount(), book.getMaxOpenOrders(), book.slotsUsed());
    }

    /**
     * 预生成订单流回放：一次性生成 1000 万订单的紧凑数组（不计时），循环转圈喂
     * onEvent——消除 Random/log 生成开销（占运行时生成模式 ~48% 采样），探测纯撮合上限。
     * 撤单目标指向过去限价单（转圈后可能已撤/成交，未命中真实）。
     */
    private static void runPregen(
            OrderEventHandler handler, OrderBook book, CountingFillSink fillSink) {
        Random rng = new Random(42);
        long[] prices = new long[PREGEN_N];
        long[] qtys = new long[PREGEN_N];
        long[] triggers = new long[PREGEN_N];
        byte[] types = new byte[PREGEN_N];
        byte[] sides = new byte[PREGEN_N];
        // 撤单帧：cancelBack[i] >= 0 表示位置 i 是撤单帧，目标 = 池内 i - cancelBack[i]
        // （对齐 Rust 池子：重放时目标 = 本轮同位置订单的绝对单号，一单一撤命中）
        int[] cancelBack = new int[PREGEN_N];
        java.util.Arrays.fill(cancelBack, -1);

        double mid = BASE_PRICE;
        long base = System.nanoTime();
        ExpiryHeap pending = new ExpiryHeap(); // 到期帧 → 目标池内索引
        for (int i = 0; i < PREGEN_N; i++) {
            mid += rng.nextGaussian() * MID_VOL + (BASE_PRICE - mid) * MEAN_REVERSION;
            // 到期撤单优先（一单一撤）
            if (!pending.isEmpty() && pending.peekExpiry() <= i) {
                int target = (int) pending.pollId();
                cancelBack[i] = i - target;
                continue;
            }
            int side = rng.nextBoolean() ? 0 : 1;
            sides[i] = (byte) side;
            double roll = rng.nextDouble();
            int type =
                    roll < MARKET_RATIO
                            ? 2
                            : roll < MARKET_RATIO + STOP_RATIO ? 3 + rng.nextInt(4) : 1;
            types[i] = (byte) type;
            boolean large = rng.nextDouble() < LARGE_RATIO;
            double qty = large ? 1.0 + rng.nextDouble() * 2.0 : 0.01 + rng.nextDouble() * 0.1;
            qtys[i] = Math.round(qty * Math.pow(10, SCALE));
            if (type != 2 && type != 4 && type != 6) {
                double d = -DECAY * Math.log(1.0 - rng.nextDouble());
                if (d > SPREAD) {
                    d = SPREAD;
                }
                double p = side == 0 ? mid - d : mid + d;
                prices[i] = Math.round(Math.round(p / TICK) * TICK * Math.pow(10, SCALE));
            }
            if (type >= 3) {
                double dist = (0.0005 + rng.nextDouble() * 0.002) * mid;
                triggers[i] =
                        Math.round(
                                (mid + (rng.nextBoolean() ? dist : -dist)) * Math.pow(10, SCALE));
            }
            if (type == 1) {
                // 一单一撤：指数寿命，截断到池尾并均匀分散（避免同帧堆积发不完）
                long maxD = PREGEN_N - 1L - i;
                long lifetime =
                        (long) (-Math.log(1.0 - rng.nextDouble()) * ORDER_LIFETIME_FRAMES) + 1;
                long d = lifetime >= maxD ? 1 + rng.nextInt((int) Math.max(1, maxD)) : lifetime;
                pending.push(i + d, i);
            }
        }
        System.out.printf("预生成订单流: %,d 条（回放，不计生成开销）%n", PREGEN_N);

        OrderEvent event = new OrderEvent();
        event.setSbeLength(1);
        event.setSnapshotRequest(false);
        OrderInFields f = event.getFields();
        long start = System.currentTimeMillis();
        long reportTs = start;
        long phaseStart = start;
        long total = 0, lastSent = 0, cancels = 0;
        int k = 0;
        while (true) {
            long now = System.currentTimeMillis();
            if (now - start >= WARMUP_SECS * 1000L && phaseStart == start) {
                phaseStart = now;
                lastSent = total;
            }
            if (now - start >= (WARMUP_SECS + RUN_SECS) * 1000L) {
                break;
            }
            if (now - reportTs >= 10_000) {
                long elapsed = (now - phaseStart) / 1000;
                System.out.printf(
                        "[%ds] tps=%,.0f fills=%,d cancels=%,d%n",
                        (now - start) / 1000,
                        elapsed > 0 ? (double) (total - lastSent) / elapsed : 0,
                        fillSink.fills,
                        cancels);
                reportTs = now;
            }
            int i = k % PREGEN_N;
            long frame = k;
            k++;
            if (cancelBack[i] >= 0) { // 撤单帧：目标 = 本轮 i-cancelBack[i] 位置订单的绝对单号
                f.id = base + frame + 1 - cancelBack[i];
                f.userId = 1L;
                f.status = 2;
                f.type = 1;
                f.side = 0;
                f.timeInForce = 0;
                f.priceMantissa = 0;
                f.priceExponent = -SCALE;
                f.origQtyMantissa = 0;
                f.origQtyExponent = -SCALE;
                f.executedQtyMantissa = 0L;
                f.executedQtyExponent = -SCALE;
                f.mergeLowPriceMantissa = 0;
                f.mergeLowPriceExponent = -SCALE;
                f.visibleQtyMantissa = 0L;
                f.visibleQtyExponent = -SCALE;
                f.createTimeMillis = now;
                handler.onEvent(event, 0L, false);
                cancels++;
                total++;
            } else {
                f.id = base + frame + 1; // 每轮新 id（对齐 Rust 池子：id = base + cursor + 1）
                f.userId = 1L + (i & 0xFFFF);
                f.status = 0;
                f.type = types[i];
                f.side = sides[i];
                f.timeInForce = 0;
                f.priceMantissa = prices[i];
                f.priceExponent = -SCALE;
                f.origQtyMantissa = qtys[i];
                f.origQtyExponent = -SCALE;
                f.executedQtyMantissa = 0L;
                f.executedQtyExponent = -SCALE;
                f.mergeLowPriceMantissa = triggers[i];
                f.mergeLowPriceExponent = -SCALE;
                f.visibleQtyMantissa = 0L;
                f.visibleQtyExponent = -SCALE;
                f.createTimeMillis = now;
                handler.onEvent(event, 0L, false);
                total++;
            }
            if ((total & (BATCH_END_EVERY - 1)) == 0) {
                handler.onBatchEnd();
            }
        }
        handler.onBatchEnd();

        long elapsedSec = (System.currentTimeMillis() - phaseStart) / 1000;
        long phaseOrders = total - lastSent;
        System.out.printf("=== 纯撮合上限（pregen）===%n");
        System.out.printf("  有效时长:   %d 秒%n", elapsedSec);
        System.out.printf(
                "  订单(含撤单): %,d  →  %,d TPS%n", phaseOrders, phaseOrders / Math.max(1, elapsedSec));
        System.out.printf("  撤单:      %,d（%.1f%%）%n", cancels, 100.0 * cancels / phaseOrders);
        System.out.printf("  成交:      %,d 笔%n", fillSink.fills);
        System.out.printf("  在簿:      %,d / %,d%n", book.openOrderCount(), book.getMaxOpenOrders());
    }

    /** 本地计数成交 sink：零编码零发送（绕过 AeronFillSink/egress）。 */
    private static final class CountingFillSink implements FillSink {
        long fills = 0;
        long rejects = 0;

        @Override
        public void submitFills(List<MatchFill> list) {
            fills += list.size();
        }

        @Override
        public OutboundDepth acquireDepth(String symbol, int coinScale, int baseCoinScale) {
            return new OutboundDepth();
        }

        @Override
        public void submitDepth(OutboundDepth depth) {}
    }
}
