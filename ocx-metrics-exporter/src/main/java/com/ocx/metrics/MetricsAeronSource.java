package com.ocx.metrics;

import com.ocx.aeron.AeronWire;
import com.ocx.aeron.OcxAeron;
import com.ocx.aeron.StreamIds;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * metrics 流 Aeron 订阅端（2026-08-30，Chronicle metrics 队列删除）：订阅各 symbol 的
 * {@code metrics(symbol)} 流——core 的 METRICS/EVENT 行 + egress 的 OUTBOUND_STATS 行，
 * 维护每个 symbol 的最新状态并渲染 Prometheus 文本。
 *
 * <p>sidecar 常驻、引擎按需拉起：订阅先于引擎发布注册即可；Aeron 对迟到订阅者不重放
 * 历史，心跳/快照由下一拍补齐（Prometheus 从 sidecar 启动后开始累计，语义等价旧队列
 * 「从队头追平」，缺中间帧无影响）。</p>
 */
@Component
public class MetricsAeronSource {

    private static final Logger log = LoggerFactory.getLogger(MetricsAeronSource.class);

    private final String aeronDir;
    private final List<String> symbols;
    private final String profile;
    private final long staleAfterSeconds;
    private final Map<String, SymbolState> states = new ConcurrentHashMap<>();
    private final List<Subscription> subscriptions = new ArrayList<>();
    private final FragmentHandler fragmentHandler = this::onFragment;
    private Aeron aeron;
    private ScheduledExecutorService executor;

    public MetricsAeronSource(
            @Value("${metrics.aeron.dir:/tmp/ocx-aeron}") String aeronDir,
            @Value("${metrics.symbols:BTCUSDT}") String symbolsCsv,
            @Value("${metrics.profile:spot}") String profile,
            @Value("${metrics.stale-after-seconds:10}") long staleAfterSeconds) {
        this.aeronDir = aeronDir;
        this.profile = profile;
        this.staleAfterSeconds = staleAfterSeconds;
        this.symbols = new ArrayList<>();
        for (String s : symbolsCsv.split(",")) {
            s = s.trim();
            if (!s.isEmpty()) {
                symbols.add(s);
                states.put(s, new SymbolState(s, profile));
            }
        }
    }

    @PostConstruct
    public void start() {
        executor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "metrics-aeron");
                            t.setDaemon(true);
                            return t;
                        });
        // sidecar 常驻、媒体驱动按需拉起：未就绪时每秒重试建立连接/订阅
        executor.scheduleWithFixedDelay(this::tick, 1, 1, TimeUnit.SECONDS);
    }

    /** 每秒 tick：未连接则重试建连，已连接则轮询解码。 */
    private void tick() {
        if (aeron == null) {
            ensureConnected();
            return;
        }
        poll();
    }

    /** 连接共享媒体驱动并注册各 symbol 的 metrics 订阅（驱动未起时静默等待，下次重试）。 */
    void ensureConnected() {
        try {
            Aeron a = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
            List<Subscription> subs = new ArrayList<>(symbols.size());
            for (String symbol : symbols) {
                subs.add(a.addSubscription(OcxAeron.CHANNEL, StreamIds.metrics(symbol)));
            }
            subscriptions.addAll(subs);
            aeron = a;
            log.info(
                    "metrics-aeron 订阅就绪: dir={} symbols={} profile={}", aeronDir, symbols, profile);
        } catch (Throwable t) {
            // 驱动未就绪/启动瞬态（含 JVM 模块访问等 Error）：重试不能被打断
            log.debug("媒体驱动未就绪，稍后重试: {}", String.valueOf(t.getMessage()));
        }
    }

    /** 轮询各 metrics 流并解码（1s 周期；METRICS/EVENT/OUTBOUND_STATS 行均自带 symbol）。 */
    void poll() {
        for (Subscription sub : subscriptions) {
            sub.poll(fragmentHandler, 256);
        }
    }

    private void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        if (length < 1) {
            return;
        }
        switch (buffer.getByte(offset)) {
            case AeronWire.TYPE_METRICS -> applyMetrics(AeronWire.decodeMetrics(buffer, offset));
            case AeronWire.TYPE_EVENT -> {
                AeronWire.EventFields fields = new AeronWire.EventFields();
                AeronWire.decodeEvent(buffer, offset, fields);
                applyEvent(fields);
            }
            case AeronWire.TYPE_OUTBOUND_STATS ->
                    applyOutboundStats(AeronWire.decodeOutboundStats(buffer, offset));
            default -> {}
        }
    }

    private void applyMetrics(MetricsSnapshot snapshot) {
        SymbolState s = states.get(snapshot.symbol());
        if (s == null) {
            return;
        }
        s.lastReadMs = System.currentTimeMillis();
        s.symbol = snapshot.symbol();
        s.ts = snapshot.exportTs();
        s.lastOrderTs = snapshot.lastOrderTs();
        s.overloaded = snapshot.overloaded();
        s.consumerLag = snapshot.consumerLag();
        s.inboundBacklog = snapshot.inboundBacklog();
        s.outboundBackpressure = snapshot.outboundBackpressure();
        s.throughput = snapshot.throughput();
        s.ordersLimit = snapshot.ordersLimit();
        s.ordersMarket = snapshot.ordersMarket();
        s.tradesTotal = snapshot.tradesTotal();
        s.rejectedTotal = snapshot.rejectedTotal();
        s.orderBookCapRejected = snapshot.orderBookCapRejected();
        s.openOrderCount = snapshot.openOrderCount();
        s.slotsUsed = snapshot.slotsUsed();
        s.buyOrders = snapshot.buyOrders();
        s.sellOrders = snapshot.sellOrders();
        s.orderBookCap = snapshot.orderBookCap();
        s.depthBid = snapshot.depthBid();
        s.depthAsk = snapshot.depthAsk();
        s.p50 = snapshot.durationP50();
        s.p95 = snapshot.durationP95();
        s.p99 = snapshot.durationP99();
        s.p999 = snapshot.durationP999();
        s.max = snapshot.durationMax();
        s.batchSizeMax = snapshot.batchSizeMax();
        s.jvm = snapshot.jvm();
        s.hasJvm = s.jvm != null;
        s.hasSnapshot = true;
    }

    private void applyEvent(AeronWire.EventFields fields) {
        SymbolState s = states.get(fields.symbol);
        if (s == null) {
            return;
        }
        s.lastReadMs = System.currentTimeMillis();
        if (fields.event == null || fields.event.isEmpty()) {
            return;
        }
        s.events.compute(
                fields.event,
                (k, cur) -> {
                    EventState es = cur == null ? new EventState() : cur;
                    es.count++;
                    es.ts = System.currentTimeMillis();
                    es.detail = fields.detail == null ? "" : fields.detail;
                    return es;
                });
    }

    private void applyOutboundStats(AeronWire.OutboundStatsRow row) {
        SymbolState s = states.get(row.symbol());
        if (s == null) {
            return;
        }
        s.lastReadMs = System.currentTimeMillis();
        s.egressPublishedBytes = row.publishedBytes();
        s.egressBackpressure = row.backpressure();
    }

    /** 渲染 Prometheus 文本格式。 */
    public String render() {
        StringBuilder sb = new StringBuilder(1024);
        long now = System.currentTimeMillis();
        long staleMs = staleAfterSeconds * 1000;

        sb.append("# HELP match_engine_up 撮合引擎是否存活（依据 metrics 流新鲜度）。\n");
        sb.append("# TYPE match_engine_up gauge\n");
        for (SymbolState s : states.values()) {
            int up = (now - s.lastReadMs <= staleMs) ? 1 : 0;
            sb.append("match_engine_up{symbol=\"")
                    .append(s.symbol)
                    .append("\",profile=\"")
                    .append(s.profile)
                    .append("\"} ")
                    .append(up)
                    .append("\n");
        }

        for (SymbolState s : states.values()) {
            String l = "{symbol=\"" + s.symbol + "\",profile=\"" + s.profile + "\"}";
            sb.append("match_engine_heartbeat_timestamp_seconds")
                    .append(l)
                    .append(' ')
                    .append(s.ts / 1000.0)
                    .append('\n');
            sb.append("match_engine_last_order_timestamp_seconds")
                    .append(l)
                    .append(' ')
                    .append(s.lastOrderTs / 1000.0)
                    .append('\n');
            sb.append("match_engine_overloaded")
                    .append(l)
                    .append(' ')
                    .append(s.overloaded ? 1 : 0)
                    .append('\n');
            sb.append("match_consumer_lag")
                    .append(l)
                    .append(' ')
                    .append(s.consumerLag)
                    .append('\n');
            sb.append("match_inbound_backlog")
                    .append(l)
                    .append(' ')
                    .append(s.inboundBacklog)
                    .append('\n');
            if (s.hasSnapshot) {
                sb.append("match_outbound_backpressure_total")
                        .append(l)
                        .append(' ')
                        .append(s.outboundBackpressure)
                        .append('\n');
                sb.append("match_outbound_published_bytes_total")
                        .append(l)
                        .append(' ')
                        .append(s.egressPublishedBytes)
                        .append('\n');
                sb.append("match_egress_backpressure_total")
                        .append(l)
                        .append(' ')
                        .append(s.egressBackpressure)
                        .append('\n');
                sb.append("match_throughput_current")
                        .append(l)
                        .append(' ')
                        .append(s.throughput)
                        .append('\n');
                sb.append("match_orders_total")
                        .append("{symbol=\"")
                        .append(s.symbol)
                        .append("\",profile=\"")
                        .append(s.profile)
                        .append("\",type=\"limit\"} ")
                        .append(s.ordersLimit)
                        .append('\n');
                sb.append("match_orders_total")
                        .append("{symbol=\"")
                        .append(s.symbol)
                        .append("\",profile=\"")
                        .append(s.profile)
                        .append("\",type=\"market\"} ")
                        .append(s.ordersMarket)
                        .append('\n');
                sb.append("match_trades_total")
                        .append(l)
                        .append(' ')
                        .append(s.tradesTotal)
                        .append('\n');
                sb.append("match_orders_rejected_total")
                        .append(l)
                        .append(' ')
                        .append(s.rejectedTotal)
                        .append('\n');
                // 明细独立指标名：避免与无标签总数同名，Prometheus sum() 叠加双计
                sb.append("match_orders_rejected_by_reason_total")
                        .append("{symbol=\"")
                        .append(s.symbol)
                        .append("\",profile=\"")
                        .append(s.profile)
                        .append("\",reason=\"order_book_cap\"} ")
                        .append(s.orderBookCapRejected)
                        .append('\n');
                sb.append("match_order_book_size")
                        .append(l)
                        .append(' ')
                        .append(s.openOrderCount)
                        .append('\n');
                sb.append("match_order_book_slots_used")
                        .append(l)
                        .append(' ')
                        .append(s.slotsUsed)
                        .append('\n');
                sb.append("match_order_book_buy_orders")
                        .append(l)
                        .append(' ')
                        .append(s.buyOrders)
                        .append('\n');
                sb.append("match_order_book_sell_orders")
                        .append(l)
                        .append(' ')
                        .append(s.sellOrders)
                        .append('\n');
                sb.append("match_order_book_cap")
                        .append(l)
                        .append(' ')
                        .append(s.orderBookCap)
                        .append('\n');
                sb.append("match_order_book_depth")
                        .append("{symbol=\"")
                        .append(s.symbol)
                        .append("\",profile=\"")
                        .append(s.profile)
                        .append("\",side=\"bid\"} ")
                        .append(s.depthBid)
                        .append('\n');
                sb.append("match_order_book_depth")
                        .append("{symbol=\"")
                        .append(s.symbol)
                        .append("\",profile=\"")
                        .append(s.profile)
                        .append("\",side=\"ask\"} ")
                        .append(s.depthAsk)
                        .append('\n');
                appendPercentile(sb, s.symbol, s.profile, "p50", s.p50);
                appendPercentile(sb, s.symbol, s.profile, "p95", s.p95);
                appendPercentile(sb, s.symbol, s.profile, "p99", s.p99);
                appendPercentile(sb, s.symbol, s.profile, "p999", s.p999);
                appendPercentile(sb, s.symbol, s.profile, "max", s.max);
                sb.append("match_batch_size_max")
                        .append(l)
                        .append(' ')
                        .append(s.batchSizeMax)
                        .append('\n');
            }
            appendEvents(sb, s);
        }

        appendJvmMetrics(sb);
        return sb.toString();
    }

    /** JVM 指标来自 core 进程（METRICS 行内嵌），标签用 profile+symbol 便于与业务指标同面板联动。 */
    private void appendJvmMetrics(StringBuilder sb) {
        sb.append("# HELP jvm_memory_used_bytes JVM 内存已使用字节数。\n");
        sb.append("# TYPE jvm_memory_used_bytes gauge\n");
        sb.append("# HELP jvm_memory_committed_bytes JVM 内存已提交字节数。\n");
        sb.append("# TYPE jvm_memory_committed_bytes gauge\n");
        sb.append("# HELP jvm_memory_max_bytes JVM 内存最大可用字节数。\n");
        sb.append("# TYPE jvm_memory_max_bytes gauge\n");
        sb.append("# HELP jvm_threads_live_threads JVM 当前活动线程数。\n");
        sb.append("# TYPE jvm_threads_live_threads gauge\n");
        sb.append("# HELP jvm_threads_daemon_threads JVM 当前 daemon 线程数。\n");
        sb.append("# TYPE jvm_threads_daemon_threads gauge\n");
        sb.append("# HELP jvm_threads_peak_threads JVM 启动以来峰值线程数。\n");
        sb.append("# TYPE jvm_threads_peak_threads gauge\n");
        sb.append("# HELP jvm_gc_pause_seconds_sum JVM GC 累计停顿秒数。\n");
        sb.append("# TYPE jvm_gc_pause_seconds_sum counter\n");
        sb.append("# HELP jvm_gc_pause_seconds_count JVM GC 累计次数。\n");
        sb.append("# TYPE jvm_gc_pause_seconds_count counter\n");
        sb.append("# HELP jvm_gc_pause_last_seconds JVM 最近一次 GC 停顿秒数，-1 表示暂无 GC 信息。\n");
        sb.append("# TYPE jvm_gc_pause_last_seconds gauge\n");
        sb.append("# HELP jvm_uptime_seconds JVM 启动以来运行秒数。\n");
        sb.append("# TYPE jvm_uptime_seconds gauge\n");

        for (SymbolState s : states.values()) {
            if (!s.hasJvm || s.jvm == null) {
                continue;
            }
            String base = "symbol=\"" + s.symbol + "\",profile=\"" + s.profile + "\"";
            String l = "{" + base + "}";
            String heapLabel = "{" + base + ",area=\"heap\"}";
            String nonHeapLabel = "{" + base + ",area=\"nonheap\"}";
            sb.append("jvm_memory_used_bytes")
                    .append(heapLabel)
                    .append(' ')
                    .append(s.jvm.heapUsed())
                    .append('\n');
            sb.append("jvm_memory_used_bytes")
                    .append(nonHeapLabel)
                    .append(' ')
                    .append(s.jvm.nonHeapUsed())
                    .append('\n');
            sb.append("jvm_memory_committed_bytes")
                    .append(heapLabel)
                    .append(' ')
                    .append(s.jvm.heapCommitted())
                    .append('\n');
            sb.append("jvm_memory_max_bytes")
                    .append(heapLabel)
                    .append(' ')
                    .append(s.jvm.heapMax())
                    .append('\n');
            sb.append("jvm_threads_live_threads")
                    .append(l)
                    .append(' ')
                    .append(s.jvm.threadsLive())
                    .append('\n');
            sb.append("jvm_threads_daemon_threads")
                    .append(l)
                    .append(' ')
                    .append(s.jvm.threadsDaemon())
                    .append('\n');
            sb.append("jvm_threads_peak_threads")
                    .append(l)
                    .append(' ')
                    .append(s.jvm.threadsPeak())
                    .append('\n');
            sb.append("jvm_gc_pause_seconds_sum")
                    .append(l)
                    .append(' ')
                    .append(s.jvm.gcCollectionTimeMillis() / 1000.0)
                    .append('\n');
            sb.append("jvm_gc_pause_seconds_count")
                    .append(l)
                    .append(' ')
                    .append(s.jvm.gcCollections())
                    .append('\n');
            sb.append("jvm_gc_pause_last_seconds")
                    .append(l)
                    .append(' ')
                    .append(s.jvm.gcLastPauseMillis() / 1000.0)
                    .append('\n');
            sb.append("jvm_uptime_seconds")
                    .append(l)
                    .append(' ')
                    .append(s.jvm.uptimeMillis() / 1000.0)
                    .append('\n');
        }
    }

    private void appendPercentile(
            StringBuilder sb, String symbol, String profile, String percentile, double value) {
        sb.append("match_duration_percentile{symbol=\"")
                .append(symbol)
                .append("\",profile=\"")
                .append(profile)
                .append("\",type=\"limit\",percentile=\"")
                .append(percentile)
                .append("\"} ")
                .append(value)
                .append('\n');
    }

    /** 关键事件：累计计数 + 最近一次时间戳与明细（引擎零业务日志后的排障信号）。 */
    private void appendEvents(StringBuilder sb, SymbolState s) {
        if (s.events.isEmpty()) {
            return;
        }
        sb.append("# HELP match_engine_events_total 撮合引擎关键事件累计计数（sidecar 启动后累计）。\n");
        sb.append("# TYPE match_engine_events_total counter\n");
        sb.append("# HELP match_engine_last_event_timestamp_seconds 撮合引擎最近一次关键事件时间戳。\n");
        sb.append("# TYPE match_engine_last_event_timestamp_seconds gauge\n");
        sb.append("# HELP match_engine_last_event_detail 撮合引擎最近一次关键事件明细（1=存在）。\n");
        sb.append("# TYPE match_engine_last_event_detail gauge\n");
        for (Map.Entry<String, EventState> e : s.events.entrySet()) {
            String event = e.getKey();
            EventState es = e.getValue();
            String base =
                    "symbol=\""
                            + s.symbol
                            + "\",profile=\""
                            + s.profile
                            + "\",event=\""
                            + event
                            + "\"";
            sb.append("match_engine_events_total{")
                    .append(base)
                    .append("} ")
                    .append(es.count)
                    .append('\n');
            sb.append("match_engine_last_event_timestamp_seconds{")
                    .append(base)
                    .append("} ")
                    .append(es.ts / 1000.0)
                    .append('\n');
            sb.append("match_engine_last_event_detail{")
                    .append(base)
                    .append(",detail=\"")
                    .append(sanitize(es.detail))
                    .append("\"} 1\n");
        }
    }

    /** Prometheus label 值消毒：截断 + 去除引号/换行/反斜杠。 */
    private static String sanitize(String detail) {
        String s = detail == null ? "" : detail;
        if (s.length() > 200) {
            s = s.substring(0, 200) + "...";
        }
        return s.replace('\\', ' ').replace('"', ' ').replace('\n', ' ').replace('\r', ' ');
    }

    @PreDestroy
    public void stop() {
        if (executor != null) {
            executor.shutdownNow();
        }
        if (aeron != null) {
            aeron.close();
        }
    }

    /** 每个 symbol 的最新状态。 */
    static final class SymbolState {
        final String key;
        final String profile;
        volatile String symbol;
        volatile long lastReadMs;
        volatile long ts;
        volatile long lastOrderTs;
        volatile boolean overloaded;
        volatile long consumerLag;
        volatile long inboundBacklog;
        volatile long outboundBackpressure;
        volatile long egressPublishedBytes;
        volatile long egressBackpressure;
        volatile double throughput;
        volatile long ordersLimit;
        volatile long ordersMarket;
        volatile long tradesTotal;
        volatile long rejectedTotal;
        volatile long orderBookCapRejected;
        volatile long openOrderCount;
        volatile long slotsUsed;
        volatile long buyOrders;
        volatile long sellOrders;
        volatile long orderBookCap;
        volatile int depthBid;
        volatile int depthAsk;
        volatile double p50;
        volatile double p95;
        volatile double p99;
        volatile double p999;
        volatile double max;
        volatile long batchSizeMax;
        volatile JvmMetrics jvm;
        volatile boolean hasSnapshot;
        volatile boolean hasJvm;
        final ConcurrentHashMap<String, EventState> events = new ConcurrentHashMap<>();

        SymbolState(String symbol, String profile) {
            this.key = symbol;
            this.symbol = symbol;
            this.profile = profile;
        }
    }

    static final class EventState {
        long count;
        long ts;
        String detail;
    }
}
