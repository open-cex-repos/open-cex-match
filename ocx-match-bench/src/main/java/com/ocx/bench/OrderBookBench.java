package com.ocx.bench;

import com.ocx.book.Long2IntMap;
import com.ocx.book.MatchFill;
import com.ocx.book.OrderBook;
import com.ocx.book.OrderRequest;

import org.agrona.collections.Long2LongHashMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 撮合簿热路径微基准——对应压测 JFR 栈顶（{@code matchTaker} / {@code Long2IntMap.get}
 * / {@code addOrderInternal} / LevelSlots 链操作）。无 WAL、无出站，纯簿内单写者语义。
 *
 * <p>稳态设计：预灌 {@code PRELOAD} 笔买单（mid-100）与卖单（mid+100），各基准以
 * 「撤单+同价回补」「taker 自愈回补」闭环，簿容量恒定不漂移。</p>
 *
 * <p>撤单定位哈希 A/B（orderId→slot 查找税归因；2026-08-29 曾尝试将槽位内嵌进
 * orderId 以省去哈希查找，2026-09-01 因回绕冲突税 e^(-CAP/N)（N=5M 时约 19% 拒单）
 * 及发号方无法自由分配而回退，当前 {@code orderIndex} 即自研 {@link Long2IntMap}，
 * 为撤单 / addOrder 主路径热点，占压测 JFR 自采样 36.2%）：独立 map 基准镜像
 * 「get→remove→put」回补环，同容量（2^20）同 live（2×PRELOAD），对照自研
 * {@link Long2IntMap} 与 Agrona {@link Long2LongHashMap}（2.5.0 无 long→int 变体，
 * 取最接近的 long→long，值 8B 略吃亏）；各实现再收紧负载到 0.5（容量 2^19）验证
 * 负载因子影响。</p>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(
        value = 1,
        jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
public class OrderBookBench {

    /** 70000.00 @ coinScale=8 定点。 */
    private static final long PRICE_BASE = 70_000L * 100_000_000L;

    /** 半幅 100.00 @ coinScale=8 定点。 */
    private static final long HALF_SPREAD = 100L * 100_000_000L;

    /** maker 单量 0.001 BTC @ baseCoinScale=8 定点。 */
    private static final long MAKER_QTY = 100_000L;

    /** taker 单量 0.01 BTC → 每笔约吃穿 10 个 maker。 */
    private static final long TAKER_QTY = 1_000_000L;

    /** 每侧预灌挂单数（2 的幂，ring 掩码取模）。 */
    private static final int PRELOAD = 1 << 17;

    private static final int MASK = PRELOAD - 1;

    /** 独立 map A/B：容量与簿内 orderIndex 一致，live 覆盖 buy+sell 两侧。 */
    private static final int MAP_CAPACITY = 1 << 20;

    private static final int MAP_LIVE = PRELOAD * 2;

    private OrderBook book;
    private Long2IntMap customMap;
    private Long2IntMap customMapLf05;
    private Long2LongHashMap agronaMap;
    private Long2LongHashMap agronaMapLf05;
    private final OrderRequest req = new OrderRequest();
    private final List<MatchFill> fills = new ArrayList<>(64);

    /** 在簿买单 id ring（撤单/回补循环）。 */
    private final long[] buyIds = new long[PRELOAD];

    private int cursor;
    private long nextId;

    @Setup(Level.Trial)
    public void setup() {
        book = new OrderBook("BTCUSDT", 8, 8, 1 << 20, 1 << 16, 1_000_000L);
        long buyPrice = PRICE_BASE - HALF_SPREAD;
        long sellPrice = PRICE_BASE + HALF_SPREAD;
        for (int i = 0; i < PRELOAD; i++) {
            long buyId = nextId++;
            buyIds[i] = buyId;
            addLimit(buyPrice, buyId, 0);
            addLimit(sellPrice, nextId++, 1);
        }
        // A/B 独立 map：自研 0.65 语义换算 expected 得到目标容量；
        // LF05 组容量减半 → 负载 0.5（262144/2^19 恰在 Agrona 扩容阈值内）。
        customMap = new Long2IntMap((int) (MAP_CAPACITY * 0.65f));
        customMapLf05 = new Long2IntMap((int) ((MAP_CAPACITY >>> 1) * 0.65f));
        agronaMap = new Long2LongHashMap(MAP_CAPACITY, 0.6f, -1L);
        agronaMapLf05 = new Long2LongHashMap(MAP_CAPACITY >>> 1, 0.5f, -1L);
        for (int i = 0; i < MAP_LIVE; i++) {
            customMap.put(i, i + 1);
            customMapLf05.put(i, i + 1);
            agronaMap.put(i, i + 1);
            agronaMapLf05.put(i, i + 1);
        }
    }

    /** 限价入簿（GTC；买单恒在卖价以下，不跨价）。 */
    private int addLimit(long price, long id, int direction) {
        req.reset();
        req.orderId = id;
        req.userId = direction + 1;
        req.orderType = OrderRequest.ORDER_TYPE_LIMIT;
        req.timeInForce = OrderRequest.TIF_GTC;
        req.direction = direction;
        req.price = price;
        req.origQty = MAKER_QTY;
        return book.addOrder(req, 0L);
    }

    /** 撤单 + 同价回补（70% 撤单流主力：Long2IntMap.get 定位 + LevelSlots 摘链）。 */
    @Benchmark
    public void cancelReadd(Blackhole bh) {
        long id = buyIds[cursor];
        bh.consume(book.cancel(id));
        bh.consume(addLimit(PRICE_BASE - HALF_SPREAD, id, 0));
        cursor = (cursor + 1) & MASK;
    }

    /** 市价 taker 吃单（matchTaker 主循环，多 maker 成交），taker 后自愈回补卖单。 */
    @Benchmark
    public void marketTaker(Blackhole bh) {
        req.reset();
        req.orderId = nextId++;
        req.userId = 3;
        req.orderType = OrderRequest.ORDER_TYPE_MARKET;
        req.timeInForce = OrderRequest.TIF_IOC;
        req.direction = 0;
        req.origQty = TAKER_QTY;
        long remaining = book.matchTaker(req, 0L, false, fills);
        bh.consume(remaining);
        book.recycleFills(fills);
        // 每笔吃穿约 10 个 maker 卖单，按同价回补 10 个，保持卖侧容量恒定。
        for (int i = 0; i < 10; i++) {
            bh.consume(addLimit(PRICE_BASE + HALF_SPREAD, nextId++, 1));
        }
    }

    /** 70% 撤单 + 30% 新增限价（对齐 stress 混合流）：7×撤单回补 + 3×新增并撤最老。 */
    @Benchmark
    @OperationsPerInvocation(10)
    public void stressMix70(Blackhole bh) {
        for (int i = 0; i < 10; i++) {
            if (i < 7) {
                long id = buyIds[cursor];
                bh.consume(book.cancel(id));
                bh.consume(addLimit(PRICE_BASE - HALF_SPREAD, id, 0));
                cursor = (cursor + 1) & MASK;
            } else {
                long id = nextId++;
                bh.consume(addLimit(PRICE_BASE - HALF_SPREAD, id, 0));
                long victim = buyIds[cursor];
                bh.consume(book.cancel(victim));
                buyIds[cursor] = id;
                cursor = (cursor + 1) & MASK;
            }
        }
    }

    /** A/B-1: 自研 Long2IntMap 独立撤单定位（load 0.25，与簿内 orderIndex 一致）。 */
    @Benchmark
    public void cancelReaddCustomMap(Blackhole bh) {
        long id = buyIds[cursor];
        bh.consume(customMap.get(id));
        bh.consume(customMap.remove(id));
        bh.consume(customMap.put(id, (int) id + 1));
        cursor = (cursor + 1) & MASK;
    }

    /** A/B-2: Agrona Long2LongHashMap 同容量同 live（load 0.25）。 */
    @Benchmark
    public void cancelReaddAgronaMap(Blackhole bh) {
        long id = buyIds[cursor];
        bh.consume(agronaMap.get(id));
        bh.consume(agronaMap.remove(id));
        bh.consume(agronaMap.put(id, id + 1));
        cursor = (cursor + 1) & MASK;
    }

    /** A/B-3: 自研 map 收紧到 load 0.5（容量 2^19）。 */
    @Benchmark
    public void cancelReaddCustomMapLf05(Blackhole bh) {
        long id = buyIds[cursor];
        bh.consume(customMapLf05.get(id));
        bh.consume(customMapLf05.remove(id));
        bh.consume(customMapLf05.put(id, (int) id + 1));
        cursor = (cursor + 1) & MASK;
    }

    /** A/B-4: Agrona map 收紧到 load 0.5（容量 2^19）。 */
    @Benchmark
    public void cancelReaddAgronaMapLf05(Blackhole bh) {
        long id = buyIds[cursor];
        bh.consume(agronaMapLf05.get(id));
        bh.consume(agronaMapLf05.remove(id));
        bh.consume(agronaMapLf05.put(id, id + 1));
        cursor = (cursor + 1) & MASK;
    }
}
