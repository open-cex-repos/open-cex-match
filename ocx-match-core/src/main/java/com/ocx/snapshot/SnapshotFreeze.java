package com.ocx.snapshot;

import java.util.Map;

/**
 * 快照冻结缓冲（2026-08-30，快照迁往 Aeron Archive recording）：撮合线程批末单遍
 * 把在簿订单/条件单拷成紧凑 primitive 列（SoA），快照线程读取编码后发布为 snapshot
 * recording。双缓冲 ping-pong 复用：撮合线程零分配、快照线程读取期间互不覆盖。
 *
 * <p>容量策略：初始 {@link #FLOOR}，快照线程在两次快照之间按上一轮在簿数预扩
 * （80% 阈值），峰值内存跟随实际在簿数而非 maxOpenOrders；扩容分配发生在非热路径。</p>
 */
public final class SnapshotFreeze {

    /** 初始容量（每订单约 48B，4096 订单 ≈ 200KB；首轮后按实际在簿数动态扩）。 */
    static final int FLOOR = 4096;

    // ── 在簿订单（SoA）──
    public long[] orderIds;
    public long[] userIds;
    public long[] prices;
    public long[] origQtys;

    /** 已成交量（baseCoinScale 定点，2026-08-31：对账锚点 U2 累计基线）。 */
    public long[] execQtys;

    /** 累计成交额（coinScale 定点，2026-08-31：对账锚点 U1 turnover 基线）。 */
    public long[] cumQuoteQtys;

    public long[] times;
    public byte[] dirs;
    public byte[] tifs;
    public int orderCount;

    // ── 条件单（SoA）──
    public long[] triggerIds;
    public long[] triggerUserIds;
    public long[] triggerPrices;
    public long[] triggerLimitPrices;
    public long[] triggerQtys;
    public long[] triggerTimes;
    public byte[] triggerDirs;
    public byte[] triggerTypes;
    public long[] triggerTrailDeltas;
    public int triggerCount;

    /** 冻结时刻（撮合线程批末）。 */
    public long freezeTs;

    /** 冻结时刻各入站 image session 的消费位置（sessionId → 位置；快照线程按 recording sessionId 取锚点）。 */
    public Map<Integer, Long> imagePositions;

    /** 桥模式 journal 耐久锚点 {durableLivePos, durableJournalPos}（2026-09-01；null = 兼容模式）。 */
    public long[] journalAnchor;

    /** 冻结行 live 位置（桥模式精确锚点查询用；-1 = 未设置）。 */
    public long maxImagePos = -1L;

    public int coinScale;
    public int baseCoinScale;

    public SnapshotFreeze() {
        ensureOrders(FLOOR);
        ensureStops(FLOOR);
    }

    /** 幂等扩容（翻倍）；快照线程预扩 + 撮合线程兜底共用。 */
    public void ensure(int orders, int triggers) {
        ensureOrders(orders);
        ensureStops(triggers);
    }

    public void ensureOrders(int capacity) {
        if (orderIds != null && orderIds.length >= capacity) {
            return;
        }
        int cap = nextCapacity(capacity);
        orderIds = new long[cap];
        userIds = new long[cap];
        prices = new long[cap];
        origQtys = new long[cap];
        execQtys = new long[cap];
        cumQuoteQtys = new long[cap];
        times = new long[cap];
        dirs = new byte[cap];
        tifs = new byte[cap];
    }

    public void ensureStops(int capacity) {
        if (triggerIds != null && triggerIds.length >= capacity) {
            return;
        }
        int cap = nextCapacity(capacity);
        triggerIds = new long[cap];
        triggerUserIds = new long[cap];
        triggerPrices = new long[cap];
        triggerLimitPrices = new long[cap];
        triggerQtys = new long[cap];
        triggerTimes = new long[cap];
        triggerDirs = new byte[cap];
        triggerTypes = new byte[cap];
        triggerTrailDeltas = new long[cap];
    }

    private static int nextCapacity(int capacity) {
        int cap = Math.max(FLOOR, capacity);
        return Integer.highestOneBit(cap) == cap ? cap : Integer.highestOneBit(cap) << 1;
    }
}
