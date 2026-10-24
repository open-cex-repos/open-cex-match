package com.ocx.book;

import com.ocx.snapshot.SnapshotFreeze;
import com.ocx.snapshot.TriggerSnapshotFields;

import java.util.function.Consumer;

/**
 * 条件单槽位纵表（机械同理心：无对象图、零分配、索引即指针）。
 *
 * <p>条件单状态纵排为独立 long/int/byte 数组；按 (triggerType, direction) 分 8 张
 * {@link LevelSlots} 触发价索引（数组跳表 + 档内 FIFO 复用本结构 next/prev 列），
 * TRAILING_STOP 走单链（触发价动态更新，无排序）。orderId→slot 用 {@link Long2IntMap}。
 * 触发检查整档判定、槽位摘除保留（调用方读字段转单后 {@link #release(int)} 归还），
 * 热路径零分配。撮合线程单写者。</p>
 *
 * <p>数值约定：triggerPrice/price/trailDelta 为 coinScale 定点 long；origQty 为
 * baseCoinScale 定点 long（调用方按 symbol 精度归一化）。</p>
 */
public final class TriggerSlots {

    public static final int TYPE_STOP_LIMIT = 0;
    public static final int TYPE_STOP_MARKET = 1;
    public static final int TYPE_TAKE_PROFIT_LIMIT = 2;
    public static final int TYPE_TAKE_PROFIT_MARKET = 3;
    public static final int TYPE_TRAILING_STOP = 4;

    /** flags bit0：方向（0=BUY 1=SELL）。 */
    public static final int FLAG_DIRECTION = 0x01;

    /** flags bit1-3：条件单类型（0-4）。 */
    public static final int TYPE_SHIFT = 1;

    public static final int TYPE_MASK = 0x07;

    public final long[] orderIds;
    public final long[] userIds;

    /** 触发价（coinScale 定点）。 */
    public final long[] triggerPrices;

    /** 限价（coinScale 定点；MARKET 类条件单为 0）。 */
    public final long[] prices;

    /** 数量（baseCoinScale 定点）。 */
    public final long[] origQtys;

    public final long[] times;

    /** 移动止损偏移（coinScale 定点；非 trailing 为 0）。 */
    public final long[] trailDeltas;

    /** 同档 FIFO / trailing 链 / freelist 复用。 */
    public final int[] next;

    public final int[] prev;
    public final byte[] flags;

    private final LevelSlots[] tables;
    private int trailingHead = -1;
    private int trailingTail = -1;
    private final Long2IntMap orderIndex;
    private final int capacity;
    private int freeHead = -1;
    private int used = 0;

    public TriggerSlots(int capacity, int levelCapacity) {
        this.capacity = capacity;
        this.orderIds = new long[capacity];
        this.userIds = new long[capacity];
        this.triggerPrices = new long[capacity];
        this.prices = new long[capacity];
        this.origQtys = new long[capacity];
        this.times = new long[capacity];
        this.trailDeltas = new long[capacity];
        this.next = new int[capacity];
        this.prev = new int[capacity];
        this.flags = new byte[capacity];
        this.tables = new LevelSlots[8];
        for (int t = 0; t < 4; t++) {
            for (int d = 0; d < 2; d++) {
                tables[t * 2 + d] = new LevelSlots(isAscendingTable(t, d), levelCapacity, 12);
            }
        }
        this.orderIndex = new Long2IntMap(capacity);
        for (int i = 0; i < capacity; i++) {
            next[i] = i + 1 < capacity ? i + 1 : -1;
            prev[i] = -1;
        }
        freeHead = 0;
    }

    public int capacity() {
        return capacity;
    }

    public boolean isEmpty() {
        return used == 0;
    }

    public int orderCount() {
        return used;
    }

    /** 该 (类型, 方向) 组合的触发扫描是否按触发价升序（对齐旧 TriggerOrderManager 语义）。 */
    private static boolean isAscendingTable(int triggerType, int dir) {
        boolean isStop = triggerType == TYPE_STOP_LIMIT || triggerType == TYPE_STOP_MARKET;
        return (isStop && dir == 0) || (!isStop && dir == 1);
    }

    public boolean isBuy(int slot) {
        return (flags[slot] & FLAG_DIRECTION) == 0;
    }

    public int triggerType(int slot) {
        return (flags[slot] >>> TYPE_SHIFT) & TYPE_MASK;
    }

    private void setDirection(int slot, int direction) {
        if (direction == 0) {
            flags[slot] = (byte) (flags[slot] & ~FLAG_DIRECTION);
        } else {
            flags[slot] = (byte) (flags[slot] | FLAG_DIRECTION);
        }
    }

    private void setStopType(int slot, int type) {
        flags[slot] =
                (byte)
                        ((flags[slot] & ~(TYPE_MASK << TYPE_SHIFT))
                                | ((type & TYPE_MASK) << TYPE_SHIFT));
    }

    /**
     * 挂载条件单。
     *
     * @return 0=成功；1=容量满；2=重复 orderId
     */
    public int add(
            long orderId,
            long userId,
            long triggerPrice,
            long price,
            long origQty,
            int direction,
            int triggerType,
            long trailDelta,
            long time) {
        if (orderIndex.get(orderId) != -1) {
            return 2;
        }
        int slot = freeHead;
        if (slot == -1) {
            return 1;
        }
        freeHead = next[slot];
        next[slot] = -1;
        prev[slot] = -1;
        flags[slot] = 0;

        orderIds[slot] = orderId;
        userIds[slot] = userId;
        triggerPrices[slot] = triggerPrice;
        prices[slot] = price;
        origQtys[slot] = origQty;
        times[slot] = time;
        trailDeltas[slot] = trailDelta;
        setDirection(slot, direction);
        setStopType(slot, triggerType);

        if (triggerType == TYPE_TRAILING_STOP) {
            if (trailingTail == -1) {
                trailingHead = slot;
            } else {
                next[trailingTail] = slot;
            }
            trailingTail = slot;
            next[slot] = -1;
        } else {
            LevelSlots table = tables[triggerType * 2 + direction];
            int level;
            try {
                level = table.getOrCreate(triggerPrice);
            } catch (IllegalStateException e) {
                // 触发价档位表耗尽：等价容量满——归还槽位并返回 1（可计数拒绝，
                // 绝不抛异常导致订单被静默丢弃）
                next[slot] = freeHead;
                freeHead = slot;
                flags[slot] = 0;
                return 1;
            }
            table.append(level, slot, next, prev);
        }
        orderIndex.put(orderId, slot);
        used++;
        return 0;
    }

    /** 按 orderId 撤条件单（O(1) 定位；trailing 单链摘除需遍历前驱）。 */
    public boolean cancel(long orderId) {
        int slot = orderIndex.get(orderId);
        if (slot == -1) {
            return false;
        }
        int type = triggerType(slot);
        if (type == TYPE_TRAILING_STOP) {
            unlinkTrailing(slot);
        } else {
            LevelSlots table = tables[type * 2 + (isBuy(slot) ? 0 : 1)];
            int level = table.get(triggerPrices[slot]);
            if (level != -1) {
                table.unlink(level, slot, next, prev);
                if (table.counts[level] == 0) {
                    table.removeLevel(level);
                }
            }
        }
        orderIndex.remove(orderId);
        release(slot);
        used--;
        return true;
    }

    private void unlinkTrailing(int slot) {
        int prevSlot = -1;
        int cur = trailingHead;
        while (cur != -1 && cur != slot) {
            prevSlot = cur;
            cur = next[cur];
        }
        if (cur != slot) {
            return;
        }
        if (prevSlot == -1) {
            trailingHead = next[slot];
        } else {
            next[prevSlot] = next[slot];
        }
        if (trailingTail == slot) {
            trailingTail = prevSlot;
        }
        next[slot] = -1;
    }

    /**
     * 触发检查（latest price 跨过触发价的全部条件单）。
     *
     * @param lastPriceKey 最新价（coinScale 定点）
     * @param out 触发槽位输出缓冲（调用方保证容量 ≥ orderCount）
     * @return 触发槽位数；槽位已从索引/链摘除但未归还（调用方读字段转单后逐个 {@link #release(int)}）
     */
    public int checkAndTrigger(long lastPriceKey, int[] out) {
        int n = 0;
        for (int t = 0; t < 4; t++) {
            for (int d = 0; d < 2; d++) {
                LevelSlots table = tables[t * 2 + d];
                // 从触发区间起点开始扫（O(log 档数 + 触发数)）：
                // BUY 降序表（reverse）sortKey=-price，target=-lastPrice；SELL 升序表 target=lastPrice。
                // 起点之后所有档位必然触发，无需逐档判断/跳过。
                // reverse=true（BUY）sortKey=-price → target=-lastPrice；reverse=false（SELL）→
                // target=lastPrice
                long target = isAscendingTable(t, d) ? -lastPriceKey : lastPriceKey;
                // O(1) 预检：全表最大 sortKey < target → 无任何档可触发，整表跳过
                // （价格未跨过任何触发边界；maxSortKey 过期时保守放行，多扫不漏）
                if (table.maxSortKey() < target) {
                    continue;
                }
                int[] update = table.findUpdate(target);
                int pred = update[0];
                int level = pred == -1 ? table.firstLevel() : table.nextLevel(pred);
                while (level != -1) {
                    int nextLevel = table.nextLevel(level);
                    int slot = table.heads[level];
                    while (slot != -1) {
                        int nextSlot = next[slot];
                        out[n++] = slot;
                        table.unlink(level, slot, next, prev);
                        orderIndex.remove(orderIds[slot]);
                        used--;
                        slot = nextSlot;
                    }
                    table.removeLevel(level);
                    level = nextLevel;
                }
                // 触发段（sortKey ≥ target）已整段摘除：剩余链最大 sortKey = 触发段前驱 pred
                // （pred 在触发段之外，removeLevel 不会碰它；表空时为 -1 → Long.MIN_VALUE）
                table.setMaxKey(
                        pred == -1
                                ? Long.MIN_VALUE
                                : (isAscendingTable(t, d)
                                        ? -table.price(pred)
                                        : table.price(pred)));
            }
        }

        // TRAILING_STOP：单链按订单自身 triggerPrice 判断
        int prevSlot = -1;
        int slot = trailingHead;
        while (slot != -1) {
            int nextSlot = next[slot];
            boolean should =
                    isBuy(slot)
                            ? lastPriceKey >= triggerPrices[slot]
                            : lastPriceKey <= triggerPrices[slot];
            if (should) {
                out[n++] = slot;
                if (prevSlot == -1) {
                    trailingHead = nextSlot;
                } else {
                    next[prevSlot] = nextSlot;
                }
                if (trailingTail == slot) {
                    trailingTail = prevSlot;
                }
                next[slot] = -1;
                orderIndex.remove(orderIds[slot]);
                used--;
            } else {
                prevSlot = slot;
            }
            slot = nextSlot;
        }
        return n;
    }

    /** 移动止损：触发价随最优价动态更新（BUY: max(trigger, bestBid - delta)；SELL: min(trigger, bestAsk + delta)）。 */
    public void updateTrailingStops(long bestBid, long bestAsk) {
        int slot = trailingHead;
        while (slot != -1) {
            if (trailDeltas[slot] > 0L) {
                if (isBuy(slot)) {
                    long newTrigger = bestBid - trailDeltas[slot];
                    if (newTrigger > triggerPrices[slot]) {
                        triggerPrices[slot] = newTrigger;
                    }
                } else {
                    long newTrigger = bestAsk + trailDeltas[slot];
                    if (newTrigger < triggerPrices[slot]) {
                        triggerPrices[slot] = newTrigger;
                    }
                }
            }
            slot = next[slot];
        }
    }

    /** 归还已触发/已处理的槽位（调用方读完字段后调用）。 */
    public void release(int slot) {
        next[slot] = freeHead;
        freeHead = slot;
    }

    /**
     * 快照枚举（2026-08-29）：遍历全部在挂条件单（8 张触发价表 + trailing 单链）为
     * {@link TriggerSnapshotFields}。快照写路径低频调用；不改变任何索引/链结构。
     */
    public void forEach(Consumer<TriggerSnapshotFields> consumer) {
        for (int t = 0; t < 4; t++) {
            for (int d = 0; d < 2; d++) {
                LevelSlots table = tables[t * 2 + d];
                int level = table.firstLevel();
                while (level != -1) {
                    int slot = table.heads[level];
                    while (slot != -1) {
                        int nextSlot = next[slot];
                        consumer.accept(
                                new TriggerSnapshotFields(
                                        orderIds[slot],
                                        userIds[slot],
                                        triggerPrices[slot],
                                        prices[slot],
                                        origQtys[slot],
                                        isBuy(slot) ? 0 : 1,
                                        triggerType(slot),
                                        trailDeltas[slot],
                                        times[slot]));
                        slot = nextSlot;
                    }
                    level = table.nextLevel(level);
                }
            }
        }
        int slot = trailingHead;
        while (slot != -1) {
            consumer.accept(
                    new TriggerSnapshotFields(
                            orderIds[slot],
                            userIds[slot],
                            triggerPrices[slot],
                            prices[slot],
                            origQtys[slot],
                            isBuy(slot) ? 0 : 1,
                            triggerType(slot),
                            trailDeltas[slot],
                            times[slot]));
            slot = next[slot];
        }
    }

    /**
     * 快照冻结（2026-08-30，快照迁往 Aeron Archive recording）：单遍把在挂条件单
     * （8 张触发价表 + trailing 单链）拷入紧凑 SoA 缓冲，零分配；与
     * {@link #forEach} 同序同语义。
     */
    public void snapshotTriggers(SnapshotFreeze f) {
        f.ensureStops(orderCount());
        int n = 0;
        for (int t = 0; t < 4; t++) {
            for (int d = 0; d < 2; d++) {
                LevelSlots table = tables[t * 2 + d];
                int level = table.firstLevel();
                while (level != -1) {
                    int slot = table.heads[level];
                    while (slot != -1) {
                        int nextSlot = next[slot];
                        f.triggerIds[n] = orderIds[slot];
                        f.triggerUserIds[n] = userIds[slot];
                        f.triggerPrices[n] = triggerPrices[slot];
                        f.triggerLimitPrices[n] = prices[slot];
                        f.triggerQtys[n] = origQtys[slot];
                        f.triggerTimes[n] = times[slot];
                        f.triggerDirs[n] = (byte) (isBuy(slot) ? 0 : 1);
                        f.triggerTypes[n] = (byte) triggerType(slot);
                        f.triggerTrailDeltas[n] = trailDeltas[slot];
                        n++;
                        slot = nextSlot;
                    }
                    level = table.nextLevel(level);
                }
            }
        }
        int slot = trailingHead;
        while (slot != -1) {
            f.triggerIds[n] = orderIds[slot];
            f.triggerUserIds[n] = userIds[slot];
            f.triggerPrices[n] = triggerPrices[slot];
            f.triggerLimitPrices[n] = prices[slot];
            f.triggerQtys[n] = origQtys[slot];
            f.triggerTimes[n] = times[slot];
            f.triggerDirs[n] = (byte) (isBuy(slot) ? 0 : 1);
            f.triggerTypes[n] = (byte) triggerType(slot);
            f.triggerTrailDeltas[n] = trailDeltas[slot];
            n++;
            slot = next[slot];
        }
        f.triggerCount = n;
    }
}
