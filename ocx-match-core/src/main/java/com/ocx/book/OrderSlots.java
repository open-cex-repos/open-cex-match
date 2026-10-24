package com.ocx.book;

/**
 * 在簿订单槽位纵表（SoA）— 撮合簿内订单状态的唯一存储。
 *
 * <p>每行 = 一个在簿订单槽位，字段纵排为独立 long/int/byte 数组；槽位由簿内
 * freelist 分配（{@code next[]} 空闲时复用为链指针，与 TriggerSlots 同款模式），
 * orderId→槽位由 {@link OrderBook#orderIndex} 映射（2026-09-01 起不再
 * 内嵌槽位）。{@code orderIds[slot] == 0} 表示空槽。所有数组构造期预分配、
 * 常驻，热路径零分配、GC 零扫描。</p>
 *
 * <p>链表列：{@code next}/{@code prev} 维护同价格档 FIFO（由 {@link LevelSlots}
 * 操作）。批量撤单走全簿扫描（2026-08-26：userNext 索引为低频操作付高频哈希成本，
 * 已移除——每挂单单省 2 次哈希 + 4B/单内存）。</p>
 *
 * <p>价格/数量为统一 scale 定点 long（构造时由调用方保证语义，撮合仅做整数运算）。</p>
 */
public final class OrderSlots {

    // ── flags 位定义 ──
    /** bit0：方向，0=BUY 1=SELL。 */
    public static final int FLAG_DIRECTION = 0x01;

    /** bit1：冰山订单（visibleQty > 0）。 */
    public static final int FLAG_ICEBERG = 0x02;

    /** bit2：仅减仓（合约）。 */
    public static final int FLAG_REDUCE_ONLY = 0x04;

    /** bit3：当前在簿（入簿置位，出簿清位；user 链惰性清理依赖此标记）。 */
    public static final int FLAG_IN_BOOK = 0x08;

    /** bit4-5：TimeInForce（GTC=0 IOC=1 FOK=2 POST_ONLY=3）。 */
    public static final int TIF_SHIFT = 4;

    public static final int TIF_MASK = 0x03;

    // ── 列（public final：撮合热路径直接数组读写，JIT 无边界检查消除后为纯地址运算）──
    public final long[] orderIds;
    public final long[] userIds;

    /** 统一 tick 精度定点价格。 */
    public final long[] prices;

    /** 原始数量（baseCoinScale 定点）。 */
    public final long[] origQtys;

    /** 已成交数量（baseCoinScale 定点）。 */
    public final long[] execQtys;

    /** 累计成交额（coinScale 定点）。 */
    public final long[] cumQuoteQtys;

    /** 冰山可见量（非冰山为 0）。 */
    public final long[] visibleQtys;

    /** 冰山当前显示量（非冰山为 0）。 */
    public final long[] displayedQtys;

    /** 入簿时间（ms）。 */
    public final long[] times;

    /** 同价 FIFO 链 next（空闲槽复用作 freelist 链指针）。 */
    public final int[] next;

    /** 同价 FIFO 链 prev（-1 表示链头）。 */
    public final int[] prev;

    /** 订单所在价格档位（LevelSlots 索引；-1=未挂载/已释放）。
     *  撤单/成交摘除直接读本列，省掉 price→level 哈希（原 removeSlot 每单一次 levelCache.get）。 */
    public final int[] levels;

    public final byte[] flags;

    private final int capacity;
    private int used = 0;

    /** 空闲槽链表头（next[slot] 链接；-1 表示无空闲槽）。 */
    private int freeHead;

    public OrderSlots(int capacity) {
        this.capacity = capacity;
        this.orderIds = new long[capacity];
        this.userIds = new long[capacity];
        this.prices = new long[capacity];
        this.origQtys = new long[capacity];
        this.execQtys = new long[capacity];
        this.cumQuoteQtys = new long[capacity];
        this.visibleQtys = new long[capacity];
        this.displayedQtys = new long[capacity];
        this.times = new long[capacity];
        this.next = new int[capacity];
        this.prev = new int[capacity];
        this.levels = new int[capacity];
        this.flags = new byte[capacity];
        // 构造期整链为 freelist（与 TriggerSlots 同款）：alloc 从头部弹、free 压头
        this.freeHead = 0;
        for (int i = 0; i < capacity; i++) {
            next[i] = i + 1 < capacity ? i + 1 : -1;
            prev[i] = -1;
            levels[i] = -1;
        }
    }

    public int capacity() {
        return capacity;
    }

    public int used() {
        return used;
    }

    /** 分配一个空闲槽（O(1)）；无空闲槽返回 -1。调用方随后须 {@link #occupy}。 */
    public int alloc() {
        int slot = freeHead;
        if (slot == -1) {
            return -1;
        }
        freeHead = next[slot];
        // 复位链指针/档位引用（防御 freelist 复用错配；与 TriggerSlots.add 同款）
        next[slot] = -1;
        prev[slot] = -1;
        levels[slot] = -1;
        return slot;
    }

    /** 归还槽位到 freelist（调用方必须先 {@link #clear}；O(1)）。 */
    public void free(int slot) {
        next[slot] = freeHead;
        freeHead = slot;
    }

    /** 占用计数（调用方先确认空槽后调用；与 openOrderCount 同步语义）。 */
    public void occupy(int slot) {
        used++;
    }

    /** 归还槽位（调用方必须先摘链）：orderIds 清空 + 档位引用清 + flags 复位。 */
    public void clear(int slot) {
        orderIds[slot] = 0L;
        levels[slot] = -1; // 摘除后清档位引用（freelist 复用前 addOrder 会重新写入）
        flags[slot] = 0;
        used--;
    }

    /** 空槽判定（orderIds[slot] == 0；id=0 被入站校验拒绝，安全作空哨兵）。 */
    public boolean isOccupied(int slot) {
        return orderIds[slot] != 0L;
    }

    public boolean isInBook(int slot) {
        return (flags[slot] & FLAG_IN_BOOK) != 0;
    }

    public void setInBook(int slot, boolean inBook) {
        if (inBook) {
            flags[slot] = (byte) (flags[slot] | FLAG_IN_BOOK);
        } else {
            flags[slot] = (byte) (flags[slot] & ~FLAG_IN_BOOK);
        }
    }

    /** 0=BUY 1=SELL（对齐 {@code OrderDirection} 数值约定：0 买入 1 卖出）。 */
    public boolean isBuy(int slot) {
        return (flags[slot] & FLAG_DIRECTION) == 0;
    }

    public void setDirection(int slot, int direction) {
        if (direction == 0) {
            flags[slot] = (byte) (flags[slot] & ~FLAG_DIRECTION);
        } else {
            flags[slot] = (byte) (flags[slot] | FLAG_DIRECTION);
        }
    }

    public boolean isIceberg(int slot) {
        return (flags[slot] & FLAG_ICEBERG) != 0;
    }

    public void setIceberg(int slot, boolean iceberg) {
        if (iceberg) {
            flags[slot] = (byte) (flags[slot] | FLAG_ICEBERG);
        } else {
            flags[slot] = (byte) (flags[slot] & ~FLAG_ICEBERG);
        }
    }

    public int tif(int slot) {
        return (flags[slot] >>> TIF_SHIFT) & TIF_MASK;
    }

    public void setTif(int slot, int tif) {
        flags[slot] =
                (byte) ((flags[slot] & ~(TIF_MASK << TIF_SHIFT)) | ((tif & TIF_MASK) << TIF_SHIFT));
    }

    /** 剩余可成交量 = origQty - execQty（定点整数差，撮合热路径）。 */
    public long remaining(int slot) {
        return origQtys[slot] - execQtys[slot];
    }

    public boolean isCompleted(int slot) {
        return remaining(slot) <= 0L;
    }
}
