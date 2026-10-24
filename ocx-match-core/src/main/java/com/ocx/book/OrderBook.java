package com.ocx.book;

import com.ocx.math.Decimal;
import com.ocx.snapshot.SnapshotFreeze;
import com.ocx.snapshot.SnapshotOrderFields;

import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * 纵表订单簿（字段版）— 在簿状态全部落在 {@link OrderSlots}/{@link LevelSlots}
 * 扁平数组，撮合主循环直接在数组列上整数运算，零对象图、零指针追逐。
 *
 * <p>语义与旧对象版一致：价格-时间优先撮合、冰山显示量聚合（隐藏量可被单笔吃穿、
 * 刷新回队尾）、批量撤单按 userId 链、orderId 判重、挂单上限硬闸。</p>
 *
 * <p>槽位由簿内 freelist 分配，orderId→槽位经 {@link #orderIndex}
 * （Long2IntMap 开地址哈希）映射（2026-09-01 起 orderId 不再内嵌槽位，
 * 回归纯业务号——Snowflake 化高位只管唯一性，引擎内部映射物理位置）。
 * 入簿/撤单/回放 O(1) 哈希一次；判重提前到撮合前。规则见 docs/02-data-structure.md §1.3。</p>
 *
 * <p>撮合线程单写者（Disruptor），所有字段无并发保护；批量撤单/深度读取与撮合
 * 同线程。</p>
 */
public final class OrderBook {

    private final String symbol;
    private final int coinScale;
    private final int baseCoinScale;

    /** 槽位容量（在簿订单上限）。 */
    private final int capacity;

    /** 挂单数硬闸（0=不启用）。 */
    private final long maxOpenOrders;

    /** 槽位纵表。 */
    public final OrderSlots slots;

    /** orderId→槽位 映射（撮合线程单写；容量按在簿上限预分配，高峰零扩容）。 */
    final Long2IntMap orderIndex;

    /**
     * L1D 近期单热缓存：开放寻址 FIFO 淘汰策略，用于加速撤单定位。
     * 撤单先查缓存（~1-2ns 命中），未命中回退 orderIndex（~15-20ns）。
     * 缓存项数 512，约占用 4KB（L1D 的 1/8），适合单核场景。
     */
    private static final int HOT_CACHE_SIZE = 512;
    private static final int HOT_CACHE_MASK = HOT_CACHE_SIZE - 1;
    private final long[] hotCacheKeys = new long[HOT_CACHE_SIZE];
    private final int[] hotCacheValues = new int[HOT_CACHE_SIZE];
    private int hotCachePos = 0;

    /** 买簿（价格降序）。 */
    public final LevelSlots buyLevels;

    /** 卖簿（价格升序）。 */
    public final LevelSlots sellLevels;

    /** 当前挂单数（限价簿；条件单另计，由外部协调）。 */
    private long openOrderCount = 0L;

    /** 在簿买单/卖单笔数（撮合线程单写；指标线程 2s 读一次）。 */
    private long buyOpenCount = 0L;

    private long sellOpenCount = 0L;

    /** 成交记录池（撮合线程单写；sink 同步消费后经 {@link #recycleFills} 归还，热路径零分配）。 */
    private final List<MatchFill> fillPool = new ArrayList<>(128);

    public OrderBook(
            String symbol,
            int coinScale,
            int baseCoinScale,
            int capacity,
            int levelCapacity,
            long maxOpenOrders) {
        this.symbol = symbol;
        this.coinScale = coinScale;
        this.baseCoinScale = baseCoinScale;
        // 容量强制 2 的幂（槽位数组按最坏在簿规模预分配）
        this.capacity = nextPowerOfTwo(Math.max(2, capacity));
        this.maxOpenOrders = maxOpenOrders;
        this.slots = new OrderSlots(this.capacity);
        // orderId→slot 映射：容量扩容至 3×slotsCapacity（负载系数 0.33），
        // 降低哈希探测链长，应对满载场景的性能衰减
        int orderIndexCapacity = (int)
            (maxOpenOrders > 0
                ? Math.min(maxOpenOrders, this.capacity)
                : this.capacity);
        this.orderIndex = new Long2IntMap(orderIndexCapacity * 3);
        this.buyLevels = new LevelSlots(true, levelCapacity, 16);
        this.sellLevels = new LevelSlots(false, levelCapacity, 16);
    }

    public String symbol() {
        return symbol;
    }

    public int coinScale() {
        return coinScale;
    }

    public int baseCoinScale() {
        return baseCoinScale;
    }

    public long openOrderCount() {
        return openOrderCount;
    }

    public long buyOpenCount() {
        return buyOpenCount;
    }

    public long sellOpenCount() {
        return sellOpenCount;
    }

    /** 已占用槽位数（occupy/clear 配对维护，恒等于 openOrderCount）。 */
    public int slotsUsed() {
        return slots.used();
    }

    public long getMaxOpenOrders() {
        return maxOpenOrders;
    }

    public int capacity() {
        return capacity;
    }

    public boolean isFull() {
        return maxOpenOrders > 0 && openOrderCount >= maxOpenOrders;
    }

    /** orderId 是否在簿（判重预检；O(1)）。 */
    public boolean contains(long orderId) {
        return orderIndex.get(orderId) != -1;
    }

    /** orderId 所在的槽位（-1 = 不在簿）。只读查询，供测试/对账侧白盒断言。 */
    public int slotOf(long orderId) {
        return orderIndex.get(orderId);
    }

    /**
     * 快照枚举（2026-08-29）：遍历在簿订单为 {@link SnapshotOrderFields}（快照写，低频路径）。
     * 消费端同步使用（撮合线程单写者，快照写与撮合同线程，无并发）。
     */
    public void forEachOrderFields(Consumer<SnapshotOrderFields> consumer) {
        SnapshotOrderFields f = new SnapshotOrderFields();
        f.symbol = symbol;
        for (int slot = 0; slot < slots.orderIds.length; slot++) {
            if (!slots.isInBook(slot)) {
                continue;
            }
            f.orderId = slots.orderIds[slot];
            f.userId = slots.userIds[slot];
            f.price = slots.prices[slot];
            f.priceExponent = 0;
            f.origQty = slots.origQtys[slot];
            f.origQtyExponent = 0;
            f.direction = slots.isBuy(slot) ? 0 : 1;
            f.tif = slots.tif(slot);
            f.time = slots.times[slot];
            consumer.accept(f);
        }
    }

    /**
     * 快照冻结（2026-08-30，快照迁往 Aeron Archive recording）：撮合线程批末单遍把
     * 在簿订单拷入紧凑 SoA 缓冲，零分配（替代 forEachOrderFields 的对象路径）。
     * 与 {@link #forEachOrderFields} 同序（槽位升序），恢复预填语义不变。
     */
    public void snapshotOrders(SnapshotFreeze f) {
        f.ensureOrders((int) openOrderCount);
        long[] orderIds = slots.orderIds;
        long[] userIds = slots.userIds;
        long[] prices = slots.prices;
        long[] origQtys = slots.origQtys;
        long[] execQtys = slots.execQtys;
        long[] cumQuoteQtys = slots.cumQuoteQtys;
        long[] times = slots.times;
        int n = 0;
        for (int slot = 0; slot < orderIds.length; slot++) {
            if (!slots.isInBook(slot)) {
                continue;
            }
            f.orderIds[n] = orderIds[slot];
            f.userIds[n] = userIds[slot];
            f.prices[n] = prices[slot];
            f.origQtys[n] = origQtys[slot];
            f.execQtys[n] = execQtys[slot];
            f.cumQuoteQtys[n] = cumQuoteQtys[slot];
            f.times[n] = times[slot];
            f.dirs[n] = (byte) (slots.isBuy(slot) ? 0 : 1);
            f.tifs[n] = (byte) slots.tif(slot);
            n++;
        }
        f.orderCount = n;
        f.coinScale = coinScale;
        f.baseCoinScale = baseCoinScale;
    }

    /**
     * 限价单入簿。
     *
     * @return 0=已入簿；1=cap/槽位耗尽；2=重复 orderId；3=价格无效
     */
    public int addOrder(OrderRequest req, long nowMs) {
        return addOrderInternal(req, nowMs);
    }

    /** 恢复预填专用入簿（快照/回放路径共用）：判重仍生效，语义与 addOrder 一致。 */
    public int addOrderDirect(OrderRequest req, long nowMs) {
        return addOrderInternal(req, nowMs);
    }

    private int addOrderInternal(OrderRequest req, long nowMs) {
        if (req.price <= 0L) {
            return 3;
        }
        if (isFull()) {
            return 1;
        }
        if (orderIndex.get(req.orderId) != -1) {
            return 2; // 重复 orderId（判重提前到撮合前，见 MatchService）
        }
        int slot = slots.alloc();
        if (slot == -1) {
            return 1; // 槽位耗尽（无空闲槽）
        }
        slots.occupy(slot);
        slots.orderIds[slot] = req.orderId;
        slots.userIds[slot] = req.userId;
        slots.prices[slot] = req.price;
        slots.origQtys[slot] = req.origQty;
        slots.execQtys[slot] = 0L;
        slots.cumQuoteQtys[slot] = req.filledQuote;
        slots.times[slot] = req.time != 0L ? req.time : nowMs;
        slots.setDirection(slot, req.direction);
        slots.setTif(slot, req.timeInForce);
        slots.setInBook(slot, true);
        orderIndex.put(req.orderId, slot);

        // 更新热缓存：FIFO 淘汰策略
        hotCachePos = (hotCachePos + 1) & HOT_CACHE_MASK;
        hotCacheKeys[hotCachePos] = req.orderId;
        hotCacheValues[hotCachePos] = slot;

        boolean iceberg = req.visibleQty > 0L && req.visibleQty < req.origQty;
        slots.setIceberg(slot, iceberg);
        long displayed = 0L;
        if (iceberg) {
            displayed = Math.min(req.visibleQty, req.origQty);
        }
        slots.visibleQtys[slot] = iceberg ? req.visibleQty : 0L;
        slots.displayedQtys[slot] = displayed;

        LevelSlots levels = req.direction == 0 ? buyLevels : sellLevels;
        int level = levels.getOrCreate(req.price);
        slots.levels[slot] = level; // 记录挂载档位：撤单/摘除直接读，省 price→level 哈希
        levels.append(level, slot, slots);
        if (iceberg) {
            levels.icebergDisplayed[level] += displayed;
        } else {
            levels.normalRemaining[level] += req.origQty;
        }

        openOrderCount++;
        if (req.direction == 0) {
            buyOpenCount++;
        } else {
            sellOpenCount++;
        }
        return 0;
    }

    /**
     * WAL 恢复专用成交回放：更新 execQty/cumQuoteQty 与档位聚合
     * （对齐对象版恢复语义；冰山显示量不做精确回放）。完全成交自动移出簿。
     *
     * @return false 表示订单不在簿（CREATE 缺失/已移除，跳过）
     */
    public boolean applyMatchRecover(long orderId, long execQty) {
        int slot = orderIndex.get(orderId);
        if (slot == -1 || slots.orderIds[slot] != orderId || !slots.isInBook(slot)) {
            return false;
        }
        int direction = slots.isBuy(slot) ? 0 : 1;
        LevelSlots levels = direction == 0 ? buyLevels : sellLevels;
        int level = levels.get(slots.prices[slot]);
        if (level == -1) {
            return false;
        }
        long oldExec = slots.execQtys[slot];
        slots.execQtys[slot] = oldExec + execQty;
        long quote =
                Decimal.multiply(
                        execQty,
                        -baseCoinScale,
                        slots.prices[slot],
                        -coinScale,
                        coinScale,
                        RoundingMode.HALF_UP);
        slots.cumQuoteQtys[slot] += quote;
        if (!slots.isIceberg(slot)) {
            levels.normalRemaining[level] -= execQty;
        } else {
            // 近似：每次回放最多吃掉当前显示量（对齐对象版"不做精确回放"）
            levels.icebergDisplayed[level] -= Math.min(execQty, slots.displayedQtys[slot]);
        }
        if (slots.isCompleted(slot)) {
            removeSlot(slot); // 完全成交移出簿（对齐对象版恢复语义）
        }
        return true;
    }

    /** 按 orderId 撤单（限价簿）。 */
    public boolean cancel(long orderId) {
        // 1. 先查热缓存（开放寻址，1-2ns 命中）
        int hcIdx = (int) (orderId & HOT_CACHE_MASK);
        if (hotCacheKeys[hcIdx] == orderId) {
            int slot = hotCacheValues[hcIdx];
            // 2. 验证缓存是否有效（订单可能已成交/撤销）
            if (slot != -1 && slots.orderIds[slot] == orderId && slots.isInBook(slot)) {
                removeSlot(slot);
                // 3. 缓存失效（订单已撤）
                hotCacheKeys[hcIdx] = 0L;
                hotCacheValues[hcIdx] = -1;
                return true;
            }
        }

        // 4. 缓存未命中，查主哈希
        int slot = orderIndex.get(orderId);
        if (slot == -1 || slots.orderIds[slot] != orderId || !slots.isInBook(slot)) {
            return false;
        }

        // 5. 撤单成功，更新热缓存
        removeSlot(slot);
        // 6. FIFO 淘汰：循环覆盖最旧位置
        hotCachePos = (hotCachePos + 1) & HOT_CACHE_MASK;
        hotCacheKeys[hotCachePos] = 0L; // 订单已撤，清除缓存
        hotCacheValues[hotCachePos] = -1;
        return true;
    }

    /** WAL 恢复专用撤单：跳过 WAL 写入。 */
    public boolean cancelDirect(long orderId) {
        // 1. 先查热缓存
        int hcIdx = (int) (orderId & HOT_CACHE_MASK);
        if (hotCacheKeys[hcIdx] == orderId) {
            int slot = hotCacheValues[hcIdx];
            if (slot != -1 && slots.orderIds[slot] == orderId && slots.isInBook(slot)) {
                removeSlot(slot);
                hotCacheKeys[hcIdx] = 0L;
                hotCacheValues[hcIdx] = -1;
                return true;
            }
        }

        // 2. 缓存未命中，查主哈希
        int slot = orderIndex.get(orderId);
        if (slot == -1 || slots.orderIds[slot] != orderId || !slots.isInBook(slot)) {
            return false;
        }

        // 3. 撤单成功，更新热缓存
        removeSlot(slot);
        hotCachePos = (hotCachePos + 1) & HOT_CACHE_MASK;
        hotCacheKeys[hotCachePos] = 0L;
        hotCacheValues[hotCachePos] = -1;
        return true;
    }

    /** 批量撤单（全簿扫描匹配 userId；2026-08-26 起 userNext 索引已移除）。
     *  低频运营操作，O(在簿单数) 可接受。返回撤销数量。 */
    public int batchCancel(long userId) {
        int count = 0;
        for (int d = 0; d < 2; d++) {
            LevelSlots levels = d == 0 ? buyLevels : sellLevels;
            int level = levels.firstLevel();
            while (level != -1) {
                int nextLevel = levels.nextLevel(level);
                int slot = levels.heads[level];
                while (slot != -1) {
                    int nextSlot = slots.next[slot];
                    if (slots.userIds[slot] == userId) {
                        removeSlot(slot);
                        count++;
                    }
                    slot = nextSlot;
                }
                level = nextLevel;
            }
        }
        return count;
    }

    /**
     * 撮合主循环（taker 吃单）：
     * <ul>
     *   <li>从对侧最优档逐 maker 成交，单笔 taker 可吃穿冰山隐藏量（对齐主流）；</li>
     *   <li>maker 完全成交 → 摘链还槽；</li>
     *   <li>冰山显示量吃穿且 taker 耗尽 → 刷新 displayedQty 并回队尾；</li>
     *   <li>GTC taker 剩余部分自动挂单（IOC/FOK 不挂）。</li>
     * </ul>
     *
     * @param selfTradePrevention true 时同 userId 跳过成交（maker 保留）
     * @param fills 撮合线程复用容器，成交追加其中（调用方负责 clear）
     * @return taker 剩余数量（>0 表示未全部成交）
     */
    public long matchTaker(
            OrderRequest taker, long nowMs, boolean selfTradePrevention, List<MatchFill> fills) {
        LevelSlots contra = taker.direction == 0 ? sellLevels : buyLevels;
        long takerRemaining = taker.origQty;
        long takerCumQuote = 0L;

        int level = contra.firstLevel();
        while (level != -1 && takerRemaining > 0L) {
            long levelPrice = contra.prices[level];
            // 价格不可成交（BUY 时对侧最低卖 > 买价 等）
            boolean cross =
                    taker.direction == 0 ? levelPrice <= taker.price : levelPrice >= taker.price;
            if (!cross) {
                break;
            }

            int slot = contra.heads[level];
            while (slot != -1 && takerRemaining > 0L) {
                int nextSlot = slots.next[slot];
                if (!slots.isInBook(slot)) {
                    slot = nextSlot;
                    continue;
                }
                if (selfTradePrevention
                        && taker.userId != Long.MIN_VALUE
                        && slots.userIds[slot] != Long.MIN_VALUE
                        && taker.userId == slots.userIds[slot]) {
                    slot = nextSlot;
                    continue;
                }

                // 单笔 taker 对同一 maker 连续吃到"taker 耗尽"或"maker 完成"（可吃穿冰山隐藏量）
                long makerDisplay = slots.displayedQtys[slot];
                long makerExecTotal = 0L;
                long consumed = 0L;
                boolean makerDone = false;
                while (takerRemaining > 0L) {
                    long makerRemaining = slots.remaining(slot);
                    if (makerRemaining <= 0L) {
                        makerDone = true;
                        break;
                    }
                    long exec = Math.min(takerRemaining, makerRemaining);
                    slots.execQtys[slot] += exec;
                    makerExecTotal += exec;
                    takerRemaining -= exec;

                    // 聚合：普通单扣全部成交；冰山只扣显示量消耗（累计增量）
                    if (slots.isIceberg(slot)) {
                        long newConsumed = Math.min(makerExecTotal, makerDisplay);
                        contra.icebergDisplayed[level] -= newConsumed - consumed;
                        consumed = newConsumed;
                    } else {
                        contra.normalRemaining[level] -= exec;
                    }

                    // 累计成交额（coinScale 定点，HALF_UP）
                    long quote =
                            Decimal.multiply(
                                    exec,
                                    -baseCoinScale,
                                    levelPrice,
                                    -coinScale,
                                    coinScale,
                                    RoundingMode.HALF_UP);
                    slots.cumQuoteQtys[slot] += quote;
                    takerCumQuote += quote;

                    // 成交记录（值拷贝；从池取，sink 同步消费后归还，零分配）
                    MatchFill fill =
                            fillPool.isEmpty()
                                    ? new MatchFill()
                                    : fillPool.remove(fillPool.size() - 1);
                    fill.direction = taker.direction;
                    fill.symbol = symbol;
                    fill.takerOrderId = taker.orderId;
                    fill.makerOrderId = slots.orderIds[slot];
                    fill.price = levelPrice;
                    fill.priceExponent = -coinScale;
                    fill.qty = exec;
                    fill.qtyExponent = -baseCoinScale;
                    fill.takerCumQuote = takerCumQuote;
                    fill.makerCumQuote = slots.cumQuoteQtys[slot];
                    fill.turnOverExponent = -coinScale;
                    fill.time = nowMs;
                    fills.add(fill);

                    if (slots.isCompleted(slot)) {
                        makerDone = true;
                        break;
                    }
                }

                // maker 处理结束：完成则摘除（聚合已在循环内扣完）；否则冰山显示量收尾
                if (makerDone) {
                    removeFilled(contra, level, slot);
                } else if (takerRemaining <= 0L && slots.isIceberg(slot)) {
                    if (makerExecTotal >= makerDisplay && slots.remaining(slot) > 0L) {
                        // 显示量吃穿且仍有隐藏：刷新显示量并回队尾（对齐主流）
                        long newDisplayed =
                                Math.min(slots.visibleQtys[slot], slots.remaining(slot));
                        contra.icebergDisplayed[level] += newDisplayed;
                        slots.displayedQtys[slot] = newDisplayed;
                        contra.moveToTail(level, slot, slots);
                    } else if (makerExecTotal < makerDisplay) {
                        slots.displayedQtys[slot] = makerDisplay - makerExecTotal;
                    }
                }
                slot = nextSlot;
            }

            if (contra.counts[level] == 0) {
                int nextLevel = contra.nextLevel(level);
                contra.removeLevel(level);
                level = nextLevel;
            } else {
                break; // taker 已耗尽，档内仍有单
            }
        }

        // 部分成交后的剩余挂单：把本单已累计成交额带给入簿路径（cumQuote 跨 taker/maker
        // 阶段单调，修复成交 recording 中累计额回落问题，2026-08-30 成交对账发现）
        taker.filledQuote = takerCumQuote;
        // 剩余挂单由调用方（MatchService）处理：此处只返回剩余量，
        // 使 cap 拒绝可上报（matchTaker 内部挂单会吞掉 addOrder 返回码）
        return takerRemaining;
    }

    /** 归还已消费的成交记录（sink 已同步拷贝值），并清空列表供下一笔复用。 */
    public void recycleFills(List<MatchFill> used) {
        // 手工索引循环：避免 ArrayList.addAll(Collection) 走 toArray/iterator 分配
        for (int i = 0; i < used.size(); i++) {
            fillPool.add(used.get(i));
        }
        used.clear();
    }

    /**
     * 完全成交摘除（撮合循环调用；聚合量已在撮合循环内维护完毕）。
     * 只摘链不摘档——档位摘除统一由撮合主循环的外层档空检查执行
     * （避免 removeLevel 重复调用把 levelSize 减成负数）。
     */
    private void removeFilled(LevelSlots levels, int level, int slot) {
        levels.unlink(level, slot, slots);
        releaseSlot(slot);
    }

    /** 撤单摘除。 */
    private void removeSlot(int slot) {
        int direction = slots.isBuy(slot) ? 0 : 1;
        LevelSlots levels = direction == 0 ? buyLevels : sellLevels;
        int level = slots.levels[slot];
        if (level == -1) {
            // 防御：异常状态（如 WAL 恢复路径未记录档位）回退 price→level 哈希
            level = levels.get(slots.prices[slot]);
        }
        if (level == -1) {
            releaseSlot(slot);
            return;
        }
        long remaining = slots.remaining(slot);
        if (!slots.isIceberg(slot)) {
            levels.normalRemaining[level] -= Math.max(0L, remaining);
        } else {
            levels.icebergDisplayed[level] -= slots.displayedQtys[slot];
        }
        levels.unlink(level, slot, slots);
        if (levels.counts[level] == 0) {
            levels.removeLevel(level);
        }
        releaseSlot(slot);
    }

    private void releaseSlot(int slot) {
        long orderId = slots.orderIds[slot];
        slots.setInBook(slot, false);
        slots.clear(slot); // orderIds 清空 + flags 复位 + used--
        slots.free(slot);
        if (orderId != 0L) {
            orderIndex.remove(orderId);
            // 失效热缓存：订单已成交/撤销
            int hcIdx = (int) (orderId & HOT_CACHE_MASK);
            if (hotCacheKeys[hcIdx] == orderId) {
                hotCacheKeys[hcIdx] = 0L;
                hotCacheValues[hcIdx] = -1;
            }
        }
        openOrderCount--;
        if (slots.isBuy(slot)) {
            buyOpenCount--;
        } else {
            sellOpenCount--;
        }
    }

    // ── 只读：深度 / 最优价 ──

    /** 最优价档（coinScale 定点价格；空簿 -1）。 */
    public long bestPrice(int direction) {
        LevelSlots levels = direction == 0 ? buyLevels : sellLevels;
        int level = levels.firstLevel();
        return level == -1 ? -1L : levels.prices[level];
    }

    /** 逐档复制深度（前 maxDepth 档），返回档数。 */
    public int copyLevels(int direction, long[] priceOut, long[] volumeOut, int maxDepth) {
        LevelSlots levels = direction == 0 ? buyLevels : sellLevels;
        int count = 0;
        int level = levels.firstLevel();
        while (level != -1 && count < maxDepth) {
            priceOut[count] = levels.prices[level];
            volumeOut[count] = levels.depthVolume(level);
            count++;
            level = levels.nextLevel(level);
        }
        return count;
    }

    /** 指定价格以内（含）的对侧可用量（FOK 预检用）。 */
    public long availableVolume(int direction, long upToPrice, long limitQty) {
        LevelSlots levels = direction == 0 ? sellLevels : buyLevels;
        long total = 0L;
        int level = levels.firstLevel();
        while (level != -1) {
            long p = levels.prices[level];
            boolean within = direction == 0 ? p <= upToPrice : p >= upToPrice;
            if (!within) {
                break;
            }
            total += levels.depthVolume(level);
            if (total >= limitQty) {
                break;
            }
            level = levels.nextLevel(level);
        }
        return total;
    }

    /** 价格档数（诊断/测试）。 */
    public int priceLevelSize(int direction) {
        return (direction == 0 ? buyLevels : sellLevels).levelCount();
    }

    private static int nextPowerOfTwo(int v) {
        int n = Integer.highestOneBit(v - 1);
        return n >= (1 << 30) ? 1 << 30 : n << 1;
    }
}
