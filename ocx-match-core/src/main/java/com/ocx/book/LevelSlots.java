package com.ocx.book;

/**
 * 价格档位纵表 + 数组跳表索引 — 替代 {@code LongObjectSkipList<MergeOrder>}。
 *
 * <p>每档一行：price / FIFO 头尾（head/tail 槽位索引）/ 档内订单数 / 两列聚合量
 * （普通剩余 {@code normalRemaining} + 冰山显示 {@code icebergDisplayed}，深度 =
 * 两者之和，隐藏量不计）。价格排序索引为"数组化跳表"：{@code next[level][slot]}
 * 存 int 档位索引而非对象引用，无 Node 对象、GC 零扫描，缓存行密度为对象跳表的 4 倍。</p>
 *
 * <p>{@code reverse=true} 为 BUY 簿（价格降序，最优价=最高），内部以 {@code -price}
 * 作排序键；{@code reverse=false} 为 SELL 簿（升序）。最优档 {@link #firstLevel()}、
 * 逐档 {@link #nextLevel(int)} 走底层完整链，深度重建 O(档数) 顺序扫聚合列。</p>
 *
 * <p>档位槽经 freelist（复用底层 {@code next[0]} 列）回收；容量耗尽抛
 * {@link IllegalStateException}（调用方应保证档位数远小于订单容量）。</p>
 */
public final class LevelSlots {

    private final boolean reverse;
    private final int maxLevel;
    private final int capacity;

    /** 原价（tick 定点）。 */
    public final long[] prices;

    /** 档内 FIFO 头槽位（OrderSlots 索引，-1 空）。 */
    public final int[] heads;

    /** 档内 FIFO 尾槽位。 */
    public final int[] tails;

    /** 档内订单数。 */
    public final int[] counts;

    /** 普通订单剩余量聚合（baseCoinScale 定点）。 */
    public final long[] normalRemaining;

    /** 冰山订单显示量聚合（baseCoinScale 定点）。 */
    public final long[] icebergDisplayed;

    /** 各档位实际跳表层数。 */
    public final byte[] nodeLevel;

    /** 跳表指针：next[level][slot]，-1 终止。 */
    public final int[][] next;

    /** 每层链头（-1 空）。 */
    private final int[] levelHeads;

    /** findUpdate 结果暂存（撮合线程单写；getOrCreate/removeLevel 串行调用，无嵌套复用 → 零分配）。 */
    private final int[] updateScratch;

    /**
     * 价格档最近访问缓存（撮合线程单写）：get/getOrCreate 命中 O(1)，免完整跳表扫描
     * （对象版 priceLevelCache 的纵表对应）。命中时校验 {@code prices[cached] == price}
     * 防御 freelist 复用错配；缓存满时静默跳过更新（仅加速，不保正确性）。
     */
    /** 缓存容量 131072：覆盖 exp 模型活跃价格档数（~4 万档）+ 高波动余量，命中率更高。 */
    private static final int LEVEL_CACHE_CAPACITY = 1 << 17;

    private final Long2IntMap levelCache = new Long2IntMap(LEVEL_CACHE_CAPACITY);
    private int freeLevel = -1;
    private int levelSize = 0;
    private long rngState = 0x9E3779B97F4A7C15L;

    /** 全表最大 sortKey（触发扫描 O(1) 预检：maxKey < target 时不可能触发）。 */
    private long maxKey = Long.MIN_VALUE;

    /** maxKey 是否可能已过期（删除过 maxKey 档位）：过期时预检保守返回 +∞（多扫不漏）。 */
    private boolean maxKeyDirty = false;

    public LevelSlots(boolean reverse, int levelCapacity, int maxLevel) {
        this.reverse = reverse;
        this.capacity = levelCapacity;
        this.maxLevel = Math.max(1, maxLevel);
        this.prices = new long[levelCapacity];
        this.heads = new int[levelCapacity];
        this.tails = new int[levelCapacity];
        this.counts = new int[levelCapacity];
        this.normalRemaining = new long[levelCapacity];
        this.icebergDisplayed = new long[levelCapacity];
        this.nodeLevel = new byte[levelCapacity];
        this.next = new int[this.maxLevel][levelCapacity];
        this.levelHeads = new int[this.maxLevel];
        this.updateScratch = new int[this.maxLevel];
        for (int i = 0; i < this.maxLevel; i++) {
            levelHeads[i] = -1;
        }
        for (int i = 0; i < this.maxLevel; i++) {
            java.util.Arrays.fill(next[i], -1);
        }
        java.util.Arrays.fill(heads, -1);
        java.util.Arrays.fill(tails, -1);
        for (int i = 0; i < levelCapacity; i++) {
            next[0][i] = i + 1 < levelCapacity ? i + 1 : -1;
        }
        freeLevel = 0;
    }

    public int capacity() {
        return capacity;
    }

    public int levelCount() {
        return levelSize;
    }

    /** 全表最大 sortKey；maxKey 过期时返回 +∞（保守：不跳过扫描，仅加速不保正确性）。 */
    public long maxSortKey() {
        return maxKeyDirty ? Long.MAX_VALUE : maxKey;
    }

    /** 触发扫描后刷新 maxKey（剩余链最大 = 触发段前驱）；同时清过期标记。 */
    public void setMaxKey(long key) {
        maxKey = key;
        maxKeyDirty = false;
    }

    /** 排序键：BUY 簿取负价（降序），SELL 簿原价（升序）。 */
    private long sortKey(int levelSlot) {
        long p = prices[levelSlot];
        return reverse ? -p : p;
    }

    /** 最优档（BUY=最高价档，SELL=最低价档）；空簿返回 -1。 */
    public int firstLevel() {
        return levelHeads[0];
    }

    /** 次优档（价格向不利方向移动）；-1 表示已到队尾。 */
    public int nextLevel(int levelSlot) {
        return next[0][levelSlot];
    }

    public long price(int levelSlot) {
        return prices[levelSlot];
    }

    /** 查找价格档；不存在返回 -1。 */
    public int get(long price) {
        int cached = levelCache.get(price);
        if (cached != -1 && cached < capacity && prices[cached] == price) {
            return cached;
        }
        int cursor = -1;
        long target = reverse ? -price : price;
        for (int l = maxLevel - 1; l >= 0; l--) {
            while (true) {
                int node = cursor == -1 ? levelHeads[l] : next[l][cursor];
                if (node == -1 || sortKey(node) >= target) {
                    break;
                }
                cursor = node;
            }
        }
        int candidate = cursor == -1 ? levelHeads[0] : next[0][cursor];
        if (candidate != -1 && sortKey(candidate) == target) {
            return candidate;
        }
        return -1;
    }

    /**
     * 查找并记录每层前驱（插入/删除共用）。
     *
     * @return update[level] = 该层最后一个 sortKey < target 的节点（-1 表示头）
     */
    public int[] findUpdate(long target) {
        int[] update = updateScratch;
        int cursor = -1;
        for (int l = maxLevel - 1; l >= 0; l--) {
            while (true) {
                int node = cursor == -1 ? levelHeads[l] : next[l][cursor];
                if (node == -1 || sortKey(node) >= target) {
                    break;
                }
                cursor = node;
            }
            update[l] = cursor;
        }
        return update;
    }

    /** 取价格档，不存在则创建（O(log 档数)）；档位容量耗尽抛异常。 */
    public int getOrCreate(long price) {
        int cached = levelCache.get(price);
        if (cached != -1 && cached < capacity && prices[cached] == price) {
            return cached;
        }
        long target = reverse ? -price : price;
        int[] update = findUpdate(target);
        int candidate = update[0] == -1 ? levelHeads[0] : next[0][update[0]];
        if (candidate != -1 && sortKey(candidate) == target) {
            cachePut(price, candidate);
            return candidate;
        }

        int slot = allocLevel();
        prices[slot] = price;
        heads[slot] = -1;
        tails[slot] = -1;
        counts[slot] = 0;
        normalRemaining[slot] = 0L;
        icebergDisplayed[slot] = 0L;

        int level = randomLevel();
        if (target > maxKey) {
            maxKey = target;
        }
        nodeLevel[slot] = (byte) level;
        for (int l = 0; l < level; l++) {
            int pred = update[l];
            if (pred == -1) {
                next[l][slot] = levelHeads[l];
                levelHeads[l] = slot;
            } else {
                next[l][slot] = next[l][pred];
                next[l][pred] = slot;
            }
        }
        // 高于实际层数的层：该档位不存在于这些层（next 保持 -1）
        for (int l = level; l < maxLevel; l++) {
            next[l][slot] = -1;
        }
        levelSize++;
        cachePut(price, slot);
        return slot;
    }

    /**
     * 返回链上第一个 sortKey ≥ target 的档位（-1 表示无）。
     * O(log 档数)：供 TriggerSlots 触发扫描从触发区间起点开始，避免逐档全表遍历。
     */
    public int firstLevelFrom(long target) {
        int[] update = findUpdate(target);
        return update[0] == -1 ? levelHeads[0] : next[0][update[0]];
    }

    /** 删除价格档（调用方必须先保证档内订单已清空）；不存在静默忽略。 */
    public void removeLevel(int levelSlot) {
        if (levelSlot < 0 || levelSlot >= capacity || counts[levelSlot] != 0) {
            return;
        }
        levelCache.remove(prices[levelSlot]);
        long target = sortKey(levelSlot);
        if (target == maxKey) {
            // 删掉了当前最大 sortKey 档：maxKey 过期，预检保守放行（扫描后由调用方刷新）
            maxKeyDirty = true;
        }
        int[] update = findUpdate(target);
        for (int l = 0; l < nodeLevel[levelSlot]; l++) {
            int pred = update[l];
            if (pred == -1) {
                levelHeads[l] = next[l][levelSlot];
            } else {
                next[l][pred] = next[l][levelSlot];
            }
        }
        nodeLevel[levelSlot] = 0;
        next[0][levelSlot] = freeLevel;
        freeLevel = levelSlot;
        levelSize--;
    }

    /** 缓存更新（tryPut 满时静默跳过，零异常开销——缓存仅加速，不保证覆盖全部档位）。 */
    private void cachePut(long price, int level) {
        levelCache.tryPut(price, level);
    }

    private int allocLevel() {
        int slot = freeLevel;
        if (slot == -1) {
            throw new IllegalStateException("价格档容量耗尽: " + capacity);
        }
        freeLevel = next[0][slot];
        return slot;
    }

    private int randomLevel() {
        rngState ^= rngState << 13;
        rngState ^= rngState >>> 7;
        rngState ^= rngState << 17;
        int level = 1;
        long bits = rngState;
        while ((bits & 1L) == 1L && level < maxLevel) {
            level++;
            bits >>>= 1;
        }
        return level;
    }

    // ── 档内 FIFO（next/prev 列由调用方槽位结构持有，OrderSlots/TriggerSlots 通用）──

    /** 追加订单槽位到档尾（时间优先：后到排后）。 */
    public void append(int levelSlot, int orderSlot, OrderSlots slots) {
        append(levelSlot, orderSlot, slots.next, slots.prev);
    }

    /** 追加到档尾（外部 next/prev 数组版本，如条件单槽位）。 */
    public void append(int levelSlot, int orderSlot, int[] nextArr, int[] prevArr) {
        int tail = tails[levelSlot];
        if (tail == -1) {
            heads[levelSlot] = orderSlot;
            tails[levelSlot] = orderSlot;
            // 防御：新头必须断开 freelist 残留链（2026-09-01 槽位 freelist 复用 next 列）
            nextArr[orderSlot] = -1;
            prevArr[orderSlot] = -1;
        } else {
            nextArr[tail] = orderSlot;
            prevArr[orderSlot] = tail;
            tails[levelSlot] = orderSlot;
            nextArr[orderSlot] = -1; // 新尾断开 freelist 残留链（2026-09-01）
        }
        counts[levelSlot]++;
    }

    /** 从档内 FIFO 摘除订单槽位（O(1)，需订单确实在本档链上）。 */
    public void unlink(int levelSlot, int orderSlot, OrderSlots slots) {
        unlink(levelSlot, orderSlot, slots.next, slots.prev);
    }

    /** 摘除（外部 next/prev 数组版本）。 */
    public void unlink(int levelSlot, int orderSlot, int[] nextArr, int[] prevArr) {
        int p = prevArr[orderSlot];
        int n = nextArr[orderSlot];
        if (p == -1) {
            heads[levelSlot] = n;
        } else {
            nextArr[p] = n;
        }
        if (n == -1) {
            tails[levelSlot] = p;
        } else {
            prevArr[n] = p;
        }
        nextArr[orderSlot] = -1;
        prevArr[orderSlot] = -1;
        counts[levelSlot]--;
    }

    /** 冰山 replenish：摘除后追加到档尾（对齐主流"刷新后回队尾"语义）。 */
    public void moveToTail(int levelSlot, int orderSlot, OrderSlots slots) {
        unlink(levelSlot, orderSlot, slots.next, slots.prev);
        append(levelSlot, orderSlot, slots.next, slots.prev);
    }

    /** 档内数量（测试/诊断）。 */
    public int count(int levelSlot) {
        return counts[levelSlot];
    }

    /** 深度展示量 = 普通剩余 + 冰山显示（隐藏量不计）。 */
    public long depthVolume(int levelSlot) {
        return normalRemaining[levelSlot] + icebergDisplayed[levelSlot];
    }
}
