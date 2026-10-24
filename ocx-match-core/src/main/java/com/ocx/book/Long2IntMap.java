package com.ocx.book;

import java.util.Arrays;

/**
 * 纵表订单簿专用的 long→int 开地址哈希（orderId→slot / userId→head 复用）。
 *
 * <p>替代 {@code Long2ObjectHashMap}：value 是 int 槽位索引（4B）而非对象引用（8B），
 * 缓存行密度翻倍，GC 零扫描。线性探测 + 删除墓碑化：remove 置墓碑 O(1)（无集群
 * rehash），墓碑超过容量 1/4 时用复用暂存数组就地重建——撮合热路径（每成交移除
 * maker）不再付出删除后的探测链修复成本。</p>
 *
 * <p>哨兵约定：{@code values[i] == -1} 表示空槽，{@code Integer.MIN_VALUE} 为墓碑
 * （槽位索引恒 ≥ 0）；key 可为任意 long 值（含 0）。容量为 2 的幂，负载 0.65
 * 预分配不扩容（对齐 bookIndex 的"高峰周期零扩容"策略）。</p>
 */
public final class Long2IntMap {

    private static final int EMPTY = -1;

    /** 墓碑哨兵（槽位索引恒 ≥ 0，Integer.MIN_VALUE 安全）。 */
    private static final int TOMBSTONE = Integer.MIN_VALUE;

    /** 墓碑占比达容量 1/4 触发就地重建（摊销 O(1)）。 */
    private static final int COMPACT_THRESHOLD_SHIFT = 2;

    private long[] keys;
    private int[] values;
    private int mask;
    private int size;
    private int tombstoneCount;

    /** 重建暂存（懒分配复用；live 峰值约容量 0.65，一次分配常驻）。 */
    private long[] tmpKeys;

    private int[] tmpValues;

    public Long2IntMap(int expected) {
        int capacity = nextPowerOfTwo(Math.max(16, (int) (expected / 0.65f) + 1));
        this.keys = new long[capacity];
        this.values = new int[capacity];
        Arrays.fill(values, EMPTY);
        this.mask = capacity - 1;
        this.size = 0;
        this.tombstoneCount = 0;

        // 预分配重建暂存数组，避免首次 compact 时在热路径触发 GC
        this.tmpKeys = new long[capacity];
        this.tmpValues = new int[capacity];
    }

    public int size() {
        return size;
    }

    public int capacity() {
        return values.length;
    }

    /** 插入/覆盖；返回旧值或 -1。墓碑槽优先复用，探测不停在墓碑。 */
    public int put(long key, int value) {
        if (size + tombstoneCount >= values.length) {
            compact();
        }
        int idx = index(key);
        int start = idx;
        int firstTomb = -1;
        while (true) {
            int v = values[idx];
            if (v == EMPTY) {
                if (firstTomb != -1) {
                    idx = firstTomb;
                    tombstoneCount--;
                }
                keys[idx] = key;
                values[idx] = value;
                size++;
                maybeCompact();
                return EMPTY;
            }
            if (v == TOMBSTONE) {
                if (firstTomb == -1) {
                    firstTomb = idx;
                }
            } else if (keys[idx] == key) {
                int old = values[idx];
                values[idx] = value;
                return old;
            }
            idx = (idx + 1) & mask;
            if (idx == start) {
                throw new IllegalStateException(
                        "Long2IntMap capacity exceeded: size="
                                + size
                                + ", capacity="
                                + values.length);
            }
        }
    }

    /** 满时不抛异常的 put（缓存场景）：返回 false 表示容量满未写入（调用方可忽略）。 */
    public boolean tryPut(long key, int value) {
        if (size + tombstoneCount >= values.length) {
            compact();
        }
        if (size + tombstoneCount >= values.length) {
            return false;
        }
        int idx = index(key);
        int start = idx;
        int firstTomb = -1;
        while (true) {
            int v = values[idx];
            if (v == EMPTY) {
                if (firstTomb != -1) {
                    idx = firstTomb;
                    tombstoneCount--;
                }
                keys[idx] = key;
                values[idx] = value;
                size++;
                maybeCompact();
                return true;
            }
            if (v == TOMBSTONE) {
                if (firstTomb == -1) {
                    firstTomb = idx;
                }
            } else if (keys[idx] == key) {
                values[idx] = value;
                return true;
            }
            idx = (idx + 1) & mask;
            if (idx == start) {
                return false; // 防御死循环：表已满无可用槽
            }
        }
    }

    /** 查询；不存在返回 -1。探测跳过墓碑、停在空槽。 */
    public int get(long key) {
        int idx = index(key);
        while (true) {
            int v = values[idx];
            if (v == EMPTY) {
                return EMPTY;
            }
            if (v != TOMBSTONE && keys[idx] == key) {
                return values[idx];
            }
            idx = (idx + 1) & mask;
        }
    }

    public boolean containsKey(long key) {
        return get(key) != EMPTY;
    }

    /** 删除并返回旧值；不存在返回 -1。墓碑化：置墓碑 + 计数，不 rehash。 */
    public int remove(long key) {
        int idx = index(key);
        while (true) {
            int v = values[idx];
            if (v == EMPTY) {
                return EMPTY;
            }
            if (v != TOMBSTONE && keys[idx] == key) {
                values[idx] = TOMBSTONE;
                size--;
                tombstoneCount++;
                maybeCompact();
                return v;
            }
            idx = (idx + 1) & mask;
        }
    }

    public void clear() {
        Arrays.fill(values, EMPTY);
        size = 0;
        tombstoneCount = 0;
    }

    private void maybeCompact() {
        if (tombstoneCount >= (values.length >> COMPACT_THRESHOLD_SHIFT)) {
            compact();
        }
    }

    /** 就地重建：live 条目收集到复用暂存数组，清空后按哈希重新插入（墓碑清零）。 */
    private void compact() {
        if (tombstoneCount == 0) {
            return;
        }
        int w = 0;
        for (int i = 0; i < values.length; i++) {
            int v = values[i];
            if (v != EMPTY && v != TOMBSTONE) {
                tmpKeys[w] = keys[i];
                tmpValues[w] = v;
                w++;
            }
        }
        Arrays.fill(values, EMPTY);
        size = 0;
        tombstoneCount = 0;
        for (int i = 0; i < w; i++) {
            long k = tmpKeys[i];
            int idx = index(k);
            while (values[idx] != EMPTY) {
                idx = (idx + 1) & mask;
            }
            keys[idx] = k;
            values[idx] = tmpValues[i];
            size++;
        }
    }

    private int index(long key) {
        long h = key * 0x9E3779B97F4A7C15L;
        h ^= h >>> 32;
        return (int) h & mask;
    }

    private static int nextPowerOfTwo(int v) {
        int n = Integer.highestOneBit(v - 1);
        return n >= (1 << 30) ? 1 << 30 : n << 1;
    }
}
