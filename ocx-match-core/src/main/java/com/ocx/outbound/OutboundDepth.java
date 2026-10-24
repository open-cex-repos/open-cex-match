package com.ocx.outbound;

/**
 * 盘口深度快照出站数据（池化复用，避免撮合线程逐次分配数组）。
 *
 * <p>撮合线程在 endOfBatch 从 {@link FillSink#acquireDepth} 领取一个空闲实例，
 * 把盘口层级拷贝到预分配数组后提交；发送线程编码完 offer 给 Aeron Archive 后归还对象池。
 * 字段全部 package 无关（public），仅作内部传输对象，不做封装。</p>
 */
public final class OutboundDepth {

    /** 与撮合线程深度快照一致的最大层级数。 */
    public static final int MAX_LEVELS = 100;

    public final long[] askPrices = new long[MAX_LEVELS];
    public final long[] askVolumes = new long[MAX_LEVELS];
    public final long[] bidPrices = new long[MAX_LEVELS];
    public final long[] bidVolumes = new long[MAX_LEVELS];

    public int askCount;
    public int bidCount;
    public long ts;
    public String symbol;
    public int priceScale;
    public int volumeScale;
}
