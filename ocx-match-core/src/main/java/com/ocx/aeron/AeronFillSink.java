package com.ocx.aeron;

import com.ocx.book.MatchFill;
import com.ocx.metrics.MatchMetrics;
import com.ocx.outbound.FillSink;
import com.ocx.outbound.OutboundDepth;

import io.aeron.Publication;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.List;

/**
 * 成交/深度出站的 Aeron 实现（core 撮合线程侧）：编码为出站行发布，
 * 背压时阻塞撮合线程（订单簿状态不变，安全重试，绝不丢单）。
 */
public final class AeronFillSink implements FillSink {

    private final Publication publication;
    private final String symbol;

    /** symbol 16 字节预编码（热路径零分配）。 */
    private final byte[] symbol16;

    /** 指标（可空）：offer 背压计数。 */
    private final MatchMetrics matchMetrics;

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[AeronWire.MAX_OUTBOUND_ROW_SIZE]);
    private final IdleStrategy idle = new BackoffIdleStrategy(100, 100, 1_000, 1_000_000);

    public AeronFillSink(Publication publication, String symbol, MatchMetrics matchMetrics) {
        this.publication = publication;
        this.symbol = symbol;
        this.symbol16 = AeronWire.symbolBytes(symbol);
        this.matchMetrics = matchMetrics;
    }

    @Override
    public void submitFills(List<MatchFill> fills) {
        int n = fills.size();
        int from = 0;
        while (from < n) {
            int count = Math.min(n - from, AeronWire.MAX_FILLS_PER_BATCH);
            int len = AeronWire.encodeFillBatch(buffer, fills, symbol16, from, count);
            offerRetry(len);
            from += count;
        }
    }

    @Override
    public OutboundDepth acquireDepth(String symbol, int coinScale, int baseCoinScale) {
        OutboundDepth depth = new OutboundDepth();
        depth.symbol = symbol;
        depth.priceScale = coinScale;
        depth.volumeScale = baseCoinScale;
        return depth;
    }

    @Override
    public void submitDepth(OutboundDepth depth) {
        int len = AeronWire.encodeDepth(buffer, depth, symbol16);
        offerRetry(len);
    }

    private void offerRetry(int length) {
        long result;
        while ((result = publication.offer(buffer, 0, length)) < 0) {
            if (matchMetrics != null) {
                matchMetrics.recordOutboundBackpressure();
            }
            idle.idle();
        }
    }
}
