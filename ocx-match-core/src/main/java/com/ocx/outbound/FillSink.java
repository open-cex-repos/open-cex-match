package com.ocx.outbound;


import com.ocx.book.MatchFill;

import java.util.List;

/**
 * 成交/深度出站契约（由撮合核心定义，egress 作为消费方实现）。
 *
 * <p>撮合线程只做值拷贝与入队即返回，编码/发送/持久化全部在出站侧。</p>
 */
public interface FillSink {

    /** 提交本批成交（值容器由实现方池化/复用）。 */
    void submitFills(List<MatchFill> fills);

    /** 领取一个可复用的深度值容器（撮合线程填充后 {@link #submitDepth} 归还）。 */
    OutboundDepth acquireDepth(String symbol, int coinScale, int baseCoinScale);

    /** 提交盘口深度快照。 */
    void submitDepth(OutboundDepth depth);
}
