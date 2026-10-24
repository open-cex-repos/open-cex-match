package com.ocx.codec.domain;

/**
 * 撮合引擎出站深度档位的原始表示：价格/数量都是已归一化到 symbol 固定精度的 long。
 * 仅供撮合引擎热路径与 IoCodec 编码使用，避免每批快照重建 Decimal 对象。
 */
public record DepthLevel(long price, long volume) {}
