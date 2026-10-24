package com.ocx.codec.domain;

import com.ocx.math.Decimal;

import java.time.LocalDateTime;

/**
 * 撮合引擎入站订单 DTO（SBE OrderIn 解码结果）。
 *
 * <p>与业务 JPA 实体解耦：撮合引擎只关心撮合所需字段，且数值字段使用
 * 撮合核心 {@link Decimal}，避免把 JPA/BigDecimal 边界带进撮合热路径。</p>
 */
public record OrderIn(
        Long id,
        Long userId,
        String symbol,
        Decimal price,
        Decimal origQty,
        Decimal executedQty,
        Decimal mergeLowPrice,
        Byte side,
        Byte type,
        String timeInForce,
        Byte status,
        LocalDateTime createTime,
        Decimal visibleQty) {}
