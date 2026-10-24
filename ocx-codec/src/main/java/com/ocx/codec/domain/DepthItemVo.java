package com.ocx.codec.domain;

import com.ocx.math.Decimal;

/** 深度条目 — 盘口单行数据 */
public record DepthItemVo(Decimal price, Decimal volume) {}
