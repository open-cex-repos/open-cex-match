package com.ocx.codec.domain;

import java.util.List;

/**
 * 盘口深度快照消息 —— ocx-match 撮合后产出，供读端盘口缓存消费
 * （历史经 Kafka topic market.depth 投递，2026-08-28 链路切换后该域对象版仅用于
 * IoCodec 编解码与测试基准，实际链路走字段版 SBE 批量帧 → egress → Archive）。
 * <p>
 * 放在 ocx-common 以保证「写端(match) / 消费端(trade) / 读端(trade)」三处用同一份
 * {@link DepthItemVo} 结构序列化，杜绝两端结构漂移。
 * <p>
 * asks/bids 与读端 Redis 深度缓存中的
 * {@code Map<String,List<DepthItemVo>>}（key: asks/bids）一一对应：
 *   asks = 卖盘(sellTradePlate.items, 价格从低到高)，
 *   bids = 买盘(buyTradePlate.items, 价格从高到低)。
 *
 * @param symbol 交易对，如 BTCGCN（与撮合引擎 OrderBooks.symbol 完全一致，用于拼 Redis key）
 * @param asks   卖盘档位
 * @param bids   买盘档位
 */
public record DepthSnapshotMessage(String symbol, List<DepthItemVo> asks, List<DepthItemVo> bids) {}
