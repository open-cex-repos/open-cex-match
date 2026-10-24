package com.ocx.recovery;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

/**
 * 恢复配置（前缀 {@code match.wal}，键名兼容保留）。由 SmallRye Config 映射为不可变
 * 实现，getter 风格方法名自动转为 kebab-case 属性名（{@code getDataDir} → {@code data-dir}）。
 *
 * <p>2026-08-29 状态变更 WAL 已整体退役：恢复唯一事实来源 = 入站 journal + 快照。
 * 2026-08-30 快照迁往 Aeron Archive recording（Chronicle 快照目录已删）：本配置仅承载
 * 恢复开关 / 内嵌 Archive 目录（单 JVM 开发形态）/ Archive 控制通道；enabled=false 时跳过恢复。</p>
 */
@ConfigMapping(prefix = "match.wal")
public interface RecoveryConfig {
    @WithName("enabled")
    @WithDefault("true")
    boolean isEnabled();

    /** 内嵌 Archive 目录根（单 JVM 开发形态 {@code {data-dir}/archive}；生产快照/入站 recording 均在 Archive 目录）。 */
    @WithName("data-dir")
    @WithDefault("/data/match/wal")
    String getDataDir();

    /** Aeron Archive 控制请求通道（恢复回放走同一控制面）。 */
    @WithName("archive-control-channel")
    @WithDefault("aeron:udp?endpoint=localhost:8010")
    String getArchiveControlChannel();

    /** Aeron Archive 控制响应通道。 */
    @WithName("archive-control-response-channel")
    @WithDefault("aeron:udp?endpoint=localhost:0")
    String getArchiveControlResponseChannel();
}
