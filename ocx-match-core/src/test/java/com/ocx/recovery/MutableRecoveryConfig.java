package com.ocx.recovery;

/**
 * 测试用可变 {@link RecoveryConfig} 实现：生产装配由 SmallRye Config 生成不可变实现，
 * 单测里需要手工构造时用本类（保持原 POJO 的 setter 风格）。
 */
public final class MutableRecoveryConfig implements RecoveryConfig {

    private boolean enabled = true;
    private String dataDir = "/data/match/wal";
    private String archiveControlChannel = "aeron:udp?endpoint=localhost:8010";
    private String archiveControlResponseChannel = "aeron:udp?endpoint=localhost:0";

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public String getDataDir() {
        return dataDir;
    }

    @Override
    public String getArchiveControlChannel() {
        return archiveControlChannel;
    }

    @Override
    public String getArchiveControlResponseChannel() {
        return archiveControlResponseChannel;
    }

    public MutableRecoveryConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public MutableRecoveryConfig setDataDir(String dataDir) {
        this.dataDir = dataDir;
        return this;
    }

    public MutableRecoveryConfig setArchiveControlChannel(String channel) {
        this.archiveControlChannel = channel;
        return this;
    }

    public MutableRecoveryConfig setArchiveControlResponseChannel(String channel) {
        this.archiveControlResponseChannel = channel;
        return this;
    }
}
