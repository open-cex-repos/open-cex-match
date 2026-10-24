package com.ocx.config;

import org.eclipse.microprofile.config.spi.ConfigSource;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * 命令行 {@code --key=value} 配置源，优先级最高（ordinal 500，高于系统属性 400）。
 * 保留原自研 loader 的 CLI 传参方式，压测脚本与部署零改动。
 */
public final class CliConfigSource implements ConfigSource {

    public static final int ORDINAL = 500;

    private final Map<String, String> properties;

    public CliConfigSource(String[] args) {
        Map<String, String> parsed = new LinkedHashMap<>();
        if (args != null) {
            for (String arg : args) {
                if (arg.startsWith("--")) {
                    String kv = arg.substring(2);
                    int idx = kv.indexOf('=');
                    if (idx > 0) {
                        parsed.put(kv.substring(0, idx), kv.substring(idx + 1));
                    }
                }
            }
        }
        this.properties = Map.copyOf(parsed);
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public Set<String> getPropertyNames() {
        return properties.keySet();
    }

    @Override
    public String getValue(String name) {
        return properties.get(name);
    }

    @Override
    public String getName() {
        return "CliConfigSource";
    }

    @Override
    public int getOrdinal() {
        return ORDINAL;
    }
}
