package com.ocx.config;

import com.ocx.recovery.RecoveryConfig;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.smallrye.config.source.yaml.YamlConfigSource;

import java.net.URL;

/**
 * 配置装配器：基于 SmallRye Config（MicroProfile Config 参考实现）合并
 * 基础 yml + profile yml + CLI 参数 + 系统属性(-D) + 环境变量，
 * 再经 {@code @ConfigMapping} 接口映射为不可变配置对象。
 *
 * <p>优先级(高→低): CLI --key=value (500) &gt; 系统属性 -D (400) &gt; 环境变量 (300)
 * &gt; application-{profile}.yml (230) &gt; application.yml (200)。
 * 键名与部署完全兼容：kebab-case/camelCase 大小写宽松匹配，未知键在启动时被
 * {@code @ConfigMapping} 校验拒绝（不再静默跳过）。</p>
 */
public final class ConfigLoader {

    /** yml 源优先级：低于环境变量(300)/系统属性(400)/CLI(500)，profile 高于 base。 */
    private static final int BASE_YAML_ORDINAL = 200;

    private static final int PROFILE_YAML_ORDINAL = 230;

    private ConfigLoader() {}

    public static MatchEngineConfig load(String[] args) {
        String profile = resolveProfile(args);
        SmallRyeConfig config =
                new SmallRyeConfigBuilder()
                        .withSources(yamlSource("application.yml", BASE_YAML_ORDINAL))
                        .withSources(
                                profile == null || profile.isBlank()
                                        ? null
                                        : yamlSource(
                                                "application-" + profile + ".yml",
                                                PROFILE_YAML_ORDINAL))
                        .withSources(new CliConfigSource(args))
                        .addDefaultSources()
                        .withMapping(MatchEngineProperties.class)
                        .withMapping(RecoveryConfig.class)
                        .withMapping(MatchTuningConfig.class)
                        .build();

        return new MatchEngineConfig(
                config.getConfigMapping(MatchEngineProperties.class),
                config.getConfigMapping(RecoveryConfig.class),
                config.getConfigMapping(MatchTuningConfig.class),
                profile);
    }

    /** 从 classpath 取 yml，不存在返回 null（builder 会跳过）。 */
    private static YamlConfigSource yamlSource(String resource, int ordinal) {
        URL url = ConfigLoader.class.getClassLoader().getResource(resource);
        if (url == null) {
            return null;
        }
        try {
            return new YamlConfigSource(url, ordinal);
        } catch (Exception e) {
            throw new IllegalStateException("解析 yml 失败: " + resource, e);
        }
    }

    private static String resolveProfile(String[] args) {
        if (args != null) {
            for (String a : args) {
                if (a.startsWith("--spring.profiles.active=")) {
                    return a.substring("--spring.profiles.active=".length());
                }
            }
        }
        String sys = System.getProperty("spring.profiles.active");
        if (sys != null && !sys.isBlank()) {
            return sys;
        }
        String env = System.getenv("SPRING_PROFILES_ACTIVE");
        if (env != null && !env.isBlank()) {
            return env;
        }
        return "spot";
    }
}
