package com.ocx.aeron;

import io.aeron.Aeron;

/**
 * 控制流命令行入口（2026-08-30，替代 {@code StressGoSignal}）：
 * {@code --go} 发布 GO 信号；{@code --wait-ready <name>} 等待某进程 READY
 * （媒体驱动未起时自动重试建连）；{@code --ready <name>} 手动发布 READY（测试用）。
 * 引擎进程自身的 READY 由 {@link ControlStream#publishReady} 在启动完成时发布。
 */
public final class StressControl {

    private StressControl() {}

    public static void main(String[] args) {
        String aeronDir = System.getProperty("ocx.aeron.dir", "/tmp/ocx-aeron");
        String mode = "";
        String name = "";
        int timeoutSec = 60;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--go" -> mode = "go";
                case "--ready" -> {
                    mode = "ready";
                    name = args[++i];
                }
                case "--wait-ready" -> {
                    mode = "wait-ready";
                    name = args[++i];
                }
                case "--timeout" -> timeoutSec = Integer.parseInt(args[++i]);
                default -> {}
            }
        }

        if (mode.equals("wait-ready")) {
            if (!ControlStream.waitReady(aeronDir, name, timeoutSec * 1000L)) {
                System.err.println("[StressControl] 等待 READY 超时: " + name);
                System.exit(1);
            }
            System.out.println("[StressControl] READY 收到: " + name);
            return;
        }

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir))) {
            switch (mode) {
                case "go" -> {
                    if (!ControlStream.publish(aeron, ControlStream.TYPE_GO, "go", 5_000L)) {
                        System.err.println(
                                "[StressControl] GO 发布超时: stream=" + StreamIds.CONTROL_STREAM);
                        System.exit(1);
                    }
                    System.out.println(
                            "[StressControl] GO 已发布 stream="
                                    + StreamIds.CONTROL_STREAM
                                    + " dir="
                                    + aeronDir);
                }
                case "ready" -> {
                    if (!ControlStream.publish(aeron, ControlStream.TYPE_READY, name, 5_000L)) {
                        System.err.println("[StressControl] READY 发布超时: " + name);
                        System.exit(1);
                    }
                    System.out.println("[StressControl] READY 已发布: " + name);
                }
                default -> {
                    System.err.println(
                            "用法: StressControl --go | --ready <name> | --wait-ready <name>"
                                + " [--timeout <sec>]");
                    System.exit(2);
                }
            }
        }
    }
}
