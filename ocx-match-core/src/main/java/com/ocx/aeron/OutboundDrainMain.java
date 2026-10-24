package com.ocx.aeron;

import com.ocx.config.ConfigLoader;
import com.ocx.config.MatchEngineConfig;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 出站哑消费（core-only 标定辅助进程）：只订阅 core 的成交/深度出站环并丢帧，
 * 不做任何 SBE 编码/持久化。
 *
 * <p>用途：把 egress 排除在链路外，单独标定 match-core 撮合线程的真实上限——
 * 出站环仍有订阅者，AeronFillSink 不会因 NOT_CONNECTED 自旋卡死撮合线程；
 * 消费侧零成本，背压≈0，压出来的就是 core 撮合 + 入站 ingest + WAL 发布的极限。</p>
 *
 * <p>symbols 由 {@code -Docx.drain.symbols=}（逗号分隔）指定，缺省读引擎配置
 * （{@code spring.match.symbols.*}），再缺省 BTCUSDT。</p>
 */
public final class OutboundDrainMain {

    private OutboundDrainMain() {}

    public static void main(String[] args) {
        String aeronDir = System.getProperty("ocx.aeron.dir", "/tmp/ocx-aeron");
        Set<String> symbols = resolveSymbols(args);
        List<Subscription> subs = new ArrayList<>();
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir))) {
            for (String symbol : symbols) {
                subs.add(aeron.addSubscription(OcxAeron.CHANNEL, StreamIds.outbound(symbol)));
            }
            System.err.println(
                    "[OutboundDrainMain] OUTBOUND_DRAIN_READY symbols="
                            + symbols
                            + " aeron="
                            + aeronDir);
            ControlStream.publishReady(aeronDir, "drain");
            FragmentHandler drop = (buffer, offset, length, header) -> {};
            IdleStrategy idle = new BackoffIdleStrategy(100, 100, 1_000, 1_000_000);
            while (true) {
                int work = 0;
                for (Subscription s : subs) {
                    work += s.poll(drop, 100);
                }
                idle.idle(work);
            }
        }
    }

    private static Set<String> resolveSymbols(String[] args) {
        String prop = System.getProperty("ocx.drain.symbols");
        if (prop != null && !prop.isBlank()) {
            Set<String> set = new LinkedHashSet<>();
            for (String s : prop.split(",")) {
                if (!s.isBlank()) {
                    set.add(s.trim());
                }
            }
            if (!set.isEmpty()) {
                return set;
            }
        }
        try {
            MatchEngineConfig cfg = ConfigLoader.load(args);
            return new LinkedHashSet<>(cfg.getMatchEngineProperties().getSymbols().keySet());
        } catch (Exception e) {
            return Set.of("BTCUSDT");
        }
    }
}
