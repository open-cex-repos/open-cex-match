package com.ocx.ha;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ocx.aeron.OcxAeron;

import io.aeron.Aeron;
import io.aeron.Subscription;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * self-fencing 语义验证（2026-09-01，防双主核心）：
 * primary 收到被吊销的 epoch（协调者已提升备机）必须自杀；
 * standby 收到新 epoch（被提升）必须接受并继续（不得误自杀）。
 */
class HaHeartbeatFencingTest {

    private static final String HB_SEND = "aeron:udp?endpoint=localhost:54101";
    private static final String HB_LISTEN = "aeron:udp?endpoint=0.0.0.0:54100";
    private static final String ACK_SEND = "aeron:udp?endpoint=localhost:54100";
    private static final String ACK_LISTEN = "aeron:udp?endpoint=0.0.0.0:54101";

    @Test
    void primaryFencesWhenEpochRevoked() throws Exception {
        try (OcxAeron embedded = OcxAeron.embedded();
                Aeron coord =
                        Aeron.connect(
                                new Aeron.Context()
                                        .aeronDirectoryName(embedded.aeronDirectoryName()));
                Subscription hbSub =
                        coord.addSubscription(ACK_LISTEN, HaControlStream.HA_CONTROL_STREAM)) {
            AtomicBoolean fenced = new AtomicBoolean();
            HaHeartbeat hb =
                    new HaHeartbeat(
                            embedded.aeronDirectoryName(),
                            HB_SEND,
                            HB_LISTEN,
                            1,
                            "primary",
                            () -> fenced.set(true));
            Thread t = new Thread(hb, "hb-test-primary");
            t.start();

            // 正常续租（epoch=1）：不得误自杀
            ack(coord, 1, 1L);
            Thread.sleep(1_500L);
            assertFalse(fenced.get(), "正常续租时 primary 不得自杀");

            // 吊销（协调者提升后 epoch=2）：primary 必须自杀
            ack(coord, 1, 2L);
            long deadline = System.currentTimeMillis() + 5_000L;
            while (System.currentTimeMillis() < deadline && !fenced.get()) {
                Thread.sleep(50L);
            }
            assertTrue(fenced.get(), "primary 收到吊销 epoch 必须 self-fence（防双主）");
            hb.stop();
            t.join(2_000L);
        }
    }

    @Test
    void standbyAcceptsNewEpochAfterPromote() throws Exception {
        try (OcxAeron embedded = OcxAeron.embedded();
                Aeron coord =
                        Aeron.connect(
                                new Aeron.Context()
                                        .aeronDirectoryName(embedded.aeronDirectoryName()));
                Subscription hbSub =
                        coord.addSubscription(ACK_LISTEN, HaControlStream.HA_CONTROL_STREAM)) {
            AtomicBoolean fenced = new AtomicBoolean();
            HaHeartbeat hb =
                    new HaHeartbeat(
                            embedded.aeronDirectoryName(),
                            HB_SEND,
                            HB_LISTEN,
                            2,
                            "standby",
                            () -> fenced.set(true));
            Thread t = new Thread(hb, "hb-test-standby");
            t.start();

            // standby 正常续租（epoch=1）
            ack(coord, 2, 1L);
            Thread.sleep(1_000L);
            assertFalse(fenced.get(), "standby 正常续租不得自杀");

            // 被提升（epoch=2）：必须接受新 epoch，不得误自杀
            ack(coord, 2, 2L);
            Thread.sleep(2_500L);
            assertFalse(fenced.get(), "standby 被提升后不得误自杀（它是新的 active）");
            hb.stop();
            t.join(2_000L);
        }
    }

    @Test
    void primarySurvivesStartupGraceWithoutAckThenFences() throws Exception {
        System.setProperty("ocx.ha.heartbeat.startup-grace-ms", "2000");
        try (OcxAeron embedded = OcxAeron.embedded()) {
            AtomicBoolean fenced = new AtomicBoolean();
            HaHeartbeat hb =
                    new HaHeartbeat(
                            embedded.aeronDirectoryName(),
                            HB_SEND,
                            HB_LISTEN,
                            1,
                            "primary",
                            () -> fenced.set(true));
            Thread t = new Thread(hb, "hb-test-grace");
            t.start();

            // 宽限期（2s）内无 ACK：不得自杀（编排/协调者晚就绪属正常）
            Thread.sleep(1_500L);
            assertFalse(fenced.get(), "启动宽限期内无 ACK 不得自杀");

            // 硬宽限兜底：协调者从未上线，宽限期后连续失联仍须 fail-stop
            long deadline = System.currentTimeMillis() + 10_000L;
            while (System.currentTimeMillis() < deadline && !fenced.get()) {
                Thread.sleep(100L);
            }
            assertTrue(fenced.get(), "宽限期后协调者仍失联必须 fail-stop（防永不自杀）");
            hb.stop();
            t.join(2_000L);
        } finally {
            System.clearProperty("ocx.ha.heartbeat.startup-grace-ms");
        }
    }

    @Test
    void primaryFencesPromptlyAfterFirstAckWhenCoordinatorLost() throws Exception {
        try (OcxAeron embedded = OcxAeron.embedded();
                Aeron coord =
                        Aeron.connect(
                                new Aeron.Context()
                                        .aeronDirectoryName(embedded.aeronDirectoryName()));
                Subscription hbSub =
                        coord.addSubscription(ACK_LISTEN, HaControlStream.HA_CONTROL_STREAM)) {
            AtomicBoolean fenced = new AtomicBoolean();
            HaHeartbeat hb =
                    new HaHeartbeat(
                            embedded.aeronDirectoryName(),
                            HB_SEND,
                            HB_LISTEN,
                            1,
                            "primary",
                            () -> fenced.set(true));
            Thread t = new Thread(hb, "hb-test-first-ack");
            t.start();

            // 收到首个 ACK（确认自己是 active）后，宽限期立即失效
            ack(coord, 1, 1L);
            Thread.sleep(1_500L);
            assertFalse(fenced.get(), "收到 ACK 后正常续租不得自杀");

            // 之后协调者失联：必须按 3 次失联快速 fail-stop（不受启动宽限保护）
            long deadline = System.currentTimeMillis() + 10_000L;
            while (System.currentTimeMillis() < deadline && !fenced.get()) {
                Thread.sleep(100L);
            }
            assertTrue(fenced.get(), "收到首个 ACK 后失联必须快速 fail-stop（防双主）");
            hb.stop();
            t.join(2_000L);
        }
    }

    private static void ack(Aeron coord, int instanceId, long epoch) {
        HaControlStream.publish(
                coord, ACK_SEND, HaControlStream.TYPE_LEASE_ACK, instanceId, "ack", epoch, 2_000L);
    }
}
