package com.ocx.egress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.ocx.aeron.AeronWire;
import com.ocx.aeron.OcxAeron;
import com.ocx.aeron.StreamIds;
import com.ocx.book.OrderBook;
import com.ocx.book.TriggerSlots;
import com.ocx.codec.IoCodec;
import com.ocx.codec.domain.OrderIn;
import com.ocx.codec.sbe.wal.MessageHeaderEncoder;
import com.ocx.codec.sbe.wal.OrderCreateEncoder;
import com.ocx.engine.MatchService;
import com.ocx.handler.OrderEventHandler;
import com.ocx.math.Decimal;
import com.ocx.recovery.MutableRecoveryConfig;
import com.ocx.recovery.RecoveryService;
import com.ocx.recovery.RecoveryService.WalRecoveryStats;
import com.ocx.snapshot.OrderRecordCodec;
import com.ocx.snapshot.SnapshotCodec;
import com.ocx.snapshot.SnapshotOrderFields;
import com.ocx.snapshot.TriggerSnapshotFields;

import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 输入 journal 恢复（2026-08-29，对齐 LMAX/Aeron）：入站 OrderIn 帧 → Archive recording
 * → RecoveryService 回放入站、重跑完整撮合重建簿状态（不再有状态变更 WAL）。
 */
class WalArchiveRecoveryTest {

    private static final int SCALE = 8;
    private static final long TS = 1787310000000L;

    private MutableRecoveryConfig config;

    @BeforeEach
    void setUp(@TempDir File tempDir) {
        config = new MutableRecoveryConfig();
        config.setEnabled(true);
        config.setDataDir(tempDir.getAbsolutePath());
    }

    private static long p(double v) {
        return Math.round(v * Math.pow(10, SCALE));
    }

    /**
     * 入站发布上下文：发布 OrderIn（→ SBE 帧 → 定长行 → inbound publication），并暴露
     * recording 的实时字节位置（快照锚点直接取 recorder 位置，不做行数→字节换算）。
     */
    private interface IoContext {
        void order(long id, long userId, String price, String qty, int side, int status);

        /** 条件单：type = triggerType+3（3=STOP_LIMIT … 6=TAKE_PROFIT_MARKET）；limitPrice 为 null 表示 MARKET 类。 */
        void orderStop(
                long id,
                long userId,
                String triggerPrice,
                String limitPrice,
                String qty,
                int side,
                int triggerType);

        long recordingId();

        /** recorder 当前字节位置（与入站流 position 同空间）。 */
        long recordingPosition();

        /** 等待 recorder 追平已发布行（位置连续两次采样稳定）。 */
        void awaitStable() throws InterruptedException;

        /**
         * 发布一份快照 recording（META→ORDER/STOP 批量帧→END，独立 snapshot 流；
         * includeEnd=false 模拟写快照中途崩溃截断）。
         */
        void snapshot(
                long anchorRecordingId,
                long anchorPosition,
                List<SnapshotOrderFields> orders,
                List<TriggerSnapshotFields> triggers,
                boolean includeEnd)
                throws Exception;
    }

    private interface IoSequence {
        void apply(IoContext ctx) throws Exception;
    }

    private static final class RecoveryResult {
        final WalRecoveryStats stats;
        final OrderBook book;
        final TriggerSlots triggers;

        RecoveryResult(WalRecoveryStats stats, OrderBook book, TriggerSlots triggers) {
            this.stats = stats;
            this.book = book;
            this.triggers = triggers;
        }
    }

    /** 内嵌媒体驱动 + 内嵌 Archive：记录入站流 → 发布输入帧 → 回放恢复。 */
    private RecoveryResult publishAndRecover(IoSequence seq) throws Exception {
        try (OcxAeron aeron = OcxAeron.embedded();
                Archive archive =
                        Archive.launch(
                                new Archive.Context()
                                        .aeronDirectoryName(aeron.aeronDirectoryName())
                                        .archiveDir(new File(config.getDataDir(), "archive"))
                                        .controlChannel(config.getArchiveControlChannel())
                                        .replicationChannel("aeron:udp?endpoint=localhost:8011")
                                        .recordingEventsEnabled(false))) {
            String symbol = "BTCUSDT";
            // LOCAL recording 需要本地发布端已存在：先建 inbound publication 再 startRecording
            try (Publication inPub = aeron.addInboundPublication(symbol);
                    AeronArchive client =
                            AeronArchive.connect(
                                    new AeronArchive.Context()
                                            .aeronDirectoryName(aeron.aeronDirectoryName())
                                            .controlRequestChannel(
                                                    config.getArchiveControlChannel())
                                            .controlResponseChannel(
                                                    config.getArchiveControlResponseChannel()))) {
                client.startRecording(
                        OcxAeron.CHANNEL, StreamIds.inbound(symbol), SourceLocation.LOCAL);
                long recordingId = awaitInboundRecording(client, symbol);

                IoContext ctx =
                        new IoContext() {
                            @Override
                            public void order(
                                    long id,
                                    long userId,
                                    String price,
                                    String qty,
                                    int side,
                                    int status) {
                                OrderIn o =
                                        new OrderIn(
                                                id,
                                                userId,
                                                symbol,
                                                Decimal.fromBigDecimal(new BigDecimal(price)),
                                                Decimal.fromBigDecimal(new BigDecimal(qty)),
                                                null,
                                                null,
                                                (byte) side,
                                                (byte) 1,
                                                "GTC",
                                                (byte) status,
                                                LocalDateTime.of(2026, 8, 29, 0, 0, 0),
                                                null);
                                byte[] frame = IoCodec.encodeOrderIn(o);
                                UnsafeBuffer row =
                                        new UnsafeBuffer(new byte[AeronWire.INBOUND_ROW_SIZE]);
                                AeronWire.encodeInboundFrame(
                                        row, new UnsafeBuffer(frame), 0, frame.length, TS);
                                long result;
                                while ((result = inPub.offer(row, 0, AeronWire.INBOUND_ROW_SIZE))
                                        < 0) {
                                    Thread.onSpinWait();
                                }
                            }

                            @Override
                            public void orderStop(
                                    long id,
                                    long userId,
                                    String triggerPrice,
                                    String limitPrice,
                                    String qty,
                                    int side,
                                    int triggerType) {
                                OrderIn o =
                                        new OrderIn(
                                                id,
                                                userId,
                                                symbol,
                                                limitPrice == null
                                                        ? null
                                                        : Decimal.fromBigDecimal(
                                                                new BigDecimal(limitPrice)),
                                                Decimal.fromBigDecimal(new BigDecimal(qty)),
                                                null,
                                                Decimal.fromBigDecimal(
                                                        new BigDecimal(triggerPrice)),
                                                (byte) side,
                                                (byte) (triggerType + 3),
                                                "GTC",
                                                (byte) 0,
                                                LocalDateTime.of(2026, 8, 29, 0, 0, 0),
                                                null);
                                byte[] frame = IoCodec.encodeOrderIn(o);
                                UnsafeBuffer row =
                                        new UnsafeBuffer(new byte[AeronWire.INBOUND_ROW_SIZE]);
                                AeronWire.encodeInboundFrame(
                                        row, new UnsafeBuffer(frame), 0, frame.length, TS);
                                long result;
                                while ((result = inPub.offer(row, 0, AeronWire.INBOUND_ROW_SIZE))
                                        < 0) {
                                    Thread.onSpinWait();
                                }
                            }

                            @Override
                            public long recordingId() {
                                return recordingId;
                            }

                            @Override
                            public long recordingPosition() {
                                return client.getRecordingPosition(recordingId);
                            }

                            @Override
                            public void awaitStable() throws InterruptedException {
                                awaitRecordingStable(client, recordingId);
                            }

                            @Override
                            public void snapshot(
                                    long anchorRecordingId,
                                    long anchorPosition,
                                    List<SnapshotOrderFields> orders,
                                    List<TriggerSnapshotFields> triggers,
                                    boolean includeEnd)
                                    throws Exception {
                                publishSnapshot(
                                        aeron,
                                        client,
                                        symbol,
                                        anchorRecordingId,
                                        anchorPosition,
                                        orders,
                                        triggers,
                                        includeEnd);
                            }
                        };
                seq.apply(ctx);
                awaitRecordingStable(client, recordingId);
            }

            // 恢复：空簿 + 回放处理器（fillSink/wal/eventSink 全空，输出抑制）
            OrderBook book = new OrderBook(symbol, SCALE, SCALE, 1024, 256, 100_000L);
            TriggerSlots triggers = new TriggerSlots(256, 64);
            OrderEventHandler handler =
                    new OrderEventHandler(
                            book, triggers, new MatchService(false), null, null, null);
            WalRecoveryStats stats =
                    new RecoveryService()
                            .recover(config, aeron.aeronDirectoryName(), Map.of(symbol, handler));
            return new RecoveryResult(stats, book, triggers);
        }
    }

    /** recording 异步建立：轮询 catalog 找到入站流的 recordingId。 */
    private static long awaitInboundRecording(AeronArchive client, String symbol)
            throws InterruptedException {
        int streamId = StreamIds.inbound(symbol);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            long[] found = {-1L};
            client.listRecordings(
                    0L,
                    Integer.MAX_VALUE,
                    (cs,
                            corr,
                            recordingId,
                            startTs,
                            triggerTs,
                            startPos,
                            stopPos,
                            it,
                            sfl,
                            tbl,
                            mtu,
                            sid,
                            streamId2,
                            sc,
                            oc,
                            si) -> {
                        if (streamId2 == streamId) {
                            found[0] = recordingId;
                        }
                    });
            if (found[0] >= 0L) {
                return found[0];
            }
            Thread.sleep(50);
        }
        fail("入站 recording 未建立: stream=" + streamId);
        return -1L;
    }

    /** 等待 recording 位置稳定（发布后归档线程追平）。 */
    private static void awaitRecordingStable(AeronArchive client, long recordingId)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        long last = -1L;
        int stable = 0;
        while (System.nanoTime() < deadline) {
            long pos = client.getRecordingPosition(recordingId);
            if (pos == last && pos > 0L && ++stable >= 2) {
                return;
            }
            if (pos != last) {
                stable = 0;
            }
            last = pos;
            Thread.sleep(100);
        }
        fail("recording 位置未稳定: pos=" + client.getRecordingPosition(recordingId));
    }

    /**
     * 发布一份快照 recording（2026-08-30，Aeron recording 快照；与生产 SnapshotWriter
     * 同一编解码契约）：META → ORDER/STOP 批量帧 → END（includeEnd=false 时省略 END，
     * 模拟截断）。每份快照独立 start/stop recording，恢复端按 recordingId 选最新有效份。
     */
    private static void publishSnapshot(
            OcxAeron aeron,
            AeronArchive client,
            String symbol,
            long anchorRecordingId,
            long anchorPosition,
            List<SnapshotOrderFields> orders,
            List<TriggerSnapshotFields> triggers,
            boolean includeEnd)
            throws Exception {
        try (Publication snapPub = aeron.addSnapshotPublication(symbol)) {
            int snapStream = StreamIds.snapshot(symbol);
            client.startRecording(OcxAeron.CHANNEL, snapStream, SourceLocation.LOCAL);
            // startRecording 返回值非 recording id：按流解析最新 recording 并等待激活
            long recordingId = awaitActiveRecording(client, snapStream);
            // recorder 激活后发布，避免起始帧落在 recorder 附着前
            UnsafeBuffer batch = new UnsafeBuffer(new byte[SnapshotCodec.MAX_BATCH_BYTES]);
            UnsafeBuffer rec =
                    new UnsafeBuffer(
                            new byte
                                    [OrderRecordCodec.MAX_ENCODED_LENGTH
                                            + OrderRecordCodec.MAGIC_LEN]);
            byte[] strScratch =
                    new byte[OrderRecordCodec.MAX_ENCODED_LENGTH + OrderRecordCodec.MAGIC_LEN];
            MessageHeaderEncoder header = new MessageHeaderEncoder();
            OrderCreateEncoder enc = new OrderCreateEncoder();

            SnapshotCodec.SnapshotMeta meta =
                    new SnapshotCodec.SnapshotMeta(
                            symbol,
                            TS,
                            SCALE,
                            SCALE,
                            orders.size(),
                            triggers.size(),
                            anchorRecordingId,
                            anchorPosition);
            int len = SnapshotCodec.encodeMeta(batch, 0, meta);
            while (snapPub.offer(batch, 0, len) < 0L) {
                Thread.onSpinWait();
            }
            publishBatches(
                    snapPub,
                    batch,
                    rec,
                    strScratch,
                    header,
                    enc,
                    symbol,
                    SnapshotCodec.TYPE_ORDER_BATCH,
                    orders.size(),
                    i -> {
                        SnapshotOrderFields f = orders.get(i);
                        return OrderRecordCodec.encodeFields(
                                symbol,
                                f.orderId,
                                f.userId,
                                f.price,
                                f.origQty,
                                f.direction,
                                f.tif,
                                f.time,
                                f.executedQty,
                                f.cumQuoteQty,
                                rec,
                                0,
                                strScratch,
                                header,
                                enc);
                    });
            publishBatches(
                    snapPub,
                    batch,
                    rec,
                    strScratch,
                    header,
                    enc,
                    symbol,
                    SnapshotCodec.TYPE_STOP_BATCH,
                    triggers.size(),
                    i -> {
                        TriggerSnapshotFields f = triggers.get(i);
                        return OrderRecordCodec.encodeStopFields(
                                symbol,
                                f.orderId(),
                                f.userId(),
                                f.triggerPrice(),
                                f.price(),
                                f.origQty(),
                                f.direction(),
                                f.triggerType(),
                                f.trailDelta(),
                                f.time(),
                                rec,
                                0,
                                strScratch,
                                header,
                                enc);
                    });
            if (includeEnd) {
                len = SnapshotCodec.encodeEnd(batch, 0, orders.size(), triggers.size());
                while (snapPub.offer(batch, 0, len) < 0L) {
                    Thread.onSpinWait();
                }
            }
            // 本版本 stopRecording(long) 按 subscriptionId 停录：快照流每次仅一条活跃
            // recording，按 channel+stream 停录等价且精准。
            client.stopRecording(OcxAeron.CHANNEL, snapStream);
        }
    }

    /** 解析流上最新 recording 并等待 recorder 激活（未附着时 getRecordingPosition 抛 unknown）。 */
    private static long awaitActiveRecording(AeronArchive client, int streamId) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            long[] found = {-1L};
            client.listRecordings(
                    0L,
                    Integer.MAX_VALUE,
                    (cs,
                            corr,
                            recordingId,
                            startTs,
                            triggerTs,
                            startPos,
                            stopPos,
                            it,
                            sfl,
                            tbl,
                            mtu,
                            sid,
                            streamId2,
                            sc,
                            oc,
                            si) -> {
                        if (streamId2 == streamId && recordingId > found[0]) {
                            found[0] = recordingId;
                        }
                    });
            if (found[0] >= 0L) {
                try {
                    if (client.getRecordingPosition(found[0]) != AeronArchive.NULL_POSITION) {
                        return found[0];
                    }
                } catch (io.aeron.archive.client.ArchiveException ignored) {
                    // recorder 尚未附着
                }
            }
            Thread.sleep(5);
        }
        fail("snapshot recorder 未激活: stream=" + streamId);
        return -1L;
    }

    private interface RecordEncoder {
        int encode(int i);
    }

    private static void publishBatches(
            Publication snapPub,
            UnsafeBuffer batch,
            UnsafeBuffer rec,
            byte[] strScratch,
            MessageHeaderEncoder header,
            OrderCreateEncoder enc,
            String symbol,
            int type,
            int count,
            RecordEncoder encoder) {
        int batchCount = 0;
        int len = SnapshotCodec.encodeBatchHeader(batch, 0, type, 0);
        for (int i = 0; i < count; i++) {
            int recLen = encoder.encode(i);
            int next = SnapshotCodec.appendRecord(batch, 0, len, rec, 0, recLen);
            if (next < 0) {
                SnapshotCodec.encodeBatchHeader(batch, 0, type, batchCount);
                while (snapPub.offer(batch, 0, len) < 0L) {
                    Thread.onSpinWait();
                }
                len = SnapshotCodec.encodeBatchHeader(batch, 0, type, 0);
                batchCount = 0;
                next = SnapshotCodec.appendRecord(batch, 0, len, rec, 0, recLen);
            }
            len = next;
            batchCount++;
        }
        if (batchCount > 0) {
            SnapshotCodec.encodeBatchHeader(batch, 0, type, batchCount);
            while (snapPub.offer(batch, 0, len) < 0L) {
                Thread.onSpinWait();
            }
        }
    }

    @Test
    void createCancelRecoversFromInboundRecording() throws Exception {
        RecoveryResult r =
                publishAndRecover(
                        ctx -> {
                            ctx.order(1L, 10L, "50000", "2.0", 0, 0);
                            ctx.order(2L, 20L, "50100", "1.0", 1, 0);
                            ctx.order(2L, 20L, "50100", "1.0", 1, 2); // 撤单 status=2
                        });
        assertEquals(3, r.stats.records, "3 帧入站全部回放");

        // 订单 1 在簿（buy 50000 qty2，50000<50100 不成交）；订单 2 已撤
        // （orderId 重构后槽位由 freelist 分配，按 orderId 扫槽而非硬编码槽 1）
        int slot1 = slotOf(r.book, 1L);
        assertEquals(1L, r.book.slots.orderIds[slot1]);
        assertEquals(p(2.0), r.book.slots.origQtys[slot1]);
        assertEquals(p(50000), r.book.slots.prices[slot1]);
        assertFalse(r.book.slots.isOccupied(2), "订单 2 已撤");
        assertEquals(1, r.book.openOrderCount());
    }

    @Test
    void crossingOrdersMatchAndLeaveBook() throws Exception {
        RecoveryResult r =
                publishAndRecover(
                        ctx -> {
                            ctx.order(1L, 10L, "50000", "1.0", 0, 0); // buy 50000
                            ctx.order(2L, 20L, "50000", "1.0", 1, 0); // sell 50000 交叉 → 全成交
                        });
        assertEquals(2, r.stats.records);
        assertFalse(r.book.slots.isOccupied(1), "buy 全成交移出簿");
        assertFalse(r.book.slots.isOccupied(2), "sell 全成交移出簿");
        assertEquals(0, r.book.openOrderCount());
        assertTrue(r.book.priceLevelSize(0) == 0, "买档清空");
    }

    @Test
    void snapshotPrefillThenIncrementalReplay() throws Exception {
        // 先灌入簿（50000 buy / 50100 sell 不成交），写快照（锚点 = 前两行），再追加撤单
        RecoveryResult r =
                publishAndRecover(
                        ctx -> {
                            ctx.order(1L, 10L, "50000", "2.0", 0, 0);
                            ctx.order(2L, 20L, "50100", "1.0", 1, 0);
                            // 快照锚点 = recorder 追平前两行后的真实字节位置（与 image 位置同空间）
                            ctx.awaitStable();
                            long anchor = ctx.recordingPosition();
                            List<SnapshotOrderFields> orders = new ArrayList<>();
                            SnapshotOrderFields f = new SnapshotOrderFields();
                            f.orderId = 1L;
                            f.userId = 10L;
                            f.symbol = "BTCUSDT";
                            f.price = p(50000);
                            f.priceExponent = 0;
                            f.origQty = p(2.0);
                            f.origQtyExponent = 0;
                            f.direction = 0;
                            f.tif = 0;
                            f.time = TS;
                            orders.add(f);
                            SnapshotOrderFields f2 = new SnapshotOrderFields();
                            f2.orderId = 2L;
                            f2.userId = 20L;
                            f2.symbol = "BTCUSDT";
                            f2.price = p(50100);
                            f2.priceExponent = 0;
                            f2.origQty = p(1.0);
                            f2.origQtyExponent = 0;
                            f2.direction = 1;
                            f2.tif = 0;
                            f2.time = TS;
                            orders.add(f2);
                            ctx.snapshot(ctx.recordingId(), anchor, orders, List.of(), true);
                            // 快照后的增量：撤单 2（第 3 行）
                            ctx.order(2L, 20L, "50100", "1.0", 1, 2);
                        });
        // 快照预填 2 单 + 增量回放 1 帧撤单 → 订单 2 被撤，订单 1 在簿
        assertFalse(r.book.slots.isOccupied(2), "快照后增量撤单生效");
        assertEquals(1L, r.book.slots.orderIds[slotOf(r.book, 1L)]);
        assertEquals(1, r.book.openOrderCount());
    }

    @Test
    void snapshotPrefillRestoresStopOrders() throws Exception {
        RecoveryResult r =
                publishAndRecover(
                        ctx -> {
                            ctx.order(1L, 10L, "50000", "2.0", 0, 0); // 普通单进簿
                            ctx.orderStop(2L, 20L, "50100", "50200", "1.0", 0, 0); // STOP_LIMIT BUY
                            ctx.orderStop(3L, 30L, "49900", null, "2.0", 1, 1); // STOP_MARKET SELL
                            ctx.awaitStable();
                            long anchor = ctx.recordingPosition();
                            List<SnapshotOrderFields> orders = new ArrayList<>();
                            SnapshotOrderFields f = new SnapshotOrderFields();
                            f.orderId = 1L;
                            f.userId = 10L;
                            f.symbol = "BTCUSDT";
                            f.price = p(50000);
                            f.priceExponent = 0;
                            f.origQty = p(2.0);
                            f.origQtyExponent = 0;
                            f.direction = 0;
                            f.tif = 0;
                            f.time = TS;
                            orders.add(f);
                            List<TriggerSnapshotFields> triggers =
                                    List.of(
                                            new TriggerSnapshotFields(
                                                    2L,
                                                    20L,
                                                    p(50100),
                                                    p(50200),
                                                    p(1.0),
                                                    0,
                                                    TriggerSlots.TYPE_STOP_LIMIT,
                                                    0L,
                                                    TS),
                                            new TriggerSnapshotFields(
                                                    3L,
                                                    30L,
                                                    p(49900),
                                                    0L,
                                                    p(2.0),
                                                    1,
                                                    TriggerSlots.TYPE_STOP_MARKET,
                                                    0L,
                                                    TS));
                            ctx.snapshot(ctx.recordingId(), anchor, orders, triggers, true);
                        });

        // 普通单 + 2 条件单全部从快照恢复
        assertEquals(1, r.book.openOrderCount());
        assertEquals(2, r.triggers.orderCount(), "条件单应从快照恢复挂载");
        assertTrue(r.triggers.orderIds[0] == 2L, "STOP_LIMIT BUY 槽位");
        assertTrue(r.triggers.orderIds[1] == 3L, "STOP_MARKET SELL 槽位");
        assertTrue(r.triggers.triggerPrices[0] == p(50100));
        assertTrue(r.triggers.prices[0] == p(50200));
        assertTrue(r.triggers.triggerPrices[1] == p(49900));

        // 恢复后的条件单可正常触发：BUY stop 触发价 50100，lastPrice 50150 跨过
        int[] out = new int[16];
        assertEquals(1, r.triggers.checkAndTrigger(p(50150), out));
        int slot = out[0];
        assertTrue(slot >= 0);
        assertTrue(r.triggers.orderIds[slot] == 2L);
        assertEquals(1, r.triggers.orderCount(), "触发摘除后剩 1 条");
    }

    /** 最新快照截断（无 END）时恢复必须回退上一有效快照，不得静默使用残缺簿。 */
    @Test
    void recoveryFallsBackWhenNewestSnapshotIncomplete() throws Exception {
        RecoveryResult r =
                publishAndRecover(
                        ctx -> {
                            ctx.order(1L, 10L, "50000", "2.0", 0, 0);
                            ctx.order(2L, 20L, "50100", "1.0", 1, 0);
                            ctx.awaitStable();
                            long anchor = ctx.recordingPosition();
                            List<SnapshotOrderFields> orders = new ArrayList<>();
                            SnapshotOrderFields f = new SnapshotOrderFields();
                            f.orderId = 1L;
                            f.userId = 10L;
                            f.symbol = "BTCUSDT";
                            f.price = p(50000);
                            f.priceExponent = 0;
                            f.origQty = p(2.0);
                            f.origQtyExponent = 0;
                            f.direction = 0;
                            f.tif = 0;
                            f.time = TS;
                            orders.add(f);
                            SnapshotOrderFields f2 = new SnapshotOrderFields();
                            f2.orderId = 2L;
                            f2.userId = 20L;
                            f2.symbol = "BTCUSDT";
                            f2.price = p(50100);
                            f2.priceExponent = 0;
                            f2.origQty = p(1.0);
                            f2.origQtyExponent = 0;
                            f2.direction = 1;
                            f2.tif = 0;
                            f2.time = TS;
                            orders.add(f2);
                            // 有效快照 A（END 完整）
                            ctx.snapshot(ctx.recordingId(), anchor, orders, List.of(), true);
                            // 截断快照 B（无 END）——按 recordingId 更新，但校验失败，应回退 A
                            ctx.snapshot(ctx.recordingId(), anchor, orders, List.of(), false);
                        });

        assertEquals(2, r.book.openOrderCount(), "应回退到有效快照 A");
        assertEquals(1L, r.book.slots.orderIds[slotOf(r.book, 1L)], "订单 1 从快照恢复");
        assertEquals(2L, r.book.slots.orderIds[slotOf(r.book, 2L)], "订单 2 从快照恢复");
    }

    /** orderId → 槽位（orderId 重构后槽位由 freelist 分配，不再等于 orderId）。 */
    private static int slotOf(OrderBook book, long orderId) {
        for (int i = 0; i < book.slots.orderIds.length; i++) {
            if (book.slots.orderIds[i] == orderId && book.slots.isOccupied(i)) {
                return i;
            }
        }
        return -1;
    }
}
