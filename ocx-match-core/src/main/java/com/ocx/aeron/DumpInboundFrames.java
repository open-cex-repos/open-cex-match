package com.ocx.aeron;

import com.ocx.codec.IoCodec;
import com.ocx.codec.domain.OrderIn;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;

/**
 * 诊断工具：回放指定入站 recording，逐帧解码打印 OrderIn 字段
 * （对比网关帧与 producer 帧，定位帧内容差异）。
 * 用法: DumpInboundFrames <aeronDir> <archiveControl> <recordingId> <frames>
 */
public final class DumpInboundFrames {

    private static final int REPLAY_STREAM = 455_000;

    private DumpInboundFrames() {}

    public static void main(String[] args) throws Exception {
        String aeronDir = args[0];
        String archiveControl = args[1];
        long recordingId = Long.parseLong(args[2]);
        int frames = Integer.parseInt(args[3]);

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
                AeronArchive archive =
                        AeronArchive.connect(
                                new AeronArchive.Context()
                                        .aeronDirectoryName(aeronDir)
                                        .controlRequestChannel(archiveControl)
                                        .controlResponseChannel("aeron:udp?endpoint=localhost:0"));
                Subscription sub = aeron.addSubscription("aeron:ipc", REPLAY_STREAM)) {
            archive.startReplay(
                    recordingId, 0L, AeronArchive.NULL_POSITION, "aeron:ipc", REPLAY_STREAM);
            int[] seen = {0};
            long deadline = System.currentTimeMillis() + 10_000L;
            while (seen[0] < frames && System.currentTimeMillis() < deadline) {
                sub.poll(
                        (buffer, offset, length, header) -> {
                            if (length < AeronWire.INBOUND_TS_LENGTH
                                    || length > AeronWire.INBOUND_ROW_SIZE) {
                                return;
                            }
                            byte[] frame = new byte[AeronWire.INBOUND_FRAME_LENGTH];
                            buffer.getBytes(
                                    offset + AeronWire.INBOUND_TS_LENGTH,
                                    frame,
                                    0,
                                    length - AeronWire.INBOUND_TS_LENGTH);
                            try {
                                OrderIn order = IoCodec.decodeOrderIn(frame);
                                System.out.println(
                                        "FRAME#"
                                                + seen[0]
                                                + " id="
                                                + order.id()
                                                + " userId="
                                                + order.userId()
                                                + " symbol="
                                                + order.symbol()
                                                + " side="
                                                + order.side()
                                                + " type="
                                                + order.type()
                                                + " tif="
                                                + order.timeInForce()
                                                + " price="
                                                + order.price()
                                                + " qty="
                                                + order.origQty()
                                                + " visible="
                                                + order.visibleQty()
                                                + " status="
                                                + order.status());
                            } catch (Exception e) {
                                System.out.println("FRAME#" + seen[0] + " DECODE_FAIL " + e);
                            }
                            seen[0]++;
                        },
                        16);
                Thread.sleep(20L);
            }
            System.out.println("TOTAL_DECODED=" + seen[0]);
        }
    }
}
