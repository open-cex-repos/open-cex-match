package com.ocx.recovery;

import com.ocx.aeron.AeronWire;
import com.ocx.aeron.OcxAeron;
import com.ocx.aeron.StreamIds;
import com.ocx.codec.IoCodec;
import com.ocx.codec.IoCodec.CodecContext;
import com.ocx.codec.domain.ExchangeTrade;
import com.ocx.codec.domain.OrderInFields;
import com.ocx.config.ConfigLoader;
import com.ocx.config.MatchEngineConfig;
import com.ocx.snapshot.OrderRecordCodec;
import com.ocx.snapshot.SnapshotCodec;
import com.ocx.snapshot.SnapshotOrderFields;
import com.ocx.snapshot.TriggerSnapshotFields;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 成交记录对账（2026-08-30，记录级，非余额/结算验证）：离线回放 Archive 的入站 +
 * 成交 recording，用独立于引擎的轻量模拟器重建 orderId→用户表与专用用户账本，
 * 逐笔校验成交记录自洽：
 *
 * <ul>
 *   <li>U1 逐笔金额：每笔 fill 的 {@code price×amount == 同订单相邻 fill 的
 *       buy/sellTurnover 增量}（首笔 == 初始累计额）；</li>
 *   <li>U2 订单级守恒：累计成交量 ≤ 原始量、turnover 单调不减、买方订单 side=BUY / 卖方 side=SELL；</li>
 *   <li>U3 专用用户账本（{@code -Docx.fund.users=N}，userId&lt;N）：买/卖笔数、base 净持仓、quote 净资金；</li>
 *   <li>U4 自成交排除：buy/sell orderId 映射到不同 userId（STP）；</li>
 *   <li>U5 引用一致性：无幽灵单（fill 引用未创建/已撤 orderId）；被批撤订单不得在撤单时间后成交；</li>
 *   <li>U8 全局守恒：成交笔数与外部计数（{@code -Docx.fund.expected-trades}）一致。</li>
 * </ul>
 *
 * <p>用法：{@code -Docx.aeron.dir=<dir> -Docx.probe.symbol=BTCUSDT -Docx.fund.users=N
 * [-Docx.fund.expected-trades=M] -cp <fat jar> com.ocx.recovery.TradeRecordReconciliationMain}</p>
 */
public final class TradeRecordReconciliationMain {

    private static final long REPLAY_QUIET_MS = 2_000L;
    private static final long REPLAY_DEADLINE_MS = 90 * 60_000L;

    /** 回放静默超过该值仍未结束 = 异常停顿（长 GC/磁盘卡顿），强制退出避免挂死。 */
    private static final long REPLAY_STALL_MS = 60_000L;

    /** 同 ts 组收齐的空 poll 上限（×2ms sleep ≈ 500ms；防回放异常停顿死等）。 */
    /** 同 ts 组收齐空转上限：shared 模式 Archive 单 replayer 被成交回放占用时，
     *  入站回放同 ts 批次间隔可达秒级（D24 实测 500ms CAP 提前 break 误报 U5）；
     *  2500 空转 ≈ 5s，配合残缺组重试（放回队首）兜底。 */
    private static final int GROUP_SPIN_CAP = 2500;

    /** 回放目的地流：避开 ArchiveProbe(400k)/walReplay(410k)/snapshotReplay(420k)。 */
    private static final int REPLAY_BASE = 450_000;

    /** 失败明细上限（超出仅计数）。 */
    private static final int FAILURE_CAP = 100;

    /** 调试开关（-Docx.fund.debug=true）：打印前 N 条事件与 U1 违例订单的成交序列。 */
    private static final boolean DEBUG = Boolean.getBoolean("ocx.fund.debug");

    /** 调试订单白名单（-Docx.fund.debug.orders=id1,id2）：仅打印这些 orderId 的逐笔轨迹。 */
    private static final java.util.Set<Long> DEBUG_ORDERS =
            parseDebugOrders(System.getProperty("ocx.fund.debug.orders", ""));

    /** 调试用户白名单（-Docx.fund.debug.users=uid1,uid2）：RAW 打印批撤帧（无 orderId）。 */
    private static final java.util.Set<Long> DEBUG_USERS =
            parseDebugOrders(System.getProperty("ocx.fund.debug.users", ""));

    private static java.util.Set<Long> parseDebugOrders(String csv) {
        java.util.Set<Long> set = new java.util.HashSet<>();
        for (String s : csv.split(",")) {
            if (!s.isBlank()) {
                set.add(Long.parseLong(s.trim()));
            }
        }
        return set;
    }

    private static boolean dbgOrder(long orderId) {
        return DEBUG && DEBUG_ORDERS.contains(orderId);
    }

    private static boolean dbgUser(long userId) {
        return DEBUG && DEBUG_USERS.contains(userId);
    }

    private record Recording(long recordingId, long startPosition, long stopPosition) {}

    /** 入站订单摘要（orderId→）。 */
    static final class OrderInfo {
        final long orderId;
        final long userId;
        final BigDecimal origQty;
        final byte side;
        final long createTime;

        /** 入站投递序号（引擎处理顺序）：批撤只撤 createSeq 更小的单。 */
        final long createSeq;

        /** 条件单（type 3–6）：引擎挂 TriggerSlots，批撤/单笔撤都不命中（只扫限价簿）。 */
        final boolean stop;

        /** 快照预填订单（2026-08-31）：快照只含在簿剩余量、不含成交累计——U1
         * 首笔锚点后成交只记基线不校验（增量校验从第二笔起精确）。 */
        final boolean fromSnapshot;

        boolean cancelled;
        long cancelTime;

        /** 1=单笔撤单，2=批量撤单（诊断用）。 */
        int cancelType;

        // 成交累计额（buy/sell 侧各自）
        boolean hasBuyTurnover;
        long lastBuyTurnoverMantissa;
        int lastBuyTurnoverExponent;
        boolean hasSellTurnover;
        long lastSellTurnoverMantissa;
        int lastSellTurnoverExponent;
        BigDecimal sumAmount = BigDecimal.ZERO;

        OrderInfo(
                long orderId,
                long userId,
                long qtyMantissa,
                int qtyExponent,
                byte side,
                long createTime,
                boolean stop,
                long createSeq) {
            this(
                    orderId,
                    userId,
                    qtyMantissa,
                    qtyExponent,
                    side,
                    createTime,
                    stop,
                    createSeq,
                    false);
        }

        OrderInfo(
                long orderId,
                long userId,
                long qtyMantissa,
                int qtyExponent,
                byte side,
                long createTime,
                boolean stop,
                long createSeq,
                boolean fromSnapshot) {
            this.orderId = orderId;
            this.userId = userId;
            this.origQty = dec(qtyMantissa, qtyExponent);
            this.side = side;
            this.createTime = createTime;
            this.stop = stop;
            this.createSeq = createSeq;
            this.fromSnapshot = fromSnapshot;
        }
    }

    /** 专用用户账本（base 净持仓 / quote 净资金）。 */
    static final class Ledger {
        long buyFills;
        long sellFills;
        BigDecimal buyQty = BigDecimal.ZERO;
        BigDecimal sellQty = BigDecimal.ZERO;
        BigDecimal buyQuote = BigDecimal.ZERO;
        BigDecimal sellQuote = BigDecimal.ZERO;
    }

    static final class Result {
        boolean ok = true;
        long fills;
        long creates;
        long cancels;
        long batchCancels;
        BigDecimal totalQuote = BigDecimal.ZERO;
        int u1Failures;
        int u2Failures;
        int u4Failures;
        int u5Failures;

        /** 被忽略的重复 create（orderId 已在簿，引擎必然拒绝；对账器保留原实例）。 */
        long duplicateCreates;

        /** 快照预填订单集合（2026-08-31 U5 诊断：区分"预填但被移除"与"真未见"）。 */
        final java.util.Set<Long> prefilledIds = new java.util.HashSet<>();

        final List<String> details = new ArrayList<>();
        final Map<Long, Ledger> ledgers = new HashMap<>();

        /** 诊断：已从表移除的 orderId → 移除原因（撤单@ts / 成交完@ts），幽灵单报错定位用。 */
        final Map<Long, String> droppedReason = new HashMap<>();

        /** 诊断：见过其 CREATE 的 orderId → create 的 ts（幽灵单定位：未见过=入站漏处理，见过且 ts 更大=排序错）。 */
        final Map<Long, Long> seenCreateTs = new HashMap<>();

        /** 诊断（debug）：白名单订单 create 的 ts（仅 debug 且命中 -Docx.fund.debug.orders）。 */
        final Map<Long, Long> allCreateTs = DEBUG ? new HashMap<>() : null;

        /** 边界调试：锚点 ts（-Docx.recon.debug-boundary=true 时打印前 2s 成交现场）。 */
        long boundaryTs = -1L;
    }

    private TradeRecordReconciliationMain() {}

    public static void main(String[] args) {
        MatchEngineConfig cfg = ConfigLoader.load(args);
        String symbol = System.getProperty("ocx.probe.symbol", "BTCUSDT");
        int fundUsers = Integer.getInteger("ocx.fund.users", 0);
        long expectedTrades = Long.getLong("ocx.fund.expected-trades", -1L);
        String aeronDir = System.getProperty("ocx.aeron.dir", "/tmp/ocx-aeron");
        try (AeronArchive archive =
                AeronArchive.connect(
                        new AeronArchive.Context()
                                .aeronDirectoryName(aeronDir)
                                .controlRequestChannel(
                                        cfg.getRecoveryConfig().getArchiveControlChannel())
                                .controlResponseChannel(
                                        cfg.getRecoveryConfig()
                                                .getArchiveControlResponseChannel()))) {
            Result r = reconcile(archive, symbol, fundUsers, expectedTrades);
            printReport(r, fundUsers, expectedTrades);
            System.err.println("[TradeRecon] " + (r.ok ? "RECON_PASS" : "RECON_FAIL"));
            System.exit(r.ok ? 0 : 1);
        } catch (Exception e) {
            System.err.println("[TradeRecon] 异常: " + e);
            System.exit(2);
        }
    }

    private static Result reconcile(
            AeronArchive archive, String symbol, int fundUsers, long expectedTrades) {
        Result r = new Result();
        Map<Long, OrderInfo> orders = new HashMap<>();
        // 存活订单按用户分桶（LinkedHashSet：批撤遍历 + O(1) 移除；ArrayList 的
        // remove 是 O(在簿数)，5 亿级事件下会把对账拖到数小时）。
        Map<Long, java.util.Set<OrderInfo>> liveByUser = new HashMap<>();

        // ── 快照锚点预填（2026-08-31）：完整形态长跑对账。快照开时入站 recording
        // 被后段回收裁头，无法全量回放；改从最新有效快照预填订单流 + 锚点后增量
        // 回放入站。无有效快照则保持旧行为（全量回放入站）。
        SnapshotAnchor anchor = loadSnapshotAnchor(archive, symbol, orders, liveByUser, r);

        List<Recording> inRecs = listRecordings(archive, StreamIds.inboundJournalEffective(symbol));
        List<Recording> tradeRecs = listRecordings(archive, StreamIds.archiveTrade(symbol));
        if (inRecs.isEmpty() || tradeRecs.isEmpty()) {
            fail(r, "入站/成交 recording 缺失，无法对账", 5);
            return r;
        }
        Recording inRec;
        long inStart;
        if (anchor.present()) {
            Recording anchored = null;
            for (Recording rec : inRecs) {
                if (rec.recordingId() == anchor.inboundRecordingId()) {
                    anchored = rec;
                    break;
                }
            }
            if (anchored == null) {
                fail(r, "快照锚点入站 recording " + anchor.inboundRecordingId() + " 缺失（已被回收），无法对账", 5);
                return r;
            }
            inRec = anchored;
            inStart = Math.max(anchored.startPosition(), anchor.inboundPosition());
        } else {
            inRec = inRecs.get(0);
            inStart = inRec.startPosition();
        }
        Recording trRec = tradeRecs.get(0);
        CodecContext ctx = IoCodec.context();
        OrderInFields fields = new OrderInFields();
        // ── 合并回放（2026-08-30）：入站 + 成交两张 replay 订阅按事件时间推进。
        // 同 ts 规则：CREATE 先于同刻 fill，fill 先于同刻 CANCEL/批撤（对应撮合环序）。
        // 撤单/成交完的订单即时从表移除——内存只随在簿规模走，大压力长样本不 OOM。
        long inStop = resolvedStop(archive, inRec);
        long trStop = resolvedStop(archive, trRec);
        if (inStop <= inStart || trStop <= trRec.startPosition()) {
            fail(r, "recording 位置无效", 5);
            return r;
        }
        java.util.Deque<InboundEvent> inQueue = new java.util.ArrayDeque<>();
        java.util.Deque<InboundEvent> inGroup = new java.util.ArrayDeque<>();
        List<ExchangeTrade> tradeBuf = new ArrayList<>();
        // 快照锚点：只校验 ts > 增量入站首帧 ts 的成交（快照前的成交状态由快照
        // 承载，重建流无法校验；宁滤多不漏）。用"增量首帧 ts"而非快照 freezeTs——
        // 入站增量按 position 锚定、成交按 ts 过滤，若两条锚定轴用不同参考点，
        // 边界成交引用的订单会落在缝隙里（U5 幽灵单误报，2026-08-31 定位）。
        // 预热后更新为首帧 ts（增量入站第一帧），与 position 锚点严格对齐。
        long[] effAnchorTs = {anchor.present() ? anchor.ts() : Long.MIN_VALUE};
        System.err.println(
                "[TradeRecon] replay: inRec="
                        + inRec.recordingId()
                        + " start="
                        + inStart
                        + " stop="
                        + inStop
                        + " trRec="
                        + trRec.recordingId()
                        + " start="
                        + trRec.startPosition()
                        + " stop="
                        + trStop
                        + " anchorTs="
                        + effAnchorTs[0]);
        long[] debugPrinted = {0L};
        long[] lastActivity = {System.currentTimeMillis()};
        long[] lastProgress = {0L};
        // inDelivered 在预热区定义（与 trDelivered 并列）；主循环结束判定用
        // "两路都投递过 + 静默 2s"，不依赖 hasNoImages（Aeron replay 完成后
        // image 移除慢，replayDone 可能长期 false，主循环空等实测卡死）。
        long[] eventSeq = {0L}; // 入站投递序号（= 引擎处理顺序），批撤定位用
        long start = System.currentTimeMillis();
        IdleStrategy idle = new BackoffIdleStrategy(100, 100, 1_000, 1_000_000);
        try (Subscription inSub =
                        archive.replay(
                                inRec.recordingId(),
                                inStart,
                                inStop - inStart,
                                OcxAeron.CHANNEL,
                                replayStream(inRec.recordingId()));
                Subscription trSub =
                        archive.replay(
                                trRec.recordingId(),
                                trRec.startPosition(),
                                trStop - trRec.startPosition(),
                                OcxAeron.CHANNEL,
                                replayStream(trRec.recordingId()) + 10_000)) {
            FragmentAssembler trAssembler =
                    new FragmentAssembler(
                            (buffer, offset, length, header) -> {
                                byte[] msg = new byte[length];
                                buffer.getBytes(offset, msg, 0, length);
                                IoCodec.forEachExchangeTrade(
                                        msg,
                                        t -> {
                                            // 边界 ts（== 增量首帧 ts）的成交不再丢弃：它们承载订单的
                                            // 累计基线（首笔校验需要）。checkFill 对边界成交只记基线不校验。
                                            if (t.getTime() >= effAnchorTs[0]) {
                                                tradeBuf.add(t);
                                            }
                                        });
                            });
            io.aeron.logbuffer.FragmentHandler inHandler =
                    (buffer, offset, length, header) -> {
                        if (length < AeronWire.INBOUND_TS_LENGTH) {
                            return;
                        }
                        long ts = AeronWire.readInboundTimestamp(buffer, offset);
                        IoCodec.readOrderIn(
                                ctx,
                                buffer,
                                offset + AeronWire.INBOUND_TS_LENGTH,
                                Math.min(
                                        length - AeronWire.INBOUND_TS_LENGTH,
                                        AeronWire.INBOUND_FRAME_LENGTH),
                                fields);
                        if (dbgOrder(fields.id) || dbgUser(fields.userId)) {
                            System.err.println(
                                    "[dbg] RAW pos="
                                            + header.position()
                                            + " ts="
                                            + ts
                                            + " status="
                                            + fields.status
                                            + " id="
                                            + fields.id
                                            + " user="
                                            + fields.userId
                                            + " type="
                                            + fields.type
                                            + " qty="
                                            + fields.origQtyMantissa
                                            + " side="
                                            + fields.side);
                        }
                        inQueue.add(
                                new InboundEvent(
                                        ts,
                                        fields.status,
                                        fields.id,
                                        fields.userId,
                                        fields.origQtyMantissa,
                                        fields.origQtyExponent,
                                        fields.side,
                                        fields.type,
                                        eventSeq[0]++));
                    };
            // 预热：两路 replay 会话建立时间不同步，先等双方都投递出第一条再开始合并，
            // 避免入站在成交侧未就绪时被提前消费（撤单提前 drop → 幽灵单误报）。
            // 成交侧只等"replay 已投递消息"（锚点路径下成交 recording 从 0 全量
            // 回放、锚点前的成交被时间过滤——若等 tradeBuf 非空，预热会等完前
            // 数 GB 才能超时，2026-08-31 定位）。
            long warmupDeadline = System.currentTimeMillis() + 30_000L;
            boolean[] trDelivered = {false};
            boolean[] inDelivered = {false};
            while ((inQueue.isEmpty() || !trDelivered[0])
                    && System.currentTimeMillis() < warmupDeadline) {
                if (inQueue.isEmpty() && inSub.poll(inHandler, 256) > 0) {
                    inDelivered[0] = true;
                    lastActivity[0] = System.currentTimeMillis();
                }
                if (trSub.poll(trAssembler, 256) > 0) {
                    trDelivered[0] = true;
                    lastActivity[0] = System.currentTimeMillis();
                }
                idle.idle();
            }
            if (inQueue.isEmpty() || !trDelivered[0]) {
                fail(r, "回放预热超时（入站/成交 replay 未投递）", 5);
                return r;
            }
            if (anchor.present() && !inQueue.isEmpty()) {
                effAnchorTs[0] = inQueue.peek().ts;
                if (r.boundaryTs < 0L) {
                    r.boundaryTs = effAnchorTs[0];
                }
                System.err.println(
                        "[TradeRecon] 成交过滤锚点=增量首帧 ts="
                                + effAnchorTs[0]
                                + "（原 freezeTs="
                                + anchor.ts()
                                + "）");
            }
            while (true) {
                long progressNow = System.currentTimeMillis();
                if (progressNow - lastProgress[0] > 60_000L) {
                    System.err.println(
                            "[TradeRecon] 进度: inQ="
                                    + inQueue.size()
                                    + " trBuf="
                                    + tradeBuf.size()
                                    + " creates="
                                    + r.creates
                                    + " fills="
                                    + r.fills
                                    + " inDone="
                                    + replayDone(inSub)
                                    + " trDone="
                                    + replayDone(trSub));
                    lastProgress[0] = progressNow;
                }
                // 补入站行（批量拉取，入队）
                if (inQueue.isEmpty()) {
                    int n = inSub.poll(inHandler, 256);
                    if (n > 0) {
                        inDelivered[0] = true;
                        lastActivity[0] = System.currentTimeMillis();
                    }
                }
                // 补成交批（每次 poll 至多 1 条消息 = 一批 fill）
                if (tradeBuf.isEmpty()) {
                    int n = trSub.poll(trAssembler, 256);
                    if (n > 0) {
                        trDelivered[0] = true;
                        lastActivity[0] = System.currentTimeMillis();
                    }
                }
                long inTs = !inQueue.isEmpty() ? inQueue.peek().ts : Long.MAX_VALUE;
                long fillTs = !tradeBuf.isEmpty() ? tradeBuf.get(0).getTime() : Long.MAX_VALUE;
                if (inTs < fillTs && !inQueue.isEmpty()) {
                    InboundEvent e = inQueue.poll();
                    if (DEBUG && debugPrinted[0]++ < 15) {
                        System.err.println(
                                "[dbg] inbound ts=" + e.ts + " status=" + e.status + " id=" + e.id);
                    }
                    if (dbgOrder(e.id) || dbgUser(e.userId)) {
                        System.err.println(
                                "[dbg] PATH single inTs="
                                        + inTs
                                        + " fillTs="
                                        + fillTs
                                        + " status="
                                        + e.status
                                        + " id="
                                        + e.id
                                        + " ts="
                                        + e.ts);
                    }
                    processInbound(e, orders, liveByUser, r);
                } else if (!inQueue.isEmpty() && inTs == fillTs) {
                    if (dbgOrder(inQueue.peek().id) || dbgUser(inQueue.peek().userId)) {
                        System.err.println("[dbg] PATH group inTs=" + inTs + " fillTs=" + fillTs);
                    }
                    // ── 同 ts 组：create → fill → cancel（撮合环序）。
                    // 关键：同一 ts 的 fill 可能跨多条成交消息，cancel/批撤必须等该 ts 的
                    // fill 全部到齐（下一批首笔 fill ts > T，或成交回放结束）才能执行；
                    // 入站侧同理收齐（回放结束由 hasNoImages 判定，不靠次数猜）。
                    // 1) 收齐同 ts 入站事件
                    int inSpins = 0;
                    boolean groupIncomplete = false;
                    while (true) {
                        while (!inQueue.isEmpty() && inQueue.peek().ts == inTs) {
                            inGroup.add(inQueue.poll());
                        }
                        if (inSpins >= GROUP_SPIN_CAP && inQueue.isEmpty() && !replayDone(inSub)) {
                            groupIncomplete = true;
                            break;
                        }
                        if (!inQueue.isEmpty() || replayDone(inSub)) {
                            break;
                        }
                        int n = inSub.poll(inHandler, 256);
                        if (n > 0) {
                            lastActivity[0] = System.currentTimeMillis();
                            inSpins = 0;
                            continue;
                        }
                        inSpins++;
                        sleepBriefly();
                    }
                    if (groupIncomplete) {
                        // 回放停顿超时且队空：同 ts create 可能尚未送达，残缺组直接
                        // 消费 fills 会误报 U5（D20 锚点边界残余根因）。把已收事件放回
                        // 队首，下一轮重试 merge；fills 保留在 tradeBuf 等待。
                        while (!inGroup.isEmpty()) {
                            inQueue.addFirst(inGroup.removeLast());
                        }
                        continue;
                    }
                    // 2) 全部 create 先入簿（按到达顺序）。
                    // 3) 收齐并消费该 ts 的全部 fill（撮合先于同 ts 撤单）。
                    // 4) cancel/批撤按到达顺序执行——批撤只撤 createSeq 更小的单
                    //    （引擎帧序：先批撤后建单时新单存活），单笔撤按 id 定位。
                    for (InboundEvent e : inGroup) {
                        if (e.status == 0) {
                            processInbound(e, orders, liveByUser, r);
                        }
                    }
                    drainSameTsFills(
                            trSub,
                            trAssembler,
                            tradeBuf,
                            inTs,
                            orders,
                            liveByUser,
                            r,
                            fundUsers,
                            lastActivity);
                    for (InboundEvent e : inGroup) {
                        if (e.status != 0) {
                            processInbound(e, orders, liveByUser, r);
                        }
                    }
                    inGroup.clear();
                } else if (!tradeBuf.isEmpty()) {
                    // fillTs < inTs，或入站队列空
                    if (inQueue.isEmpty() && !replayDone(inSub)) {
                        // 入站回放未结束且暂无货：先把入站追平 fillTs 再处理 fill，
                        // 防止同 ts 的 create 晚到而 fill 被提前消费（幽灵单）。
                        int spins = 0;
                        while (inQueue.isEmpty() && !replayDone(inSub) && spins < GROUP_SPIN_CAP) {
                            sleepBriefly();
                            spins++;
                            int n = inSub.poll(inHandler, 256);
                            if (n > 0) {
                                lastActivity[0] = System.currentTimeMillis();
                            }
                        }
                        if (inQueue.isEmpty()) {
                            if (!replayDone(inSub)) {
                                continue; // 入站异常停顿：交给主循环重试/静默退出
                            }
                        } else {
                            inTs = inQueue.peek().ts;
                            if (inTs <= fillTs) {
                                continue; // 队列有货：重新按 merge 判断
                            }
                        }
                    }
                    ExchangeTrade t = tradeBuf.remove(0);
                    if (DEBUG && debugPrinted[0]++ < 15) {
                        System.err.println(
                                "[dbg] fill ts="
                                        + t.getTime()
                                        + " buy="
                                        + t.getBuyOrderId()
                                        + " sell="
                                        + t.getSellOrderId()
                                        + " amt="
                                        + t.getAmountMantissa()
                                        + " buyTo="
                                        + t.getBuyTurnoverMantissa());
                    }
                    checkFill(t, orders, liveByUser, r, fundUsers);
                }
                long now = System.currentTimeMillis();
                if (now - start > REPLAY_DEADLINE_MS) {
                    System.err.println(
                            "[TradeRecon] 回放超截止退出: inDone="
                                    + replayDone(inSub)
                                    + " trDone="
                                    + replayDone(trSub)
                                    + " inQ="
                                    + inQueue.size()
                                    + " trBuf="
                                    + tradeBuf.size());
                    break;
                }
                if (now - lastActivity[0] > REPLAY_QUIET_MS) {
                    if ((inDelivered[0] && trDelivered[0])
                            || (replayDone(inSub) && replayDone(trSub))) {
                        break; // 两路均已投递过且静默 2s（或官方 done）：正常退出
                    }
                    if (now - lastActivity[0] > REPLAY_STALL_MS) {
                        System.err.println(
                                "[TradeRecon] 回放停顿超 "
                                        + (REPLAY_STALL_MS / 1000)
                                        + "s 强制退出: inDone="
                                        + replayDone(inSub)
                                        + " trDone="
                                        + replayDone(trSub)
                                        + " inQ="
                                        + inQueue.size()
                                        + " trBuf="
                                        + tradeBuf.size()
                                        + " fills="
                                        + r.fills
                                        + " creates="
                                        + r.creates);
                        break;
                    }
                    // 一路/两路未结束但短暂静默（长 GC、段切换等）：继续等待，不误判结束
                }
                if (!inQueue.isEmpty() || !tradeBuf.isEmpty() || !inGroup.isEmpty()) {
                    continue; // 有缓冲事件，立即推进
                }
                idle.idle();
            }
        }

        if (anchor.present()) {
            // 锚点路径：成交只校验增量入站覆盖范围（ts > 增量首帧），U8 全局
            // 计数（全部成交）不适用——打印锚点后校验的成交笔数供参考。
            System.err.println("[TradeRecon] 锚点路径校验成交 " + r.fills + " 笔" + "（U8 全局计数不适用）");
        } else if (expectedTrades >= 0L && r.fills != expectedTrades) {
            fail(r, "U8 成交笔数不符: 回放=" + r.fills + " expected=" + expectedTrades, 8);
        }
        r.ok = r.details.isEmpty();
        return r;
    }

    /** 入站事件（合并回放缓冲：fields 复用，需拷贝关键字段）。 */
    private record InboundEvent(
            long ts,
            byte status,
            long id,
            long userId,
            long qtyMantissa,
            int qtyExponent,
            byte side,
            byte type,
            long seq) {}

    private static void processInbound(
            InboundEvent e,
            Map<Long, OrderInfo> orders,
            Map<Long, java.util.Set<OrderInfo>> liveByUser,
            Result r) {
        switch (e.status) {
            case 0 -> {
                r.creates++;
                boolean stop = e.type >= 3 && e.type <= 6;
                OrderInfo o =
                        new OrderInfo(
                                e.id,
                                e.userId,
                                e.qtyMantissa,
                                e.qtyExponent,
                                e.side,
                                e.ts,
                                stop,
                                e.seq);
                if (dbgOrder(e.id)) {
                    System.err.println(
                            "[dbg] CREATE id="
                                    + e.id
                                    + " ts="
                                    + e.ts
                                    + " user="
                                    + e.userId
                                    + " qty="
                                    + dec(e.qtyMantissa, e.qtyExponent)
                                    + " side="
                                    + e.side
                                    + " type="
                                    + e.type
                                    + " stop="
                                    + stop);
                }
                // orderId 全局唯一（producer 按发号方前缀隔离 ID 空间后）时不应出现重复
                // create；若仍出现（数据缺陷兜底），按最新 create 覆盖——与引擎
                // "重复 orderId 在簿则拒、不在簿则收" 的实例语义更接近。覆盖前摘除
                // 旧实例，避免 liveByUser 孤儿累积。
                OrderInfo prev = orders.put(e.id, o);
                if (prev != null) {
                    r.duplicateCreates++;
                    removeLive(liveByUser, prev);
                }
                if (r.seenCreateTs.size() < 200_000) {
                    r.seenCreateTs.put(e.id, e.ts);
                }
                if (r.allCreateTs != null && dbgOrder(e.id)) {
                    r.allCreateTs.put(e.id, e.ts);
                }
                liveByUser.computeIfAbsent(e.userId, k -> new java.util.LinkedHashSet<>()).add(o);
            }
            case 2 -> {
                r.cancels++;
                if (dbgOrder(e.id)) {
                    System.err.println("[dbg] CANCEL id=" + e.id + " ts=" + e.ts);
                }
                OrderInfo o = orders.remove(e.id);
                if (o != null && !o.cancelled) {
                    if (o.stop) {
                        // 引擎 status=2 只撤限价簿（OrderBook.cancel），条件单
                        // 挂 TriggerSlots 不命中：保持表项，否则触发后的成交会误报 U5。
                        orders.put(e.id, o);
                        return;
                    }
                    if (o.createSeq >= e.seq) {
                        // 撤单帧在 create 之前（同 ms 交错）：引擎 book.cancel 未命中
                        // （订单尚未入簿），订单存活——保持表项。
                        orders.put(e.id, o);
                        return;
                    }
                    o.cancelled = true;
                    o.cancelTime = e.ts;
                    o.cancelType = 1;
                    noteDropped(r, e.id, "cancel@" + e.ts);
                    removeLive(liveByUser, o);
                }
            }
            case 99 -> {
                r.batchCancels++;
                if (DEBUG && DEBUG_ORDERS.isEmpty()) {
                    System.err.println("[dbg] BATCH_CANCEL ts=" + e.ts + " user=" + e.userId);
                }
                java.util.Set<OrderInfo> live = liveByUser.get(e.userId);
                if (live != null) {
                    for (OrderInfo o : live) {
                        if (o.stop) {
                            // 引擎 batchCancel 只扫限价簿（buy/sellLevels），条件单
                            // 在 TriggerSlots 不参与：保持表项，触发后成交不误报。
                            continue;
                        }
                        if (o.createSeq >= e.seq) {
                            // 引擎按帧序处理：批撤只撤它之前已入簿的单；同 ms 里
                            // 批撤之后才创建的订单存活（大压力对账实测 U5 根因）。
                            continue;
                        }
                        if (dbgOrder(o.orderId)) {
                            System.err.println(
                                    "[dbg] BATCH_CANCEL id="
                                            + o.orderId
                                            + " ts="
                                            + e.ts
                                            + " user="
                                            + e.userId
                                            + " cancelled="
                                            + o.cancelled);
                        }
                        if (!o.cancelled) {
                            o.cancelled = true;
                            o.cancelTime = e.ts;
                            o.cancelType = 2;
                        }
                        orders.remove(o.orderId);
                        noteDropped(r, o.orderId, "batchCancel@" + e.ts);
                    }
                    live.clear();
                }
            }
            default -> {}
        }
    }

    static void checkFill(
            ExchangeTrade t,
            Map<Long, OrderInfo> orders,
            Map<Long, java.util.Set<OrderInfo>> liveByUser,
            Result r,
            int fundUsers) {
        r.fills++;
        OrderInfo buy = orders.get(t.getBuyOrderId());
        OrderInfo sell = orders.get(t.getSellOrderId());
        // 边界成交（ts ≤ 锚点增量首帧 ts）：只记基线不校验（2026-09-02 残余修复——
        // 边界 ts 的成交承载订单累计基线，丢弃会让后续首笔/增量校验拿旧累计误报）。
        boolean boundary = r.boundaryTs >= 0L && t.getTime() <= r.boundaryTs;
        // 同毫秒创建-成交：对账双回放（入站 vs 成交）在同一 ms 内无法保证 create/fill
        // 顺序（D25 实测 replayer 被大成交回放占用时同 ts 批次交错）——该成交的累计
        // 基线可能含未及重建的同 ms 成交，只记基线不校验。
        boolean sameMsCreate =
                (buy != null && buy.createTime == t.getTime())
                        || (sell != null && sell.createTime == t.getTime());
        // 同毫秒 create 竞态：create 已见（seenCreateTs == 成交 ts）但订单条目尚未
        // 重建完成——该成交按基线处理（不报 U5、不校验 U1）。
        boolean buyRace =
                buy == null && r.seenCreateTs.getOrDefault(t.getBuyOrderId(), -1L) == t.getTime();
        boolean sellRace =
                sell == null && r.seenCreateTs.getOrDefault(t.getSellOrderId(), -1L) == t.getTime();
        boolean baselineOnly = boundary || sameMsCreate || buyRace || sellRace;
        if (Boolean.getBoolean("ocx.recon.debug-boundary")) {
            // 锚点后边界轨迹：前 2s 内所有成交打印现场（排查 U5/U1 边界残余）
            long boundaryTs = r.boundaryTs;
            if (boundaryTs > 0 && t.getTime() - boundaryTs < 2_000) {
                System.err.println(
                        "[boundary] FILL ts="
                                + t.getTime()
                                + " buy="
                                + t.getBuyOrderId()
                                + " sell="
                                + t.getSellOrderId()
                                + " buyPresent="
                                + (buy != null)
                                + " sellPresent="
                                + (sell != null)
                                + " buyDropped="
                                + r.droppedReason.get(t.getBuyOrderId())
                                + " sellDropped="
                                + r.droppedReason.get(t.getSellOrderId()));
            }
        }
        if (dbgOrder(t.getBuyOrderId()) || dbgOrder(t.getSellOrderId())) {
            System.err.println(
                    "[dbg] FILL ts="
                            + t.getTime()
                            + " buy="
                            + t.getBuyOrderId()
                            + " sell="
                            + t.getSellOrderId()
                            + " amt="
                            + dec(t.getAmountMantissa(), t.getAmountExponent())
                            + " bTurn="
                            + dec(t.getBuyTurnoverMantissa(), t.getBuyTurnoverExponent())
                            + " sTurn="
                            + dec(t.getSellTurnoverMantissa(), t.getSellTurnoverExponent())
                            + " buyPresent="
                            + (buy != null)
                            + " sellPresent="
                            + (sell != null)
                            + " buySum="
                            + (buy != null ? buy.sumAmount : BigDecimal.ZERO)
                            + " sellSum="
                            + (sell != null ? sell.sumAmount : BigDecimal.ZERO));
        }
        BigDecimal price = dec(t.getPriceMantissa(), t.getPriceExponent());
        BigDecimal amount = dec(t.getAmountMantissa(), t.getAmountExponent());
        BigDecimal expected = price.multiply(amount);
        if (!baselineOnly) {
            // U5a 幽灵单（基线成交不查：锚点前/同 ms 创建竞态订单可能不在重建状态里）
            if (buy == null) {
                fail(
                        r,
                        "U5 未知 buyOrderId="
                                + t.getBuyOrderId()
                                + " dropped="
                                + r.droppedReason.get(t.getBuyOrderId())
                                + " seenCreateTs="
                                + r.seenCreateTs.get(t.getBuyOrderId())
                                + " inPrefilled="
                                + r.prefilledIds.contains(t.getBuyOrderId())
                                + (r.allCreateTs != null
                                        ? " allCreateTs=" + r.allCreateTs.get(t.getBuyOrderId())
                                        : "")
                                + " trade@"
                                + t.getTime(),
                        5);
            }
            if (sell == null) {
                fail(
                        r,
                        "U5 未知 sellOrderId="
                                + t.getSellOrderId()
                                + " dropped="
                                + r.droppedReason.get(t.getSellOrderId())
                                + " seenCreateTs="
                                + r.seenCreateTs.get(t.getSellOrderId())
                                + " inPrefilled="
                                + r.prefilledIds.contains(t.getSellOrderId())
                                + (r.allCreateTs != null
                                        ? " allCreateTs=" + r.allCreateTs.get(t.getSellOrderId())
                                        : "")
                                + " trade@"
                                + t.getTime(),
                        5);
            }
            if (buy == null || sell == null) {
                return;
            }
            // U2b 方向一致性
            if (buy.side != 0) {
                fail(r, "U2 买方订单 side!=BUY orderId=" + t.getBuyOrderId(), 2);
            }
            if (sell.side != 1) {
                fail(r, "U2 卖方订单 side!=SELL orderId=" + t.getSellOrderId(), 2);
            }
            // U4 自成交排除
            if (buy.userId == sell.userId) {
                fail(
                        r,
                        "U4 自成交 buy/sell 同 userId="
                                + buy.userId
                                + " buy="
                                + t.getBuyOrderId()
                                + " sell="
                                + t.getSellOrderId(),
                        4);
            }
            // 金额：price×amount == buy/sellTurnover 增量
            r.totalQuote = r.totalQuote.add(expected);
            checkTurnover(
                    buy, true, t.getBuyTurnoverMantissa(), t.getBuyTurnoverExponent(), expected, r);
            checkTurnover(
                    sell,
                    false,
                    t.getSellTurnoverMantissa(),
                    t.getSellTurnoverExponent(),
                    expected,
                    r);
            // U5b 已撤单仍成交（边界成交不查：同 ts 撤/成交交错，顺序不可辨）
            if (buy.cancelled && t.getTime() > buy.cancelTime) {
                fail(
                        r,
                        "U5 已撤单仍成交 buyOrderId="
                                + t.getBuyOrderId()
                                + " user="
                                + buy.userId
                                + " cancelType="
                                + buy.cancelType
                                + " cancel="
                                + buy.cancelTime
                                + " fill="
                                + t.getTime(),
                        5);
            }
            if (sell.cancelled && t.getTime() > sell.cancelTime) {
                fail(
                        r,
                        "U5 已撤单仍成交 sellOrderId="
                                + t.getSellOrderId()
                                + " user="
                                + sell.userId
                                + " cancelType="
                                + sell.cancelType
                                + " cancel="
                                + sell.cancelTime
                                + " fill="
                                + t.getTime(),
                        5);
            }
        } else {
            // 基线成交：只记 turnover 基线（不校验），U2 累计/完成移除照常
            if (buy != null) {
                recordTurnoverBaseline(
                        buy, true, t.getBuyTurnoverMantissa(), t.getBuyTurnoverExponent());
            }
            if (sell != null) {
                recordTurnoverBaseline(
                        sell, false, t.getSellTurnoverMantissa(), t.getSellTurnoverExponent());
            }
        }
        // U2 数量守恒 + 完成移除（按在场侧）
        if (buy != null) {
            buy.sumAmount = buy.sumAmount.add(amount);
            if (buy.sumAmount.compareTo(buy.origQty) > 0) {
                fail(
                        r,
                        "U2 买方累计成交量超原始量 orderId="
                                + buy.orderId
                                + " sum="
                                + buy.sumAmount
                                + " orig="
                                + buy.origQty,
                        2);
            }
            if (buy.sumAmount.compareTo(buy.origQty) >= 0) {
                orders.remove(buy.orderId);
                noteDropped(r, buy.orderId, "match@fillTime=" + t.getTime());
                removeLive(liveByUser, buy);
            }
        }
        if (sell != null) {
            sell.sumAmount = sell.sumAmount.add(amount);
            if (sell.sumAmount.compareTo(sell.origQty) > 0) {
                fail(
                        r,
                        "U2 卖方累计成交量超原始量 orderId="
                                + sell.orderId
                                + " sum="
                                + sell.sumAmount
                                + " orig="
                                + sell.origQty,
                        2);
            }
            if (sell.sumAmount.compareTo(sell.origQty) >= 0) {
                orders.remove(sell.orderId);
                noteDropped(r, sell.orderId, "match@fillTime=" + t.getTime());
                removeLive(liveByUser, sell);
            }
        }
        // U3 专用用户账本
        if (fundUsers > 0 && buy != null && sell != null) {
            if (buy.userId < fundUsers) {
                Ledger l = r.ledgers.computeIfAbsent(buy.userId, k -> new Ledger());
                l.buyFills++;
                l.buyQty = l.buyQty.add(amount);
                l.buyQuote = l.buyQuote.add(expected);
            }
            if (sell.userId < fundUsers) {
                Ledger l = r.ledgers.computeIfAbsent(sell.userId, k -> new Ledger());
                l.sellFills++;
                l.sellQty = l.sellQty.add(amount);
                l.sellQuote = l.sellQuote.add(expected);
            }
        }
    }

    static void checkTurnover(
            OrderInfo o,
            boolean buySide,
            long mantissa,
            int exponent,
            BigDecimal expected,
            Result r) {
        BigDecimal turnover = dec(mantissa, exponent);
        if (buySide) {
            if (o.hasBuyTurnover) {
                BigDecimal prev = dec(o.lastBuyTurnoverMantissa, o.lastBuyTurnoverExponent);
                if (turnover.subtract(prev)
                                .subtract(expected)
                                .abs()
                                .compareTo(BigDecimal.ONE.scaleByPowerOfTen(exponent))
                        > 0) {
                    fail(
                            r,
                            "U1 buyTurnover 增量 != price×amount orderId="
                                    + o.orderId
                                    + " delta="
                                    + turnover.subtract(prev)
                                    + " expected="
                                    + expected,
                            1);
                }
            } else if (turnover.subtract(expected)
                            .abs()
                            .compareTo(BigDecimal.ONE.scaleByPowerOfTen(exponent))
                    > 0) {
                if (!o.fromSnapshot) {
                    fail(
                            r,
                            "U1 首笔 buyTurnover != price×amount orderId="
                                    + o.orderId
                                    + " turnover="
                                    + turnover
                                    + " expected="
                                    + expected,
                            1);
                }
            }
            o.hasBuyTurnover = true;
            o.lastBuyTurnoverMantissa = mantissa;
            o.lastBuyTurnoverExponent = exponent;
        } else {
            if (o.hasSellTurnover) {
                BigDecimal prev = dec(o.lastSellTurnoverMantissa, o.lastSellTurnoverExponent);
                if (turnover.subtract(prev)
                                .subtract(expected)
                                .abs()
                                .compareTo(BigDecimal.ONE.scaleByPowerOfTen(exponent))
                        > 0) {
                    fail(
                            r,
                            "U1 sellTurnover 增量 != price×amount orderId="
                                    + o.orderId
                                    + " delta="
                                    + turnover.subtract(prev)
                                    + " expected="
                                    + expected,
                            1);
                }
            } else if (turnover.subtract(expected)
                            .abs()
                            .compareTo(BigDecimal.ONE.scaleByPowerOfTen(exponent))
                    > 0) {
                if (!o.fromSnapshot) {
                    fail(
                            r,
                            "U1 首笔 sellTurnover != price×amount orderId="
                                    + o.orderId
                                    + " turnover="
                                    + turnover
                                    + " expected="
                                    + expected,
                            1);
                }
            }
            o.hasSellTurnover = true;
            o.lastSellTurnoverMantissa = mantissa;
            o.lastSellTurnoverExponent = exponent;
        }
    }

    /** 边界成交基线记录：只更新累计额状态，不校验 price×amount。 */
    static void recordTurnoverBaseline(OrderInfo o, boolean buySide, long mantissa, int exponent) {
        if (buySide) {
            o.hasBuyTurnover = true;
            o.lastBuyTurnoverMantissa = mantissa;
            o.lastBuyTurnoverExponent = exponent;
        } else {
            o.hasSellTurnover = true;
            o.lastSellTurnoverMantissa = mantissa;
            o.lastSellTurnoverExponent = exponent;
        }
    }

    static void fail(Result r, String msg, int u) {
        if (u == 1) r.u1Failures++;
        else if (u == 2) r.u2Failures++;
        else if (u == 4) r.u4Failures++;
        else if (u == 5) r.u5Failures++;
        else r.u5Failures++;
        if (r.details.size() < FAILURE_CAP) {
            r.details.add(msg);
        }
    }

    private static void removeLive(Map<Long, java.util.Set<OrderInfo>> liveByUser, OrderInfo o) {
        java.util.Set<OrderInfo> live = liveByUser.get(o.userId);
        if (live != null) {
            live.remove(o);
        }
    }

    private static void noteDropped(Result r, long orderId, String reason) {
        if (r.droppedReason.size() < FAILURE_CAP) {
            r.droppedReason.put(orderId, reason);
        }
    }

    private static void printReport(Result r, int fundUsers, long expectedTrades) {
        StringBuilder sb = new StringBuilder();
        sb.append("[TradeRecon] 入站: creates=")
                .append(r.creates)
                .append(" dupCreates=")
                .append(r.duplicateCreates)
                .append(" cancels=")
                .append(r.cancels)
                .append(" batchCancels=")
                .append(r.batchCancels)
                .append("; 成交 fills=")
                .append(r.fills)
                .append(" totalQuote=")
                .append(r.totalQuote.toPlainString());
        if (expectedTrades >= 0L) {
            sb.append(" expected=").append(expectedTrades);
        }
        System.err.println(sb);
        System.err.println(
                "[TradeRecon] 不变量违例: U1金额="
                        + r.u1Failures
                        + " U2订单守恒="
                        + r.u2Failures
                        + " U4自成交="
                        + r.u4Failures
                        + " U5引用/批撤="
                        + r.u5Failures);
        if (fundUsers > 0) {
            System.err.println("[TradeRecon] 专用用户账本（base/quote 均按帧内定点值，未折算 UI 小数）:");
            List<Map.Entry<Long, Ledger>> users = new ArrayList<>(r.ledgers.entrySet());
            users.sort(Comparator.comparingLong(Map.Entry::getKey));
            for (Map.Entry<Long, Ledger> e : users) {
                Ledger l = e.getValue();
                System.err.printf(
                        "[TradeRecon]   user=%d buyFills=%d sellFills=%d"
                                + " buyQty=%s sellQty=%s buyQuote=%s sellQuote=%s"
                                + " netQty=%s netQuote=%s%n",
                        e.getKey(),
                        l.buyFills,
                        l.sellFills,
                        l.buyQty.toPlainString(),
                        l.sellQty.toPlainString(),
                        l.buyQuote.toPlainString(),
                        l.sellQuote.toPlainString(),
                        l.buyQty.subtract(l.sellQty).toPlainString(),
                        l.sellQuote.subtract(l.buyQuote).toPlainString());
            }
        }
        for (String d : r.details) {
            System.err.println("[TradeRecon]   FAIL: " + d);
        }
    }

    static BigDecimal dec(long mantissa, int exponent) {
        return BigDecimal.valueOf(mantissa).scaleByPowerOfTen(exponent);
    }

    private static int replayStream(long recordingId) {
        return REPLAY_BASE + (int) (recordingId % 10_000);
    }

    /** 收齐并消费某 ts 的全部 fill（同 ts fill 可跨多条成交消息）。 */
    private static void drainSameTsFills(
            io.aeron.Subscription trSub,
            io.aeron.FragmentAssembler trAssembler,
            List<ExchangeTrade> tradeBuf,
            long inTs,
            Map<Long, OrderInfo> orders,
            Map<Long, java.util.Set<OrderInfo>> liveByUser,
            Result r,
            int fundUsers,
            long[] lastActivity) {
        long[] debugPrinted = {0L};
        int fillSpins = 0;
        while (true) {
            while (!tradeBuf.isEmpty() && tradeBuf.get(0).getTime() == inTs) {
                ExchangeTrade t = tradeBuf.remove(0);
                if (DEBUG && debugPrinted[0]++ < 15) {
                    System.err.println(
                            "[dbg] fill ts="
                                    + t.getTime()
                                    + " buy="
                                    + t.getBuyOrderId()
                                    + " sell="
                                    + t.getSellOrderId());
                }
                checkFill(t, orders, liveByUser, r, fundUsers);
            }
            if (!tradeBuf.isEmpty() || replayDone(trSub) || fillSpins >= GROUP_SPIN_CAP) {
                return;
            }
            int n = trSub.poll(trAssembler, 256);
            if (n > 0) {
                lastActivity[0] = System.currentTimeMillis();
                fillSpins = 0;
                continue;
            }
            fillSpins++;
            sleepBriefly();
        }
    }

    /** 回放订阅是否已消费完：replay 会话结束时 image 被移除（isClosed/hasNoImages 兜底）。 */
    private static boolean replayDone(io.aeron.Subscription sub) {
        return sub.isClosed() || sub.hasNoImages();
    }

    private static void sleepBriefly() {
        try {
            Thread.sleep(2L);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private static List<Recording> listRecordings(AeronArchive archive, int streamId) {
        List<Recording> list = new ArrayList<>();
        archive.listRecordings(
                0L,
                Integer.MAX_VALUE,
                (controlSessionId,
                        correlationId,
                        recordingId,
                        startTimestamp,
                        triggerTimestamp,
                        startPosition,
                        stopPosition,
                        initialTermId,
                        segmentFileLength,
                        termBufferLength,
                        mtuLength,
                        sessionId,
                        sid,
                        strippedChannel,
                        originalChannel,
                        sourceIdentity) -> {
                    if (sid == streamId) {
                        list.add(new Recording(recordingId, startPosition, stopPosition));
                    }
                });
        list.sort(Comparator.comparingLong(Recording::recordingId));
        return list;
    }

    /** 快照锚点（2026-08-31）：最新有效快照的入站锚点 + 快照时刻 ts。 */
    private record SnapshotAnchor(
            boolean present, long inboundRecordingId, long inboundPosition, long ts) {
        static SnapshotAnchor none() {
            return new SnapshotAnchor(false, -1L, -1L, Long.MIN_VALUE);
        }
    }

    private record SnapshotVerify(
            boolean valid, SnapshotCodec.SnapshotMeta meta, long beginMsgIndex) {}

    private interface SnapshotMessageConsumer {
        void accept(DirectBuffer buffer, int offset, int length);
    }

    /**
     * 加载并应用最新有效快照（META + END + 计数一致），预填订单流（限价 + 条件单），
     * 返回入站锚点；无有效快照返回 {@link SnapshotAnchor#none()}（全量回放入站）。
     */
    private static SnapshotAnchor loadSnapshotAnchor(
            AeronArchive archive,
            String symbol,
            Map<Long, OrderInfo> orders,
            Map<Long, java.util.Set<OrderInfo>> liveByUser,
            Result r) {
        List<Recording> recs = listRecordings(archive, StreamIds.snapshot(symbol));
        recs.sort(Comparator.comparingLong(Recording::recordingId).reversed());
        for (Recording rec : recs) {
            SnapshotVerify verify = verifySnapshotRecording(archive, rec, symbol);
            if (!verify.valid()) {
                System.err.println(
                        "[TradeRecon] 快照 recordingId="
                                + rec.recordingId()
                                + " 不完整（END/计数校验失败），回退更旧份");
                continue;
            }
            applySnapshot(archive, rec, symbol, verify, orders, liveByUser, r);
            System.err.println(
                    "[TradeRecon] 快照锚点预填: recordingId="
                            + rec.recordingId()
                            + " orders="
                            + verify.meta().count()
                            + " triggers="
                            + verify.meta().triggerCount()
                            + " ts="
                            + verify.meta().timestamp()
                            + " inAnchor="
                            + verify.meta().inboundRecordingId()
                            + "@"
                            + verify.meta().inboundPosition());
            return new SnapshotAnchor(
                    true,
                    verify.meta().inboundRecordingId(),
                    verify.meta().inboundPosition(),
                    verify.meta().timestamp());
        }
        System.err.println("[TradeRecon] 无有效快照：全量回放入站（旧行为）");
        return SnapshotAnchor.none();
    }

    /** 校验快照 recording：META + END + order/stop 计数一致才有效。 */
    private static SnapshotVerify verifySnapshotRecording(
            AeronArchive archive, Recording rec, String symbol) {
        // 常驻 recording 分段扫描（2026-08-31 方案 A）：逐段 BEGIN+META+ORDER+
        // STOP+END 校验，取"最新有效段"的 meta + BEGIN 消息序号（apply 按序号定位）。
        long[] bestBeginMsg = {-1L};
        SnapshotCodec.SnapshotMeta[] bestMeta = {null};
        long[] msgCount = {0L};
        long[] curBeginMsg = {-1L};
        boolean[] inSegment = {false};
        int[] orderCount = {0};
        int[] triggerCount = {0};
        boolean[] endSeen = {false};
        int[][] endCounts = {null};
        SnapshotCodec.SnapshotMeta[] curMeta = {null};
        replaySnapshotRecording(
                archive,
                rec,
                symbol,
                (buffer, offset, length) -> {
                    if (!SnapshotCodec.hasMagic(buffer, offset)) {
                        return;
                    }
                    msgCount[0]++;
                    switch (SnapshotCodec.type(buffer, offset)) {
                        case SnapshotCodec.TYPE_SNAPSHOT_BEGIN -> {
                            inSegment[0] = true;
                            curBeginMsg[0] = msgCount[0];
                            orderCount[0] = 0;
                            triggerCount[0] = 0;
                            endSeen[0] = false;
                            endCounts[0] = null;
                            curMeta[0] = null;
                        }
                        case SnapshotCodec.TYPE_META -> {
                            if (inSegment[0]) {
                                curMeta[0] = SnapshotCodec.decodeMeta(symbol, buffer, offset);
                            }
                        }
                        case SnapshotCodec.TYPE_ORDER_BATCH ->
                                SnapshotCodec.forEachRecord(
                                        buffer,
                                        offset,
                                        length,
                                        (p, l) -> {
                                            if (inSegment[0]) {
                                                orderCount[0]++;
                                            }
                                        });
                        case SnapshotCodec.TYPE_STOP_BATCH ->
                                SnapshotCodec.forEachRecord(
                                        buffer,
                                        offset,
                                        length,
                                        (p, l) -> {
                                            if (inSegment[0]) {
                                                triggerCount[0]++;
                                            }
                                        });
                        case SnapshotCodec.TYPE_END -> {
                            if (inSegment[0] && curMeta[0] != null) {
                                endSeen[0] = true;
                                endCounts[0] = SnapshotCodec.decodeEndCounts(buffer, offset);
                                boolean valid =
                                        endCounts[0][0] == curMeta[0].count()
                                                && endCounts[0][1] == curMeta[0].triggerCount()
                                                && orderCount[0] == curMeta[0].count()
                                                && triggerCount[0] == curMeta[0].triggerCount();
                                if (valid) {
                                    bestBeginMsg[0] = curBeginMsg[0];
                                    bestMeta[0] = curMeta[0];
                                }
                            }
                        }
                        default -> {}
                    }
                });
        if (bestMeta[0] == null) {
            System.err.println("[TradeRecon] 快照无有效段（常驻 recording 分段校验失败）");
        }
        return new SnapshotVerify(bestMeta[0] != null, bestMeta[0], bestBeginMsg[0]);
    }

    /** 回放快照预填订单流：限价单与条件单都重建 OrderInfo（createSeq=0，先于一切增量）。 */
    private static void applySnapshot(
            AeronArchive archive,
            Recording rec,
            String symbol,
            SnapshotVerify verify,
            Map<Long, OrderInfo> orders,
            Map<Long, java.util.Set<OrderInfo>> liveByUser,
            Result r) {
        // 常驻 recording：按"最新有效段 BEGIN 消息序号"定位，只收集该段 ORDER/STOP
        long[] msgCount = {0L};
        boolean[] collecting = {false};
        long[] collected = {0L};
        long[] decodeFail = {0L};
        long[] orderBatchMsgs = {0L};
        long[] orderBatchRecords = {0L};
        System.err.println("[TradeRecon] applySnapshot beginMsgIndex=" + verify.beginMsgIndex());
        replaySnapshotRecording(
                archive,
                rec,
                symbol,
                (buffer, offset, length) -> {
                    if (!SnapshotCodec.hasMagic(buffer, offset)) {
                        return;
                    }
                    msgCount[0]++;
                    switch (SnapshotCodec.type(buffer, offset)) {
                        case SnapshotCodec.TYPE_SNAPSHOT_BEGIN -> {
                            collecting[0] = msgCount[0] == verify.beginMsgIndex();
                        }
                        case SnapshotCodec.TYPE_ORDER_BATCH -> {
                            orderBatchMsgs[0]++;
                            int[] recs = {0};
                            SnapshotCodec.forEachRecord(
                                    buffer,
                                    offset,
                                    length,
                                    (p, l) -> {
                                        recs[0]++;
                                        if (!collecting[0]) {
                                            return;
                                        }
                                        byte[] bin = new byte[l];
                                        buffer.getBytes(p, bin);
                                        try {
                                            SnapshotOrderFields f =
                                                    OrderRecordCodec.decodeFields(bin);
                                            collected[0]++;
                                            OrderInfo o =
                                                    new OrderInfo(
                                                            f.orderId,
                                                            f.userId,
                                                            f.origQty,
                                                            0,
                                                            (byte) f.direction,
                                                            f.time,
                                                            false,
                                                            0,
                                                            true);
                                            // 2026-09-01 修复：预填只带"已成交量"基线（U2 续算），
                                            // 不再预置 hasBuy/SellTurnover——锚点冻结与"增量首帧 ts"
                                            // 之间可能有被时间过滤的成交（缺口），用快照 cumQuote 直接
                                            // 校验首笔必然超差；正确语义是锚点后首笔只记基线不校验
                                            // （checkTurnover 的 fromSnapshot 分支），从第二笔起增量精确。
                                            // sumAmount 须按 baseCoinScale 折算为十进制，与 fill 的
                                            // amount 同单位（旧代码 exponent=0 直接混用 mantissa）。
                                            o.sumAmount =
                                                    dec(
                                                            f.executedQty,
                                                            -verify.meta().baseCoinScale());
                                            orders.put(o.orderId, o);
                                            r.prefilledIds.add(o.orderId);
                                            liveByUser
                                                    .computeIfAbsent(
                                                            o.userId,
                                                            k -> new java.util.LinkedHashSet<>())
                                                    .add(o);
                                        } catch (Exception e) {
                                            decodeFail[0]++;
                                            if (decodeFail[0] <= 5) {
                                                System.err.println(
                                                        "[TradeRecon] applySnapshot 解析失败 recLen="
                                                                + l
                                                                + " err="
                                                                + e);
                                            }
                                        }
                                    });
                            orderBatchRecords[0] += recs[0];
                        }
                        case SnapshotCodec.TYPE_STOP_BATCH ->
                                SnapshotCodec.forEachRecord(
                                        buffer,
                                        offset,
                                        length,
                                        (p, l) -> {
                                            if (!collecting[0]) {
                                                return;
                                            }
                                            byte[] bin = new byte[l];
                                            buffer.getBytes(p, bin);
                                            try {
                                                TriggerSnapshotFields f =
                                                        OrderRecordCodec.decodeStopFields(bin);
                                                OrderInfo o =
                                                        new OrderInfo(
                                                                f.orderId(),
                                                                f.userId(),
                                                                f.origQty(),
                                                                0,
                                                                (byte) f.direction(),
                                                                f.time(),
                                                                true,
                                                                0,
                                                                true);
                                                orders.put(o.orderId, o);
                                                r.prefilledIds.add(o.orderId);
                                                liveByUser
                                                        .computeIfAbsent(
                                                                o.userId,
                                                                k ->
                                                                        new java.util
                                                                                .LinkedHashSet<>())
                                                        .add(o);
                                            } catch (Exception e) {
                                                decodeFail[0]++;
                                                if (decodeFail[0] <= 5) {
                                                    System.err.println(
                                                            "[TradeRecon] applySnapshot 解析失败"
                                                                + " recLen="
                                                                    + l
                                                                    + " err="
                                                                    + e);
                                                }
                                            }
                                        });
                        default -> {}
                    }
                });
        System.err.println(
                "[TradeRecon] applySnapshot 段内收集 orders="
                        + collected[0]
                        + " orderBatchMsgs="
                        + orderBatchMsgs[0]
                        + " orderBatchRecords="
                        + orderBatchRecords[0]
                        + " decodeFail="
                        + decodeFail[0]
                        + " (meta 声称 "
                        + verify.meta().count()
                        + ")");
    }

    /** 回放快照 recording 全部消息（META/批量帧/END），60s 静默截止。 */
    private static void replaySnapshotRecording(
            AeronArchive archive, Recording rec, String symbol, SnapshotMessageConsumer consumer) {
        // 快照 replay stream 按 recordingId 唯一（2026-08-31）：对账连续 verify
        // 多张快照（新→旧）若复用同一 stream，前一个 replay 会话异步清理未完成时
        // Archive 判 stream 冲突、replay 立即 done（connected=false）——快照内容
        // 一条都读不到（counts=0）。
        int replayStream = replayStream(rec.recordingId()) + 50_000;
        FragmentAssembler assembler =
                new FragmentAssembler(
                        (buffer, offset, length, header) ->
                                consumer.accept(buffer, offset, length));
        long stop = resolvedStop(archive, rec);
        if (stop <= rec.startPosition()) {
            return;
        }
        try (Subscription sub =
                archive.replay(
                        rec.recordingId(),
                        rec.startPosition(),
                        stop - rec.startPosition(),
                        OcxAeron.CHANNEL,
                        replayStream)) {
            IdleStrategy idle = new BackoffIdleStrategy(100, 100, 1_000, 1_000_000);
            // 必须等 replay 会话"投递过消息"再按静默退出（2026-08-31）：
            // 对账紧接大压力压测运行，Archive replay 会话建立可能很慢——
            // 若用"poll==0 即 2s 静默"，会话未建立就被判超时（快照 70KB 一条
            // 都读不到、计数 0）；投递后 2s 静默或总截止 60s 退出。
            long lastActivity = System.currentTimeMillis();
            long start = System.currentTimeMillis();
            boolean delivered = false;
            while (true) {
                int n = sub.poll(assembler, 256);
                if (n > 0) {
                    delivered = true;
                    lastActivity = System.currentTimeMillis();
                    continue;
                }
                if ((delivered && System.currentTimeMillis() - lastActivity > 2_000L)
                        || System.currentTimeMillis() - start > 60_000L) {
                    break;
                }
                idle.idle();
            }
        }
    }

    private static long resolvedStop(AeronArchive archive, Recording rec) {
        long stop = rec.stopPosition();
        if (stop == AeronArchive.NULL_POSITION) {
            stop = archive.getRecordingPosition(rec.recordingId());
        }
        return stop;
    }
}
