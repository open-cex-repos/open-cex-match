# -*- coding: utf-8 -*-
"""
撮合引擎 SoA 订单簿可视化（教学 demo · 3Blue1Brown 风格 · 严格对齐文档/代码）
============================================================================

画面严格对应 open-cex-match 的真实结构与文档：
  - docs/02-data-structure.md  （§3.1 纵表 13 数组；§4 入簿/撤单/撮合走查）
  - docs/03-order-lifecycle.md            （§4 撮合主循环）
  - ocx-match-core/.../book/OrderSlots.java        （13 根数组 + freelist: next[] 复用）
  - ocx-match-core/.../book/LevelSlots.java        （价格档 + 档内 FIFO head/tail/next/prev）
  - ocx-match-core/.../book/OrderBook.java     （addOrderInternal / cancel / matchTaker）

示例数字直接采用文档 §3.1/§4 的教学快照（9001@70000 / 9002@70001 / 9003@70000 / 9004@70000），
走查 §4.1 入簿 → §4.3 撤单 → §4.4 撮合，与文档逐项一致。

省略项（生产引擎存在，此处为聚焦主流程而略，画面已加注解以免误解）：
  - 冰山 visibleQty/displayedQty 总是 0（非冰山）；
  - LevelSlots 的数组跳表索引 next[level][]（演示用价格升序链 firstLevel 表示）；
  - levelCache / orderIndex 哈希细节 / fillPool / STP / TIF 分支（撮合 caption 提及）。

运行：
  manim render orderbook_soa.py OrderBookSoADemo -qm
  manim render orderbook_soa.py OrderBookSoADemo -qm --format gif
"""
from manim import *

FONT = "PingFang SC"
CAP = 12  # 槽位容量（与 HTML 演示一致；生产 cap=2^23）

# ── 3Blue1Brown 配色 ────────────────────────────────────────────────────────
config.background_color = "#1f2233"
C_BLUE = "#58C4DD"        # 招牌蓝（BUY / 主色 / 标题）
C_RED = "#FC6255"         # 3b1b 红（SELL）
C_GREEN = "#83C167"       # 3b1b 绿（freelist 空闲链）
C_YELLOW = "#FFFF00"      # 高亮（活跃 / 已成交 eQ·cumQuote）
C_PURPLE = "#C586C0"      # 同价 FIFO 链（next/prev）
C_TEXT = "#E8E8E8"        # 主文字
C_DIM = "#9AA5B1"         # 次要文字
C_FREE_FILL = "#2A2C3A"   # 空闲槽填充（略亮于底）
C_FREE_TEXT = "#6E7B8B"   # 空闲文字（灰蓝）
C_HEAD = "#4A5568"        # 网格线
C_CORNER = 0.06           # 圆角

# 方向 → (代码 side, 颜色)
DIR_BUY, DIR_SELL = 0, 1


# ───────────────────────────────────────────────────────────────────────────
# MiniBook：忠实镜像 OrderSlots/LevelSlots/OrderBook 的核心机制
# （仅演示用；字段名与代码 1:1；qty 用可读小数，cumQuote=price×exec）
# ───────────────────────────────────────────────────────────────────────────
class MiniBook:
    def __init__(self):
        # —— OrderSlots 13 根数组（名与代码一致）——
        self.orderIds = [0] * CAP
        self.userIds = [0] * CAP
        self.prices = [0] * CAP
        self.origQtys = [0.0] * CAP
        self.execQtys = [0.0] * CAP
        self.cumQuoteQtys = [0.0] * CAP
        self.visibleQtys = [0] * CAP
        self.displayedQtys = [0] * CAP
        self.times = [""] * CAP
        self.next = [(i + 1 if i + 1 < CAP else -1) for i in range(CAP)]
        self.prev = [-1] * CAP
        self.free_head = 0                # freelist 链头（next[slot] 链接）
        self.levels = [-1] * CAP          # 存 LevelSlots 档位索引（演示映射回价）
        self.flags = [0] * CAP            # byte 位集：bit0 方向 / bit3 在簿 / TIF…
        self.used = 0
        # —— LevelSlots（SELL 簿，价格升序）——
        self.lvIdx = {}                   # price -> 档位索引
        self.lvPrice = {}                 # 档位索引 -> price
        self._lvn = 0
        self.lv = {}                      # 档位索引 -> {head,tail,count,normalRemaining}
        self.orderIndex = {}              # orderId -> slot

    # ── flags 位操作（对齐 OrderSlots 常量）──
    def set_dir(self, s, d):
        if d == DIR_BUY:
            self.flags[s] &= ~0x01
        else:
            self.flags[s] |= 0x01
    def is_buy(self, s):
        return (self.flags[s] & 0x01) == 0
    def set_inbook(self, s, v):
        if v:
            self.flags[s] |= 0x08
        else:
            self.flags[s] &= ~0x08

    # ── OrderSlots.alloc / free（freelist 复用 next[]）──
    def alloc(self):
        s = self.free_head
        if s == -1:
            return -1
        self.free_head = self.next[s]
        self.next[s] = -1
        self.prev[s] = -1
        self.levels[s] = -1
        return s

    def free(self, s):
        # 注意：真实 free() 只改 next[]，不动 prev[]（prev 留陈旧值，文档标 "—"）
        self.next[s] = self.free_head
        self.free_head = s

    # ── 档位索引（price -> LevelSlots 索引）──
    def _get_or_create_level(self, price):
        if price in self.lvIdx:
            return self.lvIdx[price]
        idx = self._lvn
        self._lvn += 1
        self.lvIdx[price] = idx
        self.lvPrice[idx] = price
        self.lv[idx] = {"head": -1, "tail": -1, "count": 0, "normalRemaining": 0.0}
        return idx

    # ── OrderBook.addOrderInternal（SELL 入 sellLevels）──
    def addOrder(self, orderId, userId, side, price, qty, t, tif=0):
        s = self.alloc()
        if s == -1:
            return None
        self.used += 1
        self.orderIds[s] = orderId
        self.userIds[s] = userId
        self.prices[s] = price
        self.origQtys[s] = qty
        self.execQtys[s] = 0.0
        self.cumQuoteQtys[s] = 0.0
        self.visibleQtys[s] = 0
        self.displayedQtys[s] = 0
        self.times[s] = t
        self.set_dir(s, side)
        self.set_inbook(s, True)           # flags: 方向 + 在簿 (+TIF)
        self.orderIndex[orderId] = s
        idx = self._get_or_create_level(price)
        self.levels[s] = idx
        L = self.lv[idx]
        if L["tail"] == -1:
            L["head"] = s
            L["tail"] = s
            self.next[s] = -1
            self.prev[s] = -1
        else:
            self.next[L["tail"]] = s
            self.prev[s] = L["tail"]
            L["tail"] = s
            self.next[s] = -1
        L["count"] += 1
        L["normalRemaining"] += qty
        return s

    # ── OrderBook.removeSlot（撤单/成交摘除共用）──
    def removeSlot(self, orderId):
        s = self.orderIndex.get(orderId, -1)
        if s == -1:
            return False
        if self.orderIds[s] != orderId or not (self.flags[s] & 0x08):
            return False               # 验身：orderIds[slot]==orderId 且仍在簿
        idx = self.levels[s]
        L = self.lv.get(idx)
        if L is not None:
            p, n = self.prev[s], self.next[s]
            if p == -1:
                L["head"] = n
            else:
                self.next[p] = n
            if n == -1:
                L["tail"] = p
            else:
                self.prev[n] = p
            self.next[s] = -1
            self.prev[s] = -1
            L["count"] -= 1
            if not (self.flags[s] & 0x02):   # 非冰山：扣 normalRemaining
                L["normalRemaining"] -= (self.origQtys[s] - self.execQtys[s])
            if L["count"] == 0:
                del self.lvIdx[self.lvPrice[idx]]
                del self.lvPrice[idx]
                del self.lv[idx]
        # releaseSlot：清 orderIds/levels/flags，free 还槽，摘 orderIndex
        oid = self.orderIds[s]
        self.orderIds[s] = 0
        self.levels[s] = -1
        self.flags[s] = 0
        self.used -= 1
        self.orderIndex.pop(oid, None)
        self.free(s)
        return True

    # ── OrderBook.matchTaker（BUY taker 吃 SELL 簿，升序逐档）──
    def matchTaker(self, orderId, userId, side, price, qty):
        remaining = qty
        for idx in sorted(self.lv.keys(), key=lambda i: self.lvPrice[i]):   # SELL 升序
            if remaining <= 0:
                break
            if self.lvPrice[idx] > price:        # BUY：档价 <= 买价才可成交
                break
            L = self.lv[idx]
            s = L["head"]
            while s != -1 and remaining > 0:
                nxt = self.next[s]
                maker_rem = self.origQtys[s] - self.execQtys[s]
                if maker_rem <= 0:
                    s = nxt
                    continue
                exec = min(remaining, maker_rem)
                self.execQtys[s] += exec
                self.cumQuoteQtys[s] += self.prices[s] * exec   # 成交额（price×exec）
                remaining -= exec
                if not (self.flags[s] & 0x02):
                    L["normalRemaining"] -= exec
                # maker 完全成交 → removeFilled（摘除 + 还槽）
                if self.origQtys[s] - self.execQtys[s] <= 0:
                    self._release_filled(idx, s)
                s = nxt
            if idx in self.lv and self.lv[idx]["count"] == 0:
                del self.lvIdx[self.lvPrice[idx]]
                del self.lvPrice[idx]
                del self.lv[idx]
        return remaining

    def _release_filled(self, idx, s):
        L = self.lv[idx]
        p, n = self.prev[s], self.next[s]
        if p == -1:
            L["head"] = n
        else:
            self.next[p] = n
        if n == -1:
            L["tail"] = p
        else:
            self.prev[n] = p
        self.next[s] = -1
        self.prev[s] = -1
        L["count"] -= 1
        oid = self.orderIds[s]
        self.orderIds[s] = 0
        self.levels[s] = -1
        self.flags[s] = 0
        self.used -= 1
        self.orderIndex.pop(oid, None)
        self.free(s)

    # ── 撮合逐单填充（镜像 matchTaker 的 per-maker 更新，不摘链）──
    def fillMaker(self, slot, exec):
        self.execQtys[slot] += exec
        self.cumQuoteQtys[slot] += self.prices[slot] * exec
        idx = self.levels[slot]
        L = self.lv.get(idx)
        if L is not None and not (self.flags[slot] & 0x02):
            L["normalRemaining"] -= exec

    def free_chain(self):
        chain, s = [], self.free_head
        seen = set()
        while s != -1 and s not in seen:
            seen.add(s)
            chain.append(s)
            s = self.next[s]
        return chain


# ───────────────────────────────────────────────────────────────────────────
# 渲染
# ───────────────────────────────────────────────────────────────────────────
class Cell:
    def __init__(self, text, w, h, fs=12):
        self.w = w
        self.h = h
        self.rect = RoundedRectangle(width=w, height=h, corner_radius=C_CORNER) \
            .set_stroke(C_HEAD, 0.5).set_fill(C_FREE_FILL, 0.85)
        self.text = Text(text, font=FONT, font_size=fs, color=C_FREE_TEXT)
        self.text.move_to(self.rect.get_center())
        self.group = VGroup(self.rect, self.text)
        self.val = text
        self.fs = fs

    def set_val(self, new_text, fill, fill_op, tcolor):
        anims = []
        if new_text != self.val:
            nt = Text(new_text, font=FONT, font_size=self.fs, color=tcolor).move_to(self.text.get_center())
            self.text.become(nt)
            self.val = new_text
        anims.append(self.rect.animate.set_fill(fill, opacity=fill_op))
        return anims


# 13 根数组（行），与代码字段顺序一致
FIELDS = [
    "orderIds", "userIds", "prices", "origQtys", "execQtys",
    "cumQuoteQtys", "visibleQtys", "displayedQtys", "times",
    "next", "prev", "levels", "flags",
]


def fmt(x):
    if isinstance(x, float) and x == int(x):
        return str(int(x))
    return str(x)


# ───────────────────────────────────────────────────────────────────────────
# 走查脚本（与 docs/01–04 §4 逐项对应；每个 tuple = 一步动画）
#   (mutator, cap_text, hl_slot, hl_rows, scan)
#   - mutator: 对 MiniBook 的变更（None=本步只换高亮/字幕，状态已在前面步写入）
#   - hl_slot: 高亮列（黄，强调"这一列=一张订单"）；None=不高亮列
#   - hl_rows: 高亮行组（紫带，强调某组数组，如 next/prev 的 FIFO 链、exec/cumQuote）
#   - scan   : 撮合扫描环是否点亮（圈住 LevelSlots 面板）
# ───────────────────────────────────────────────────────────────────────────
WALKTHROUGH = [
    # 初始
    (None, "初始：8 槽全在 freelist（next[] 串 0→1→…→7，freeHead=0）；orderIds 全 0。",
     None, None, False),
    # 9001（基础三步拆解：alloc / 写数组 / 挂档）
    (lambda b: b.addOrder(9001, 11, DIR_SELL, 70000, 1.5, "09:30:00.001"),
     "① alloc：freeHead 0→1，槽0 从空闲链摘出（绿→红）。", 0, None, False),
    (None, "② 写 13 数组：orderIds=9001 / prices=70000 / origQty=1.5 / flags=SELL·GTC·在簿。",
     0, ["orderIds", "prices", "origQtys", "flags"], False),
    (None, "③ 挂档70000：新建 LevelSlots，head=tail=槽0，normalRemaining=1.5。",
     0, ["next", "prev", "levels"], False),
    # 9002（新档）
    (lambda b: b.addOrder(9002, 22, DIR_SELL, 70001, 2.0, "09:30:00.002"),
     "④ 入簿 9002 @70001×2.0：alloc 槽1，写数组；新档70001。", 1, ["orderIds", "prices", "origQtys"], False),
    (None, "⑤ 挂档70001：SELL 升序 firstLevel 70000→70001（数组跳表）；head=槽1。",
     1, ["next", "prev", "levels"], False),
    # 9003（同档 FIFO 追加）
    (lambda b: b.addOrder(9003, 33, DIR_SELL, 70000, 1.0, "09:30:00.003"),
     "⑥ 入簿 9003 @70000×1.0：alloc 槽2，写数组；freeHead 2→3。", 2, ["orderIds", "prices", "origQtys"], False),
    (None, "⑦ 同档70000 队尾追加：FIFO 槽0→槽2（next/prev 链接）。", 2, ["next", "prev"], False),
    # 9004（同档 FIFO 追加）
    (lambda b: b.addOrder(9004, 44, DIR_SELL, 70000, 0.5, "09:30:00.004"),
     "⑧ 入簿 9004 @70000×0.5：alloc 槽3，写数组；freeHead 3→4。", 3, ["orderIds", "prices", "origQtys"], False),
    (None, "⑨ 档70000 队尾追加：槽0→槽2→槽3；同价 FIFO 链成型。", 3, ["next", "prev"], False),
    # 撤单 9003
    (lambda b: b.removeSlot(9003),
     "⑩ 撤单 9003：orderIndex[9003]=槽2（哈希 O(1) 命中）。", 2, None, False),
    (None, "⑪ 验身+摘链：orderIds[2]==9003 且 在簿；同价链 槽0→槽3 重连。", 2, ["next", "prev"], False),
    (None, "⑫ 还槽：free(next[2]=4, freeHead=2)，槽2 回空闲链（绿）。", 2, ["next"], False),
    # 撮合 BUY 2.0（增量：扫 / 吃 / 摘 / 吃 / 摘）
    (None, "⑬ 撮合 BUY 2.0 @≥70000：taker 扫 firstLevel=70000（价≤买价可成交）。", 0, None, True),
    (lambda b: b.fillMaker(0, 1.5),
     "⑭ 吃 head 9001×1.5：exec=1.5, cumQuote=70000×1.5=105000；taker 剩 0.5。", 0, ["execQtys", "cumQuoteQtys"], True),
    (lambda b: b.removeSlot(b.orderIds[0]),
     "⑮ 9001 满 → removeFilled：摘链（槽0 出 FIFO）+ 还槽（freeHead→0）；档70000 rem 0。", 0, ["next", "prev"], True),
    (lambda b: b.fillMaker(3, 0.5),
     "⑯ 续扫同档 head=9004×0.5：exec=0.5, cumQuote=35000；taker 剩 0。", 3, ["execQtys", "cumQuoteQtys"], True),
    (lambda b: b.removeSlot(b.orderIds[3]),
     "⑰ 9004 还槽；档70000 count=0 删除，firstLevel→70001（留 9002）。撮合完成。", 1, ["next", "prev"], False),
]


class OrderBookSoADemo(Scene):
    def construct(self):
        book = MiniBook()

        # 淡网格（3b1b 数学空间感）
        grid = NumberPlane(
            x_range=[-7, 7, 1], y_range=[-4, 4, 1],
            background_line_style={"stroke_color": C_BLUE, "stroke_width": 1, "stroke_opacity": 0.10},
            faded_line_style={"stroke_color": C_BLUE, "stroke_width": 1, "stroke_opacity": 0.04},
        ).scale(1.4)
        self.add(grid)

        # ── 标题卡 ──
        title = Text("撮合引擎 · SoA 订单簿（纵表）", font=FONT, font_size=36,
                     color=C_BLUE, weight=BOLD)
        underline = Line(title.get_left(), title.get_right(), color=C_BLUE, stroke_width=4) \
            .next_to(title, DOWN, buff=0.14)
        card = VGroup(title, underline)
        card.move_to(ORIGIN)
        self.play(Write(title), run_time=1.2)
        self.play(Create(underline), run_time=0.4)
        self.wait(0.5)
        self.play(card.animate.scale(0.55).to_edge(UP, buff=0.28), grid.animate.set_opacity(0.06),
                  run_time=0.7)

        # ── 图例 ──
        legend = self._legend()
        legend.next_to(title, DOWN, buff=0.32).to_edge(LEFT, buff=0.4)
        self.play(FadeIn(legend, shift=RIGHT * 0.3), run_time=0.5)

        # ── 纵表（行=数组，列=slot）──
        table = self._build_table()
        table["group"].move_to([-3.55, -0.35, 0])
        self.play(FadeIn(table["group"], shift=DOWN * 0.3), run_time=0.6)

        # 列/行中心必须在 move_to 之后按真实世界坐标复算，否则高亮错位
        tg = table["group"].get_center()
        table["col_centers"] = [table["group"][0][1 + s].get_center()[0] for s in range(CAP)]
        table["row_centers"] = {f: table["group"][1 + i].get_center()[1] for i, f in enumerate(FIELDS)}

        # 列高亮（背后矩形，强调"这一列 = 一张订单"）
        col_w = 0.62
        hl = RoundedRectangle(width=col_w + 0.06, height=table["h"] + 0.06, corner_radius=C_CORNER) \
            .set_stroke(C_YELLOW, 2.5).set_fill(C_YELLOW, 0.10)
        hl.move_to(table["col_centers"][0]).set_opacity(0)
        self.add(hl)

        # 行高亮（横带，强调"这一组数组"：next/prev 的 FIFO 链、exec/cumQuote 的成交额）
        row_hl = RoundedRectangle(width=table["group"].width + 0.10, height=0.41,
                                  corner_radius=C_CORNER) \
            .set_stroke(C_PURPLE, 2.2).set_fill(C_PURPLE, 0.12).set_opacity(0)
        row_hl.move_to([tg[0], table["row_centers"]["next"], 0])
        self.add(row_hl)

        # 右侧：LevelSlots 面板 + freelist
        levels_grp = self._levels_group(book)
        free_grp = self._freelist_group(book)
        self.play(FadeIn(levels_grp, shift=RIGHT * 0.3), FadeIn(free_grp), run_time=0.6)

        # 底部注解（持久，说明省略项以免误解）
        note = Text(
            "省略项(生产存在)：iceberg 显示量、LevelSlots 数组跳表索引 next[level][]、orderIndex 哈希/STP/TIF。"
            "BUY 簿(降序)与 SELL 簿(升序)对称，本 demo 仅演 SELL 侧。flags=byte 位集(展开显示)；levels 实存档位索引(显示价)。",
            font=FONT, font_size=10.5, color=C_DIM)
        note.to_edge(DOWN, buff=0.12).scale_to_fit_width(13.2)
        self.add(note)

        caption = Text("", font=FONT, font_size=14, color=C_BLUE).to_edge(DOWN, buff=0.62)
        self.add(caption)

        # 撮合扫描环（匹配阶段圈住 LevelSlots 面板）
        scan_ring = SurroundingRectangle(levels_grp, color=C_YELLOW, buff=0.12, stroke_width=2.5).set_opacity(0)
        self.add(scan_ring)

        def step(cap_text, hl_slot=None, hl_rows=None, scan=False):
            nonlocal levels_grp, free_grp, caption, scan_ring
            anims = []
            for s in range(CAP):
                for f in FIELDS:
                    cell = table["cells"][f][s]
                    occ = book.orderIds[s] != 0
                    if not occ:
                        fill, op, tc = C_FREE_FILL, 0.85, C_FREE_TEXT
                        if f == "next":
                            fill, op, tc = C_GREEN, 0.28, C_TEXT
                        elif f == "orderIds":
                            tc = C_FREE_TEXT
                        elif f == "prev":
                            tc = C_FREE_TEXT
                        else:
                            tc = C_FREE_TEXT
                    else:
                        d = DIR_BUY if book.is_buy(s) else DIR_SELL
                        base = C_BLUE if d == DIR_BUY else C_RED
                        fill, op, tc = base, 0.20, C_TEXT
                        if f == "execQtys" and book.execQtys[s] > 0:
                            tc = C_YELLOW
                        if f == "cumQuoteQtys" and book.cumQuoteQtys[s] > 0:
                            tc = C_YELLOW
                        if f == "next" or f == "prev":
                            fill, op, tc = C_PURPLE, 0.20, C_TEXT
                    anims += cell.set_val(self._cell_val(book, s, f), fill, op, tc)
            if hl_slot is not None:
                anims.append(hl.animate.set_opacity(1).move_to(table["col_centers"][hl_slot]))
            else:
                anims.append(hl.animate.set_opacity(0))
            if hl_rows:
                ys = [table["row_centers"][f] for f in hl_rows]
                yc = (min(ys) + max(ys)) / 2
                rh = (max(ys) - min(ys)) + 0.41
                anims.append(row_hl.animate.set_opacity(1).move_to([tg[0], yc, 0]).set_height(rh, stretch=True))
            else:
                anims.append(row_hl.animate.set_opacity(0))
            self.play(*anims, run_time=0.55)
            # 面板整组替换（freeHead / head 标记随之更新）
            self.remove(levels_grp)
            levels_grp = self._levels_group(book)
            self.add(levels_grp)
            self.remove(free_grp)
            free_grp = self._freelist_group(book)
            self.add(free_grp)
            # 扫描环跟随新面板
            self.remove(scan_ring)
            scan_ring = SurroundingRectangle(levels_grp, color=C_YELLOW, buff=0.12, stroke_width=2.5) \
                .set_opacity(1 if scan else 0)
            self.add(scan_ring)
            new_cap = Text(cap_text, font=FONT, font_size=14, color=C_BLUE).to_edge(DOWN, buff=0.62)
            self.play(FadeOut(caption, run_time=0.15), FadeIn(new_cap, run_time=0.15))
            caption = new_cap
            self.wait(0.6)

        # ── 走查（与 docs/01–04 §4 逐项对应；增量演示 alloc→写→挂档 / 撤单 / 撮合）──
        for mutator, cap_text, hl_slot, hl_rows, scan in WALKTHROUGH:
            if mutator is not None:
                mutator(book)
            step(cap_text, hl_slot=hl_slot, hl_rows=hl_rows, scan=scan)
            self.wait(0.25)

        end = Text("全程零对象构造：alloc/free 复用 freelist，撮合链直持 slot。与 docs/01–04 对应。",
                   font=FONT, font_size=14, color=C_GREEN).to_edge(DOWN, buff=0.45)
        self.play(grid.animate.set_opacity(0.04), FadeOut(note, run_time=0.3), FadeOut(caption, run_time=0.3),
                  FadeIn(end, shift=UP * 0.2), run_time=0.7)
        self.wait(1.2)

    # ── 单元格取值（严格对应代码/文档的存储语义）──
    def _cell_val(self, book, s, f):
        occ = book.orderIds[s] != 0
        if f == "orderIds":
            return "0" if not occ else str(book.orderIds[s])
        if f == "userIds":
            return "—" if not occ else str(book.userIds[s])
        if f == "prices":
            return "—" if not occ else str(book.prices[s])
        if f == "origQtys":
            return "—" if not occ else fmt(book.origQtys[s])
        if f == "execQtys":
            return "—" if not occ else fmt(book.execQtys[s])
        if f == "cumQuoteQtys":
            return "—" if not occ else fmt(book.cumQuoteQtys[s])
        if f == "visibleQtys":
            return "—" if not occ else str(book.visibleQtys[s])
        if f == "displayedQtys":
            return "—" if not occ else str(book.displayedQtys[s])
        if f == "times":
            if not occ:
                return "—"
            # 文档时间戳 "09:30:00.001" 拆双行以适配窄格：行1=mm:ss 行2=.ms
            t = book.times[s]
            if len(t) >= 9 and t[2] == ":":
                return t[3:8] + "\n" + t[8:]      # "30:00\n.001"
            return t
        if f == "next":
            if not occ:
                return "·" if book.next[s] == -1 else str(book.next[s])
            return "·" if book.next[s] == -1 else str(book.next[s])
        if f == "prev":
            if not occ:
                return "—"               # 空闲格 prev 陈旧，文档标 "—"
            return "·" if book.prev[s] == -1 else str(book.prev[s])
        if f == "levels":
            if book.levels[s] == -1:
                return "-1"
            return "档" + str(book.lvPrice[book.levels[s]]) if book.levels[s] in book.lvPrice else "-1"
        if f == "flags":
            if not occ:
                return "0"
            dir_s = "BUY" if book.is_buy(s) else "SELL"
            # 双行：方向 / (TIF+在簿) — 对应 flags 位集展开
            return dir_s + "\nGTC/在簿"
        return "?"

    # ── 构件 ──
    def _legend(self):
        items = VGroup()
        for txt, col in [("BUY(买)", C_BLUE), ("SELL(卖)", C_RED),
                         ("空闲槽", C_FREE_FILL), ("freelist 指针", C_GREEN),
                         ("同价 FIFO 链", C_PURPLE), ("已成交 eQ/cumQuote", C_YELLOW)]:
            sw = RoundedRectangle(width=0.26, height=0.26, corner_radius=C_CORNER) \
                .set_fill(col, 0.85 if col != C_FREE_FILL else 0.9).set_stroke(WHITE, 0.25)
            t = Text(txt, font=FONT, font_size=11, color=C_TEXT)
            items.add(VGroup(sw, t).arrange(RIGHT, buff=0.12))
        return items.arrange(RIGHT, buff=0.36)

    def _build_table(self):
        cw, ch = 0.62, 0.36
        cells = {}
        col_centers = []
        grp = VGroup()
        # 表头
        header = VGroup()
        spacer = RoundedRectangle(width=1.45, height=ch * 0.85, corner_radius=C_CORNER) \
            .set_stroke(C_BLUE, 0.5).set_fill(C_BLUE, 0.10)
        header.add(spacer)
        for s in range(CAP):
            c = RoundedRectangle(width=cw, height=ch * 0.85, corner_radius=C_CORNER) \
                .set_stroke(C_BLUE, 0.6).set_fill(C_BLUE, 0.16)
            t = Text("S" + str(s), font=FONT, font_size=11, color=C_BLUE).move_to(c.get_center())
            header.add(VGroup(c, t))
        header.arrange(RIGHT, buff=0.02)
        grp.add(header)
        col_centers = [header[1 + s].get_center()[0] for s in range(CAP)]  # S0..S7 中心 x

        for f in FIELDS:
            row = VGroup()
            lab = RoundedRectangle(width=1.45, height=ch, corner_radius=C_CORNER) \
                .set_stroke(C_HEAD, 0.4).set_fill(C_FREE_FILL, 0.55)
            ltxt = Text(f, font=FONT, font_size=10, color=C_TEXT).move_to(lab.get_center())
            row.add(VGroup(lab, ltxt))
            row_cells = {}
            # flags 双行 "GTC/在簿" 最宽 → 字号再小一档；times 折两行稍宽
            val_fs = 8 if f == "flags" else (9 if f == "times" else 10)
            for s in range(CAP):
                init_val = "—" if f != "orderIds" else "0"
                cell = Cell(init_val, cw, ch, fs=val_fs)
                row.add(cell.group)
                row_cells[s] = cell
            row.arrange(RIGHT, buff=0.02)
            grp.add(row)
            cells[f] = row_cells
        grp.arrange(DOWN, buff=0.012)
        return {"group": grp, "cells": cells, "col_centers": col_centers, "h": grp.height}

    def _levels_group(self, book):
        box = VGroup()
        label = Text("SELL 簿 LevelSlots（价格升序）", font=FONT, font_size=13, color=C_RED)
        box.add(label)
        if not book.lv:
            box.add(Text("（空）", font=FONT, font_size=12, color=C_FREE_TEXT))
        else:
            prices = sorted(book.lvPrice[i] for i in book.lv.keys())
            # firstLevel 链（升序）
            chain_arrow = VGroup()
            for i, p in enumerate(prices):
                t = Text("档" + str(p), font=FONT, font_size=12, color=C_RED)
                chain_arrow.add(t)
                if i < len(prices) - 1:
                    chain_arrow.add(Text("→", font=FONT, font_size=14, color=C_DIM))
            chain_arrow.arrange(RIGHT, buff=0.08)
            box.add(chain_arrow)
            for p in prices:
                idx = book.lvIdx[p]
                L = book.lv[idx]
                chain = []
                s = L["head"]
                while s != -1:
                    chain.append(s)
                    s = book.next[s]
                chips = VGroup()
                for i, cs in enumerate(chain):
                    chip = RoundedRectangle(width=0.5, height=0.38, corner_radius=C_CORNER) \
                        .set_fill(C_RED, 0.26).set_stroke(C_RED, 0.8)
                    tx = Text("槽" + str(cs), font=FONT, font_size=11, color=C_TEXT).move_to(chip.get_center())
                    chips.add(VGroup(chip, tx))
                    if i < len(chain) - 1:
                        chips.add(Text("→", font=FONT, font_size=14, color=C_PURPLE))
                chips.arrange(RIGHT, buff=0.05)
                # FIFO head 标记：指向同价链首单（演示 matchTaker 吃 head 的语义）
                if chain:
                    hc = chips[0].get_center()
                    head_tag = Triangle().scale(0.13).set_fill(C_PURPLE, 1).set_stroke(width=0) \
                        .move_to([hc[0], hc[1] + 0.30, 0])
                    chips.add(head_tag)
                info = Text(f"档{p}: count={L['count']}  rem={fmt(L['normalRemaining'])}",
                            font=FONT, font_size=11, color=C_DIM)
                inner = VGroup(info, chips).arrange(DOWN, buff=0.04, aligned_edge=LEFT)
                sur = SurroundingRectangle(inner, color=C_RED, buff=0.10, stroke_width=1.3)
                box.add(VGroup(sur, inner).arrange(DOWN, buff=0))
        box.arrange(DOWN, buff=0.16, aligned_edge=LEFT)
        box.scale(0.82)
        box.to_edge(RIGHT, buff=0.4)
        box.move_to([box.get_center()[0], 1.3, 0])  # 右上区，显式定 y，避免与 freelist 面板重叠
        return box

    def _freelist_group(self, book):
        chain = book.free_chain()
        g = VGroup()
        g.add(Text("freelist（空闲槽，next[] 指针）", font=FONT, font_size=12, color=C_GREEN))
        chips = VGroup()
        for i, s in enumerate(chain):
            chip = RoundedRectangle(width=0.5, height=0.38, corner_radius=C_CORNER) \
                .set_fill(C_GREEN, 0.28).set_stroke(C_GREEN, 0.9)
            tx = Text("槽" + str(s), font=FONT, font_size=11, color=C_TEXT).move_to(chip.get_center())
            chips.add(VGroup(chip, tx))
            if i < len(chain) - 1:
                chips.add(Text("→", font=FONT, font_size=14, color=C_GREEN))
        chips.arrange(RIGHT, buff=0.05)
        g.add(chips)
        g.arrange(DOWN, buff=0.07, aligned_edge=LEFT)
        g.scale(0.82)
        # freeHead 指针：指向空闲链首（演示 alloc/free 时头指针滑动）
        if chain:
            hc = chips[0].get_center()
            tri = Triangle().scale(0.16).set_fill(C_GREEN, 1).set_stroke(width=0) \
                .move_to([hc[0], hc[1] + 0.30, 0])
            lbl = Text("freeHead", font=FONT, font_size=9, color=C_GREEN) \
                .next_to(tri, UP, buff=0.01)
            g.add(VGroup(tri, lbl))
        g.to_edge(RIGHT, buff=0.4)
        g.move_to([g.get_center()[0], -2.3, 0])  # 右下区，显式定 y，避免与 caption/note 及 levels 面板重叠
        return g
