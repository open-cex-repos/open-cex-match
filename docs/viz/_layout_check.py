# -*- coding: utf-8 -*-
"""布局校验：逐步骤复现 WALKTHROUGH，检测文字越界/重叠；并校验增量撮合与 matchTaker 等价。
不渲染，只做几何核查，用于确认视频文字不重叠、不超框、且与引擎语义一致。"""
from manim import Text, MathTex, Tex, Line, VGroup, BOLD, DOWN, UP, LEFT, RIGHT
import sys
sys.path.insert(0, ".")
import orderbook_soa as M

S = M.OrderBookSoADemo
FB, FT = 7.11, 4.0  # 16:9 半宽/半高


def collect_texts(m, acc):
    if isinstance(m, (Text, MathTex, Tex)):
        acc.append(m)
    for sub in m.submobjects:
        collect_texts(sub, acc)


def aabb(m):
    return (m.get_left()[0], m.get_bottom()[1], m.get_right()[0], m.get_top()[1])


def build_groups(book, cap_text):
    title = Text("撮合引擎 · SoA 订单簿（纵表）", font=M.FONT, font_size=36, color=M.C_BLUE, weight=BOLD)
    underline = Line(title.get_left(), title.get_right(), color=M.C_BLUE, stroke_width=4).next_to(title, DOWN, buff=0.14)
    card = VGroup(title, underline)
    card.scale(0.55).to_edge(UP, buff=0.28)
    legend = S._legend(S)
    legend.next_to(title, DOWN, buff=0.32).to_edge(LEFT, buff=0.4)
    table = S._build_table(S)
    table["group"].move_to([-3.55, -0.35, 0])
    levels_grp = S._levels_group(S, book)
    free_grp = S._freelist_group(S, book)
    note = Text("省略项(生产存在)：iceberg 显示量、LevelSlots 数组跳表索引 next[level][]、orderIndex 哈希/STP/TIF。"
                 "BUY 簿(降序)与 SELL 簿(升序)对称，本 demo 仅演 SELL 侧。flags=byte 位集(展开显示)；levels 实存档位索引(显示价)。",
                 font=M.FONT, font_size=10.5, color=M.C_DIM)
    note.to_edge(DOWN, buff=0.12).scale_to_fit_width(13.2)
    caption = Text(cap_text, font=M.FONT, font_size=14, color=M.C_BLUE).to_edge(DOWN, buff=0.62)
    return [card, legend, table["group"], levels_grp, free_grp, note, caption]


book = M.MiniBook()
total_oob = 0
total_ovl = 0
for idx, (mutator, cap_text, hl_slot, hl_rows, scan) in enumerate(M.WALKTHROUGH):
    if mutator is not None:
        mutator(book)
    groups = build_groups(book, cap_text)
    texts = []
    for g in groups:
        collect_texts(g, texts)
    boxes = [aabb(t) for t in texts]
    oob = [t for t, (x0, y0, x1, y1) in zip(texts, boxes)
           if x0 < -FB - 0.01 or x1 > FB + 0.01 or y0 < -FT - 0.01 or y1 > FT + 0.01]
    ovl = []
    for i in range(len(texts)):
        for j in range(i + 1, len(texts)):
            ox = min(boxes[i][2], boxes[j][2]) - max(boxes[i][0], boxes[j][0])
            oy = min(boxes[i][3], boxes[j][3]) - max(boxes[i][1], boxes[j][1])
            if ox > 0.04 and oy > 0.04:
                ovl.append((getattr(texts[i], 'original_text', '?')[:16], getattr(texts[j], 'original_text', '?')[:16]))
    if oob or ovl:
        print(f"步骤{idx + 1:2d} OOB={len(oob)} OVL={len(ovl)}  cap={cap_text[:20]}")
        for t in oob:
            print("   OOB", getattr(t, 'original_text', '?')[:30], tuple(round(v, 2) for v in aabb(t)))
        for a, b in ovl[:12]:
            print("   OVL", a, "<>", b)
    total_oob += len(oob)
    total_ovl += len(ovl)

print(f"\n合计：OOB={total_oob}  OVL={total_ovl}  步骤数={len(M.WALKTHROUGH)}")

# 正确性校验：增量走查的结果应与 matchTaker 一次性撮合等价
ref = M.MiniBook()
for oid, price, qty in [(9001, 70000, 1.5), (9002, 70001, 2.0), (9003, 70000, 1.0), (9004, 70000, 0.5)]:
    ref.addOrder(oid, 0, M.DIR_SELL, price, qty, "t")
ref.removeSlot(9003)
ref.matchTaker(9005, 0, M.DIR_BUY, 70000, 2.0)
print("正确性：exec等价=", book.execQtys == ref.execQtys,
      " cumQuote等价=", book.cumQuoteQtys == ref.cumQuoteQtys,
      " free等价=", book.free_chain() == ref.free_chain(),
      " used等价=", book.used == ref.used)
