# 交互式 SoA 订单簿可视化 · 测试用例

被测对象：`docs/viz/orderbook_soa.html`（单文件、原生 Canvas 2D、零外部依赖）。
验证方式：**真实浏览器（agent-browser / Chromium）驱动**，`canvas.getImageData` 做像素级断言 + DOM 断言 + 引擎最终态对比 Python 参考。所有断言均可重复执行（`node run_tests.mjs`）。

## 需求与断言矩阵

| 编号 | 需求 | 断言 | 验证手段 |
|---|---|---|---|
| R1 | 页面正常加载、无 JS 错误 | `canvas#cv` 存在且 `width>0`；打开时无 console error | DOM / 运行日志 |
| R2 | 初始态：12 槽全在 freelist | `free_chain == [0,1,2,…,11]`（CAP=12），`free_head==0` | 引擎状态 |
| R3 | 导航交互（按钮/键盘） | 点 `#next`×2 → `#counter=="步骤 3 / 47"`；`press ArrowRight` → `"步骤 4 / 47"`；`#reset` → `"步骤 1 / 47"`；首步时 `#prev` disabled、末步时 `#next` disabled | 真实点击/按键 + DOM 文本 |
| R4 | 黄色列高亮定位正确（核心回归点） | 对 `hlSlot=s` 的步骤：`colX(s)-1` 处竖向扫描出现黄色描边像素（>0）；而另一列 `(s+3)%8` 的同位置扫描为 0（证明不是整表宽高亮，且随步骤移动到正确槽） | 像素采样 |
| R5 | 紫色行带可见（z-index 修复点） | 对有 `hlRows` 的步骤：在**空闲列**的 `hlRows` 行采样，其绿色主导度（G−min(R,B)）比同列步骤 0 基线下降 >3（紫带 alpha 0.18 叠加把绿底往中性拉；带画在单元格之上，未被实色空闲格覆盖） | 像素采样（带内 vs 基线差分） |
| R6 | 撮合扫描环（scan 步骤） | 第一幕 14–17 与第二幕若干步（`scanBook` 指定被扫簿）：右侧面板列 x≈RIGHT_X−7 出现黄色描边像素（>0） | 像素采样 |
| R7 | 引擎保真（与 docs/code 一致，第一幕末 idx19） | `free_chain==[3,0,2,4,5,…,21]`、`exec==[1.5,0,0,0.5,0,…]`（长 12）、`cumQuote==[105000,0,0,35000,0,…]`、`used==1`、SELL 档 `[70001]` | 引擎状态 vs Python `_pyref.py` |
| R8 | orderIndex 判重/登记/清除/拒绝（第一幕） | 步 1 判重 miss 簿零改动；步 3 登记 `9001→槽0`；步 11 定位命中且簿未动；步 13 清除；步 19 拒绝步与步 18 全状态快照相等、`contains(9002)==true` | 引擎状态（步进差分） |
| R9 | 第二幕双簿分支终态（A–E，idx46） | `used==3`；BUY 档 `[70000]`（含 9007 resting）；SELL 档 `[70003,70004]`（9105/9111）；FOK(25)/POST_ONLY(26) 拒绝步簿零改动（步进快照相等）；idx27 POST_ONLY→GTC resting 入 BUY 簿；idx35 9103 部分成交留队（exec0.4/rem1.1）；idx38 9009 resting 入 BUY；idx45 batchCancel(u100) 后 9121-23 不在簿而 u200 9111 保留 | 引擎状态 |

## 逐步骤高亮规格（STEPS，0-based，共 47 步）

### 第一幕（步 0–19，基础流程：判重 → 入簿 → FIFO → 撤单 → 撮合 → 重复单拒绝）

| 步 | hlSlot | hlRows | scan | orderIndex 动作 | 说明 |
|---|---|---|---|---|---|
| 0 | — | — | — | — | 初始（12 槽 freelist；布局：左 12 列表格 / 右列 freelist→SELL→BUY→orderIndex） |
| 1 | — | — | — | getmiss 9001 | 判重闸门：contains(9001) miss → 放行 |
| 2 | 0 | — | — | — | alloc 槽0 |
| 3 | 0 | orderIds,prices,origQtys,flags | — | put 9001→0 | 写数组 + orderIndex 登记 |
| 4 | 0 | next,prev,levels | — | — | 挂档 70000 |
| 5 | 1 | orderIds,prices,origQtys | — | put 9002→1 | 入簿 9002（判重 miss） |
| 6 | 1 | next,prev,levels | — | — | 挂档 70001 |
| 7 | 2 | orderIds,prices,origQtys | — | put 9003→2 | 入簿 9003（判重 miss） |
| 8 | 2 | next,prev | — | — | FIFO 追加 9003 |
| 9 | 3 | orderIds,prices,origQtys | — | put 9004→3 | 入簿 9004（判重 miss） |
| 10 | 3 | next,prev | — | — | 队尾追加 9004 |
| 11 | 2 | — | — | gethit 9003 | 撤单定位：orderIndex[9003]=槽2 命中，簿未动 |
| 12 | 2 | next,prev | — | — | 摘链（removeFromLevel，槽仍占用） |
| 13 | 2 | next | — | remove 9003 | 还槽 + orderIndex.remove |
| 14 | 0 | — | SELL | — | 撮合扫描 70000 |
| 15 | 0 | execQtys,cumQuoteQtys | SELL | — | fillMaker(0,1.5) |
| 16 | 0 | next,prev | SELL | remove 9001 | removeFilled 9001 |
| 17 | 3 | execQtys,cumQuoteQtys | SELL | — | fillMaker(3,0.5) |
| 18 | 1 | next,prev | — | remove 9004 | removeFilled 9004（剩 9002） |
| 19 | — | — | — | reject 9002 | 重复单 9002 被拒：簿零改动 |

### 第二幕（步 20–46，双簿撮合主循环 / 部分成交 / resting / TIF / 批量撤单）

| 步 | 内容 | 对应源码 |
|---|---|---|
| 20 | 幕间：现状盘点 + BUY 簿面板登场 | — |
| 21–24 | 铺 SELL maker：9101/9102(u100, 档70002 FIFO)、9103(u100, 70003)、9111(u200, 70004) | `addOrderInternal` |
| 25 | FOK @70004×7：可成交深度 6.3<7 → 整拒，簿零改动 | `MatchService` L55-61 |
| 26 | POST_ONLY @70004 会对侧成交 → 整拒 | L63-68 |
| 27 | POST_ONLY @70000 无交叉 → 转 GTC → remaining resting 入 BUY 簿 | L69 + L74-83 |
| 28–35 | BUY GTC 9008 @70003×4.2 扫 SELL：逐档(70001→70002→70003) 逐 maker 撮合，每档满即 removeLevel（A）；taker 剩 0.4 时在 9103 上部分成交留队（C） | `matchTaker` L340-441 |
| 36–37 | BUY GTC 9009 @70003×1.6 续扫同 maker → 9103 全成摘除；下档 70004 价不可成交 | `matchTaker` L345 |
| 38 | resting：9009 剩余 0.5 GTC 自动入 BUY 簿（B） | L74-83 |
| 39–40 | 对称：SELL 9105 扫 BUY 簿（价格降序），吃 9009 → removeLevel；下档 70000 不可成交 | `matchTaker` 反向簿 |
| 41 | resting：9105 剩余 0.7 入 SELL 簿（对称 B） | L74-83 |
| 42–44 | 铺 u100 maker 9121/9122/9123（70005 FIFO / 70006） | `addOrderInternal` |
| 45 | batchCancel(u100) 全簿扫描撤 3 单；u200 9111 保留 | `OrderBook.batchCancel` L299-319 |
| 46 | 收尾盘点：SELL=9105×0.7+9111×1.0；BUY=9007×1.0 | — |

## 运行

```bash
cd docs/viz/tests
node run_tests.mjs
```

首次运行若报 Chromium 缺失：`agent-browser install`（macOS）或 `agent-browser install --with-deps`（Linux）。
截图存于 `viz/tests/shot_step3_put9001.png`、`shot_step27_buyRest.png`、`shot_step35_partial.png`、`shot_step45_batch.png`，供人工核对。
