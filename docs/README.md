# 撮合引擎文档

撮合引擎是高复杂度模块，这套文档把它讲成**一本书**：章节之间有逻辑依赖、有阅读
路径，帮助人类工程师与运维人员理解它——从「订单怎么住」到「怎么成交」，再到
「崩了怎么追回来」，最后到「怎么部署、监控、调优」。

> **定位**：面向技术人员（研发、维护、深度接入与排障的工程师）。正文默认按实现
> 语言写作，可直接出现类名、字段、位与算法；演示数字必须可核对。每章 = 设计决策
> + 实现详解（数字案例走查 / 代码锚点 / 验证命令 / 常见误解）合一。

## 阅读路径

按编号顺序读，知识逐章累积：**核心机制**（01–03）→ **可靠性与恢复**（04–07）→
**运行与观测**（08–12）。每章开头有「本章解决 / 前置依赖 / 读者前置」导读块，读前
先看它确认自己是否具备前置知识。

## 目录

| 编号 | 章节 | 一句话定位 | 前置依赖 |
| --- | --- | --- | --- |
| 01 | [01-architecture.md](./01-architecture.md) | 引擎是什么、五进程如何协同、数据怎么流 | 无（全书起点） |
| 02 | [02-data-structure.md](./02-data-structure.md) | 订单怎么住、orderId 怎么发、档怎么组织 | 01 |
| 03 | [03-order-lifecycle.md](./03-order-lifecycle.md) | 一笔订单怎么成交 | 02 |
| 04 | [04-ha.md](./04-ha.md) | 主机挂了备机怎么接管 | 01、03 |
| 05 | [05-recording-decoupling.md](./05-recording-decoupling.md) | 入站 journal 怎么录、怎么从撮合解耦 | 01、04 |
| 06 | [06-crash-recovery.md](./06-crash-recovery.md) | 崩溃后状态怎么追回来 | 02、05、04 |
| 07 | [07-client-recovery-protocol.md](./07-client-recovery-protocol.md) | 客户端怎么不丢单 | 06、02 |
| 08 | [08-observability-deployment.md](./08-observability-deployment.md) | 怎么监控、怎么部署 | 01 |
| 09 | [09-monitoring-alerting.md](./09-monitoring-alerting.md) | 主备高可用的告警怎么接 | 04、08 |
| 10 | [10-performance.md](./10-performance.md) | 能跑多快、瓶颈在哪 | 01–03、05 |
| 11 | [11-runtime-runbook.md](./11-runtime-runbook.md) | 怎么启动、验证、排障 | 08、09 |
| 12 | [12-stress-test-report.md](./12-stress-test-report.md) | 历次压测记录（附录，可跳读） | 10 |

配套资源：`diagrams/`（`match-engine-class.mermaid`、`match-engine-sequence.mermaid`）、
`imgs/`（`addorder-flow.svg`、`match-engine-layered-overview.svg`、`wal-recovery-flow.svg`）、
`viz/`（SoA 订单簿交互式讲解可视化，配合 02/03 章走查）。

## 写作约定

1. 一份主题一份文档，编号连续（`NN-topic.md`，依赖在前、专题在后）；
2. 每份文档：章导读（解决什么 / 前置依赖 / 读者前置）+ 设计决策（章节连续编号
   `1.` / `1.1`）+ 实现详解（数字案例走查、代码锚点、可执行验证、常见误解）；
3. 正文直接引用代码锚点与实现细节；演示数字必须可核对；复杂过程拆到单步可验证；
4. 跨篇引用以文件名为前缀（如 `02-data-structure.md §1.3`）；
5. 验证命令必须真实可跑。

> 迁移说明：本目录文档 2026-09-05 自 `open-cex-docs`（design-docs 05/18–22 及
> references）迁入并整合；`open-cex-docs` 仅保留指向本目录的单向链接。
