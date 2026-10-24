# 客户端恢复协议（2026-09-01，P0-2）

> **本章解决**：leader 故障后客户端如何不丢单、如何核对自己的订单状态。
> **前置依赖**：06（服务端恢复）、02（orderId 幂等）。
> **读者前置**：幂等 / 重试。

> 迁移自 open-cex-docs `design-docs/21-client-recovery-protocol.md`（2026-09-05 迁入 open-cex-match/docs）。



## 1. 问题：leader 故障时客户端可能丢数据

撮合高可用（[04-ha.md §5.5](./04-ha.md)）在主机宕机后由
仲裁层选备机接管，但**主机宕机瞬间
尚未复制到备机的输入会丢**——这是 leader 单写 + 异步复制的固有窗口，Aeron
Cluster 文档同样承认（客户端必须自行决定如何处理：断开 FIX 会话、核对订单状态、
按 correlationId 重试）。**引擎不保证"客户端已发出的请求一定会被处理"，客户端
必须自恢复。**

## 2. 幂等键：引擎按 orderId 天然幂等

引擎核心（`MatchEngine`）对同一 orderId 重复创建返回
`duplicate_order_id` 拒绝，不会二次撮合。因此：

- **orderId 即幂等键**：客户端重试必须复用同一 orderId，重试被拒
  （duplicate_order_id）即证明"该请求已被接受/处理过"，转入状态核对；
- 撤单同理按 orderId 定位（撤不存在的单返回明确拒绝，不破坏状态）。

> 当前为单 ID 空间（OrderIn.id 即簿内 orderId，三段式发号规则见
> docs/02-data-structure.md §1.3 ）。**网关换发已落地（2026-09-01，ocx-match-gateway 模块）**：
> 客户端提交 clientOrderId → 网关三段式发号 + 幂等映射（持久化）→ 组标准
> OrderIn 帧发往 ingress，引擎链路零改动。

## 3. 客户端协议

### 3.1 请求

- 每个新单/撤单请求携带幂等键（orderId，或网关换发后的簿内 orderId）；
- 客户端本地记录 `幂等键 → 请求内容 → 发送时间 → 状态`，落盘（崩溃恢复用）。

### 3.2 超时与重试

- 请求发送后等待确认/拒绝；**超时窗口 ≥ 故障切换预算**（当前 5~7s，建议
  10s+ 余量）；
- 超时后**用同一幂等键重试**（幂等保证不会重复成交）；重试次数上限
  （建议 3 次）；
- 重试期间引擎可能已完成切换——请求会打到新 active，新 active 的状态机
  与旧主一致（陪跑确定性），重复订单按 duplicate 拒绝。

### 3.3 核对（重连/被拒后）

- 收到 `duplicate_order_id` 或重连成功后，**查询订单状态**：
  - 已成交/部分成交 → 以成交回报为准；
  - 已接受（簿内活跃）→ 正常；
  - 不存在/已拒绝 → 视为未发生，可安全重发新单（新幂等键）；
- 成交核对以 cold-log/Archive 的成交记录为唯一事实源
  （[04-ha.md §3.5 对账](./04-ha.md)）。

### 3.4 故障窗口语义

| 场景 | 客户端行为 |
|---|---|
| 请求未到达旧主 | 超时重试（幂等键不变）→ 新 active 正常处理 |
| 请求已到达但未复制到备机 | 旧主宕机后该请求丢失 → 超时重试 → 新 active 按首次处理 |
| 请求已处理但回执丢失 | 重试被 duplicate 拒绝 → 查询状态核对 |
| 请求处理中（成交已产生） | 查询状态/成交回报为准，不重复下单 |

## 4. 网关换发（已落地，ocx-match-gateway）

**HTTP API**（`GatewayBootstrap`，JDK HttpServer 零依赖）：

- `POST /orders?clientOrderId=..&symbol=..&side=..&type=..&price=..&qty=..
  [&userId=..&timeInForce=..&visibleQty=..]` —— 幂等换发：首次返回
  `{"clientOrderId","orderId","status":"accepted"}`；重复提交返回同一 orderId
  + `status:"duplicate"`（客户端转状态核对，不重复发单）；
- `GET /orders/{clientOrderId}` —— 映射查询。

**发号**（`OrderIdAllocator`）：Snowflake 化布局
`orderId = (seg << 42) | (issuerId << 34) | counter`，`seg` 为自 1970 起的
小时段（21 位，单调不减、时钟回拨取 max、段内耗尽提前推进），`counter` 每段从
1 重发；(seg, counter) 水位持久化（stateFile），重启同段从水位续发、跨段重发，
杜绝撞历史单。orderId 为**纯业务号**（不内嵌槽位）；引擎内部经
orderId→slot 映射 + 槽位 freelist 落位（规则见 02-data-structure.md §1.3  /
02-data-structure.md）。

**幂等映射**（`IdMapping`）：clientOrderId → orderId 双向映射 append-only
持久化，网关重启后幂等键恢复。

**参数**：`-Docx.gateway.http.port`（默认 8080）、
`-Docx.gateway.ingress.endpoint`（默认 localhost:11000）、
`-Docx.gateway.issuer.id`（默认 100）、`-Docx.gateway.state.dir`、
`-Docx.aeron.dir`（连引擎媒体驱动）。

**验证**：单测 7 个（发号/水位恢复/幂等/映射持久化/stream id 与引擎交叉校验）
+ 端到端冒烟（gateway-smoke-test.sh）：HTTP 下单 → 引擎撮合成交（probe 2 帧）
+ 幂等 duplicate + 网关重启映射恢复。

## 5. 待落地（剩余前置依赖）

- **订单状态查询接口**：客户端核对入口（当前网关只返回映射；在线状态
  查询接口待建，可先按对账工具离线核对）；
- **客户端 SDK/参考实现**：重试/核对状态机样例（可先按本协议文档实现）。


