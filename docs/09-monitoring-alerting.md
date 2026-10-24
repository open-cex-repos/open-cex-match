# 监控告警接入（2026-09-01，P1）

> **本章解决**：主备高可用的监控指标与告警如何接入。
> **前置依赖**：04（主备）、08（可观测）。
> **读者前置**：Prometheus / Alertmanager。

> 迁移自 open-cex-docs `design-docs/22-monitoring-alerting.md`（2026-09-05 迁入 open-cex-match/docs）。



## 1. 指标来源

### 仲裁组（每个仲裁节点一个 Prometheus 端点，`-Docx.ha.metrics.port=970X`）

每个 `HaRaftCoordinatorMain` 进程暴露 `/metrics`（JDK HttpServer，零依赖）。
编排脚本默认 9701/9702/9703（arb1/arb2/arb3）。

| 指标 | 类型 | 含义 |
|---|---|---|
| `ocx_ha_arb_leader{node}` | gauge | 1 = 本节点是 Raft leader |
| `ocx_ha_arb_epoch{node}` | gauge | 当前 fencing epoch |
| `ocx_ha_arb_active_instance{node}` | gauge | 当前 active 实例 id |
| `ocx_ha_arb_lease_expired{node}` | gauge | 1 = active 租约已过期 |
| `ocx_ha_arb_member_position{node,instance}` | gauge | 各实例复制位置（备机追赶进度） |
| `ocx_ha_arb_standby_lag{node}` | gauge | active position − 最高备机 position（字节） |
| `ocx_ha_arb_promote_total{node}` | counter | 提升（接管）次数 |
| `ocx_ha_arb_transient_error_total{node}` | counter | 瞬态错误次数 |

### 引擎（现有 sidecar，`ocx-metrics-exporter`）

每实例一个 `/metrics`（host 9201、备机 9202/9203），含 `match_engine_up`
（0 = 停机/self-fence）、`match_trades_total`、`match_order_book_size` 等。

## 2. Prometheus 抓取配置

```yaml
scrape_configs:
  - job_name: ocx-ha-arbitration
    static_configs:
      - targets: ["host1:9701", "host2:9702", "host3:9703"]
        labels: { component: arbitration }
  - job_name: ocx-match-engine
    static_configs:
      - targets: ["host1:9201", "host2:9202", "host3:9203"]
        labels: { component: match-engine }

rule_files:
  - /etc/prometheus/prometheus-ha-alerts.yml   # ocx-match-core/prometheus-ha-alerts.yml
```

## 3. 告警规则（prometheus-ha-alerts.yml）

| 规则 | 触发条件 | 级别 | 含义 |
|---|---|---|---|
| `HaArbitrationNoLeader` | 30s 无 leader | critical | 仲裁失效，不可接管 |
| `HaArbitrationQuorumDegraded` | ≥2 仲裁节点不可达 | critical | 无多数派，Raft 不可提交 |
| `HaLeaseExpired` | 租约过期 10s | warning | active 心跳丢失，即将提升 |
| `HaPromotionOccurred` | 1m 内提升 >0 | warning | 发生主备切换，需人工确认 |
| `HaArbitrationTransientError` | 5m 内瞬态错误 >0 | warning | 检查节点日志 |
| `HaStandbyLagBehind` | 备机追赶延迟 >1MB 持续 1m | warning | 切换丢尾部输入风险 |
| `HaEngineDown` | 引擎 up=0 持续 1m | critical | 引擎停机/self-fence |

`HaStandbyLagBehind` 的 1MB 阈值是示例：按生产 TPS 与可接受的丢单窗口换算
（输入行约 264B，1MB ≈ 3800 单；若要求 <100 单，阈值设为 26400）。

## 4. Alertmanager 路由建议

```yaml
route:
  group_by: [alertname]
  routes:
    - matchers: [severity = critical]
      receiver: oncall-pager
    - matchers: [severity = warning]
      receiver: trading-team
```

## 5. Grafana 面板要点

- 仲裁组视图：`sum(ocx_ha_arb_leader)`（应恒为 1）、`max(ocx_ha_arb_epoch)`、
  `ocx_ha_arb_active_instance`、`max(ocx_ha_arb_lease_expired)`；
- 备机追赶：`ocx_ha_arb_standby_lag` 时序（正常趋近 0，尖峰 = 压测段）；
- 切换事件：`increase(ocx_ha_arb_promote_total[1h])` 柱状；
- 引擎一致性：host/standby 的 `match_trades_total` 差（应恒 0）。

## 6. 验证

```bash
curl -s localhost:9701/metrics | grep ocx_ha_arb_   # 仲裁组指标
# 触发一次受控切换后应看到：
#   ocx_ha_arb_epoch 递增、ocx_ha_arb_active_instance 变化、
#   ocx_ha_arb_promote_total 增加（HaPromotionOccurred 告警触发）
java -cp ocx-match-ingress-1.0.jar com.ocx.match.aeron.HaArbitrationTool --switch 2
```


