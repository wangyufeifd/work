# Harbour Margin 计算模块 Class 架构设计

| 属性 | 值 |
|------|-----|
| **文档版本** | V1.0 |
| **适用范围** | 开发、架构评审、保证金计算模块开发指南 |
| **前置阅读** | `最新的文档.md` 第 3.4 节（计算模块层）、第 3.7 节（SOD/Intraday 合并时序）、第 8 章（流处理语义）、`calculation-module-architecture.md`（Position 计算模块） |

---

## 目录

1. [Margin 模块在架构中的位置](#1-margin-模块在架构中的位置)
2. [输入源全景](#2-输入源全景)
3. [整体 Class 架构图](#3-整体-class-架构图)
4. [多源事件聚合模型](#4-多源事件聚合模型)
5. [ReferenceDataCache — 参考数据缓存层](#5-referencedatacache--参考数据缓存层)
6. [ManualOverrideHandler — 人工超控接入](#6-manualoverridehandler--人工超控接入)
7. [MarginStateStore — 状态存储](#7-marginstatestore--状态存储)
8. [MarginMergeEngine — 多源合并引擎](#8-marginmergeengine--多源合并引擎)
9. [AbstractMarginCalculationModule — 拓扑骨架](#9-abstractmargincalculationmodule--拓扑骨架)
10. [ResultPublisher — 结果输出](#10-resultpublisher--结果输出)
11. [故障恢复方案](#11-故障恢复方案)
12. [市场专属实现示例](#12-市场专属实现示例)
13. [Class 清单与复用总结](#13-class-清单与复用总结)

---

## 1. Margin 模块在架构中的位置

Margin 模块是 Harbour 平台最复杂的计算模块。与 Position 模块（仅双 Topic SOD + Intraday 合并）不同，Margin 模块需要聚合 **6 类输入源**，是一个**多源事件聚合器**。

```
                          ┌───────────────────────────────┐
                          │     上游输入源（6 类）          │
                          │                               │
                          │  ① margin.sod Topic           │
                          │  ② position.result Topic      │
                          │  ③ pnl.intraday Topic         │
                          │  ④ fund.transfer Topic        │
                          │  ⑤ margin.override.command    │
                          │  ⑥ Reference Data REST APIs   │
                          └───────────────┬───────────────┘
                                          │
                        ┌─────────────────┼─────────────────┐
                        │                 │                 │
                        ▼                 ▼                 ▼
              ┌─────────────────────────────────────────────────┐
              │          Margin 计算模块                         │
              │                                                 │
              │  MarginMergeEngine（多源事件 → 统一状态）         │
              │  公式：Margin = PositionMargin + PnLImpact       │
              │         - FundBalance ± ManualOverride           │
              │                                                 │
              │  PositionMargin = Σ(position × margin_rate       │
              │                    × contract_multiplier)        │
              └─────────────────────┬───────────────────────────┘
                                    │
                                    ▼
              ┌─────────────────────────────────────────────────┐
              │        harbour.result.margin Topic              │
              │        下游：OSM / Aviator / 风控 / 清算         │
              └─────────────────────────────────────────────────┘
```

## 2. 输入源全景

### 2.1 六类输入源一览

| # | 输入源 | 传输方式 | 频率 | 语义 | 对 Margin 的影响 |
|---|--------|---------|------|------|-----------------|
| ① | `harbour.unified.margin.sod` | Kafka | 每日 1 次 | 日初保证金基线 | 建立 baseline |
| ② | `harbour.result.position` | Kafka | 实时 | 已计算的持仓结果 | 触发 PositionMargin 重算 |
| ③ | `harbour.pnl.intraday` | Kafka | 实时 | 日内盈亏变动 | 累加到 running P&L |
| ④ | `harbour.fund.transfer` | Kafka | 实时 | 客户出入金 | 调整可用资金余额 |
| ⑤ | `margin.override.command` | Kafka | 按需 | 人工保证金超控 | 临时覆盖计算结果 |
| ⑥ | Reference Data REST APIs | HTTP | 定时刷新 | 账户映射/合约参数 | 提供计算所需的静态参数 |

### 2.2 各输入源数据模型

**① SOD Margin Topic** — 日初保证金基线

```protobuf
message SodMargin {
    string account_id = 1;
    string business_date = 2;
    int64 sod_version = 3;
    map<string, MarginDetail> margin_by_asset = 4; // symbol → margin detail
    string total_initial_margin = 5;
}

message MarginDetail {
    string initial_margin = 1;
    string maintenance_margin = 2;
    string premium = 3;              // 期权权利金
}
```

**② Position Result Topic** — 持仓计算结果

```protobuf
message PositionResult {
    string account_id = 1;
    string business_date = 2;
    repeated PositionDetail positions = 3;
    // 来自 Position 模块的输出，已有 net position、方向等信息
}
```

**③ Intraday P&L Topic** — 日内盈亏

```protobuf
message PnlUpdate {
    string account_id = 1;
    string symbol = 2;
    string pnl_amount = 3;           // 正=盈利，负=亏损
    int64 timestamp = 4;
}
```

**④ Fund Transfer Topic** — 出入金事件

```protobuf
message FundTransfer {
    string account_id = 1;
    string transfer_id = 2;
    FundTransferType type = 3;       // DEPOSIT / WITHDRAWAL
    string amount = 4;
    int64 timestamp = 5;
}
```

**⑤ Margin Override Command** — 人工超控（通过 REST → Kafka）

```protobuf
message MarginOverrideCommand {
    string account_id = 1;
    string override_id = 2;
    OverrideAction action = 3;       // APPLY / REVOKE
    string override_amount = 4;      // 超控金额
    string reason = 5;               // 超控原因（必填，审计）
    string operator_id = 6;          // 操作员
    int64 expiry_timestamp = 7;      // 超控过期时间
}
```

**⑥ Reference Data** — REST APIs 提供的静态参考数据

```
GET /api/v1/accounts/{account_id}/margin-group
  → margin_group_id, hierarchy_level, parent_account

GET /api/v1/instruments/{symbol}/margin-params
  → margin_rate, contract_multiplier, currency, risk_class

GET /api/v1/margin-rules/{market}
  → rule_engine_type (SPAN / KRX / 阶梯), rule_params
```

## 3. 整体 Class 架构图

```
          ┌──────────────────────────────────────────────────────┐
          │      AbstractMarginCalculationModule                  │
          │      (Kafka Streams Topology 骨架)                    │
          │                                                      │
          │  消费: ① margin.sod           ──┐                    │
          │       ② position.result       ──┤                    │
          │       ③ pnl.intraday          ──┼── 5 路事件流       │
          │       ④ fund.transfer         ──┤                    │
          │       ⑤ margin.override.cmd   ──┘                    │
          │                                                      │
          │  依赖: ReferenceDataCache (REST 缓存)                  │
          │        MarginMergeEngine (状态机)                     │
          │        ResultPublisher (输出)                         │
          └──────────┬───────────────────────────────────────────┘
                     │ extends
      ┌──────────────┼──────────────┬──────────────┐
      ▼              ▼              ▼              ▼
ChinaMargin    IndiaMargin    KoreaMargin    HkexMargin
Module         Module         Module         Module

     各 Margin Module 组合以下组件（依赖注入）：

     ┌────────────────┐  ┌─────────────────────┐  ┌──────────────────┐
     │MarginMerge     │  │PositionMargin        │  │ReferenceData     │
     │Engine          │  │Calculator            │  │Cache             │
     │                │  │                      │  │                  │
     │applySod()      │  │calculate(marginType) │  │accountMapping    │
     │applyPosition() │  │• initial_margin      │  │instrumentParams  │
     │applyPnl()      │  │• maintenance_margin  │  │marginRules       │
     │applyTransfer() │  │• premium             │  │refresh()         │
     │applyOverride() │  │                      │  │                  │
     └───────┬────────┘  └──────────┬───────────┘  └────────┬─────────┘
             │                      │                       │
             ▼                      ▼                       ▼
     ┌────────────────┐  ┌─────────────────────┐  ┌──────────────────┐
     │MarginState     │  │ResultPublisher      │  │ManualOverride    │
     │Store           │  │                     │  │Handler           │
     │                │  │publish(marginState) │  │                  │
     │per-account     │  │Kafka 事务写入        │  │POST /override    │
     │RocksDB +       │  │DLQ 路由             │  │DELETE /override   │
     │Changelog       │  │                     │  │→ Kafka topic     │
     └────────────────┘  └─────────────────────┘  └──────────────────┘
```

## 4. 多源事件聚合模型

### 4.1 核心设计思路

Margin 模块面临的关键挑战：**6 类事件到达顺序完全不确定**。

```
时序示例（某一个账户 A01 在开盘后 5 分钟内）：

  t=09:30:01  ④ fund.transfer     A01 deposit 500,000
  t=09:30:05  ② position.result   A01 IF2403 net=+10
  t=09:30:12  ③ pnl.intraday      A01 IF2403 -2000
  t=09:30:12  ② position.result   A01 SC2403 net=-5
  t=09:30:18  ⑤ margin.override   A01 apply -100,000 (风控紧急降额)
  t=09:30:25  ① margin.sod        A01 baseline (才到！)
  t=09:30:30  ③ pnl.intraday      A01 SC2403 +500
```

解决方案：**事件溯源架构 (Event Sourcing Lite)**

- 每条事件独立写入 StateStore
- 任何时候收到新事件 → 全量重算该账户的保证金
- 状态机保证同一账户串行处理（`account_id` 分区锁）

### 4.2 事件处理优先级

每种事件到达时都触发全量重算，但内部有处理规则：

```java
public enum MarginEventType {
    SOD_ARRIVED,        // margin.sod → 更新 baseline → 全量重算
    POSITION_CHANGED,   // position.result → 更新持仓 → 重算 PositionMargin
    PNL_UPDATED,        // pnl.intraday → 累加 P&L → 重算 PnLImpact
    FUND_TRANSFERRED,   // fund.transfer → 调整余额 → 重算 AvailableMargin
    OVERRIDE_APPLIED,   // margin.override → 应用超控 → 重算
    OVERRIDE_REVOKED    // margin.override → 撤销超控 → 重算
}
```

## 5. ReferenceDataCache — 参考数据缓存层

### 5.1 问题

保证金计算依赖大量静态参考数据（合约乘数、保证金率、账户层级），如果每条消息都调 REST API，延迟不可接受（REST 调用 > 50ms vs 消息处理 < 5ms）。

### 5.2 设计方案

```
                    ┌──────────────────────────────────┐
                    │     ReferenceDataCache            │
                    │                                   │
                    │  ┌─────────────────────────────┐ │
                    │  │ AccountMappingCache          │ │
                    │  │ account_id → MarginGroup     │ │
                    │  │   • margin_group_id          │ │
                    │  │   • parent_account_id        │ │
                    │  │   • hierarchy_level          │ │
                    │  └─────────────────────────────┘ │
                    │                                   │
                    │  ┌─────────────────────────────┐ │
                    │  │ InstrumentParamCache         │ │
                    │  │ symbol → MarginParams        │ │
                    │  │   • margin_rate              │ │
                    │  │   • contract_multiplier      │ │
                    │  │   • currency                 │ │
                    │  │   • risk_class               │ │
                    │  └─────────────────────────────┘ │
                    │                                   │
                    │  ┌─────────────────────────────┐ │
                    │  │ MarginRuleCache              │ │
                    │  │ market → MarginRuleSet       │ │
                    │  │   • rule_engine_type         │ │
                    │  │   • initial_margin_brackets  │ │
                    │  │   • maintenance_margin_ratio │ │
                    │  └─────────────────────────────┘ │
                    │                                   │
                    │  刷新策略：                         │
                    │  • 启动时全量加载                   │
                    │  • 定时增量刷新 (cron: */5 * * *)  │
                    │  • 手动触发 (POST /admin/cache/refresh)│
                    └──────────────────────────────────┘
```

```java
public class ReferenceDataCache {

    private final AccountMappingClient accountClient;
    private final InstrumentParamClient instrumentClient;
    private final MarginRuleClient marginRuleClient;

    // 本地缓存：Caffeine (近实时 + 自动过期)
    private final Cache<String, MarginGroup> accountMappingCache;
    private final Cache<String, MarginParams> instrumentParamCache;
    private final Cache<String, MarginRuleSet> marginRuleCache;

    // 脏标记：当外部通知参考数据变更时置位
    private final Set<String> dirtyAccounts = ConcurrentHashMap.newKeySet();
    private final Set<String> dirtyInstruments = ConcurrentHashMap.newKeySet();

    @PostConstruct
    public void init() {
        fullLoad();                          // 启动时全量加载
        scheduleRefresh(5, TimeUnit.MINUTES); // 每 5 分钟增量刷新
    }

    private void fullLoad() {
        accountMappingCache.putAll(accountClient.fetchAll());
        instrumentParamCache.putAll(instrumentClient.fetchAll());
        marginRuleCache.putAll(marginRuleClient.fetchAll());
    }

    private void scheduleRefresh(int intervalMinutes) {
        scheduledExecutor.scheduleAtFixedRate(() -> {
            // 增量刷新：只重新加载脏条目
            for (String accountId : dirtyAccounts) {
                accountMappingCache.put(accountId, accountClient.fetch(accountId));
            }
            dirtyAccounts.clear();

            for (String symbol : dirtyInstruments) {
                instrumentParamCache.put(symbol, instrumentClient.fetch(symbol));
            }
            dirtyInstruments.clear();

            // 保证金规则通常不频繁变更，全量刷新
            marginRuleCache.putAll(marginRuleClient.fetchAll());
        }, intervalMinutes, intervalMinutes, TimeUnit.MINUTES);
    }

    // 调用方无需处理缓存未命中 — 未命中时同步回源
    public MarginParams getInstrumentParams(String symbol) {
        return instrumentParamCache.get(symbol, instrumentClient::fetch);
    }

    // 外部通知：参考数据变更（例如来自 Webhook 通知）
    public void markAccountDirty(String accountId) {
        dirtyAccounts.add(accountId);
    }

    public void markInstrumentDirty(String symbol) {
        dirtyInstruments.add(symbol);
    }
}
```

## 6. ManualOverrideHandler — 人工超控接入

### 6.1 为什么不是直接写 StateStore

人工超控如果直接修改 StateStore：

- ❌ 绕过了 Kafka 事件流，无法审计追踪
- ❌ 故障恢复时 Changelog 不会回放这笔超控
- ❌ 并发不安全（流处理器正在读 + REST 线程在写同一块内存）

**正确做法**：REST → Kafka Topic → Stream Processor

```
 运维/Risk Desk
       │
       ▼
POST /admin/margin-override          REST 层（同步响应 202）
       │
       ▼
margin.override.command Topic        Kafka（异步事件）
       │
       ▼
MarginMergeEngine.applyOverride()    流处理器（串行安全）
```

### 6.2 REST Endpoint 实现

```java
@RestController
@RequestMapping("/admin")
public class ManualOverrideHandler {

    private final KafkaTemplate<String, MarginOverrideCommand> kafkaTemplate;
    private final AccountValidator accountValidator;
    private final AuditLogger auditLogger;

    @PostMapping("/margin-override")
    public ResponseEntity<OverrideResponse> applyOverride(
            @RequestBody @Valid OverrideRequest request,
            @RequestHeader("Authorization") String token) {

        // ① 认证 + 授权（操作员必须有 MARGIN_OVERRIDE 权限）
        Operator operator = authService.authenticate(token);
        if (!operator.hasPermission("MARGIN_OVERRIDE")) {
            return ResponseEntity.status(403).build();
        }

        // ② 业务校验
        accountValidator.validateExists(request.getAccountId());
        accountValidator.validateAmountWithinLimit(request.getOverrideAmount());

        // ③ 构建命令消息
        MarginOverrideCommand command = MarginOverrideCommand.newBuilder()
            .setAccountId(request.getAccountId())
            .setOverrideId(UUID.randomUUID().toString())
            .setAction(OverrideAction.APPLY)
            .setOverrideAmount(request.getOverrideAmount())
            .setReason(request.getReason())              // 原因必填，用于审计
            .setOperatorId(operator.getId())
            .setExpiryTimestamp(request.getExpiryTimestamp())
            .build();

        // ④ 写入 Kafka → 异步处理
        kafkaTemplate.send("harbour.margin.override.command",
            command.getAccountId(),  // 按 account_id 分区
            command
        ).get(5, TimeUnit.SECONDS);

        // ⑤ 审计日志
        auditLogger.log("MARGIN_OVERRIDE_APPLIED", operator.getId(),
            request.getAccountId(), request.getOverrideAmount(), request.getReason());

        // ⑥ 返回 202 Accepted（异步处理，不返回最终结果）
        return ResponseEntity.accepted().body(
            new OverrideResponse(command.getOverrideId(), "ACCEPTED")
        );
    }

    @DeleteMapping("/margin-override/{overrideId}")
    public ResponseEntity<Void> revokeOverride(
            @PathVariable String overrideId,
            @RequestHeader("Authorization") String token) {

        Operator operator = authService.authenticate(token);
        if (!operator.hasPermission("MARGIN_OVERRIDE")) {
            return ResponseEntity.status(403).build();
        }

        MarginOverrideCommand command = MarginOverrideCommand.newBuilder()
            .setOverrideId(overrideId)
            .setAction(OverrideAction.REVOKE)
            .setOperatorId(operator.getId())
            .setReason("Manual revocation")
            .build();

        kafkaTemplate.send("harbour.margin.override.command",
            command.getAccountId(), command);

        auditLogger.log("MARGIN_OVERRIDE_REVOKED", operator.getId(), overrideId);

        return ResponseEntity.accepted().build();
    }
}
```

### 6.3 超控安全性设计

| 机制 | 说明 |
|------|------|
| **必填原因** | `reason` 字段不可为空，写入审计日志 |
| **自动过期** | `expiry_timestamp` 必填，超时自动撤销（MergeEngine 定时检查） |
| **金额上限** | 单次超控不超过账户正常保证金的 N%（可配置） |
| **审批流程** | 可选集成外部审批系统，审批通过后才写入 Kafka |
| **全链路追踪** | OpenTelemetry trace_id 贯穿 REST → Kafka → Stream Processor |

## 7. MarginStateStore — 状态存储

### 7.1 每账户状态模型

```java
public class AccountMarginState {

    // ── 账户标识 ──
    private String accountId;
    private String businessDate;

    // ── 日初基线 ──
    private int sodVersion;
    private Map<String, MarginDetail> sodMarginByAsset;  // symbol → baseline margin
    private String sodTotalInitialMargin;

    // ── 持仓数据（来自 Position Result Topic）──
    private Map<String, PositionSnapshot> positions;      // symbol → latest position

    // ── 日内盈亏 ──
    private Map<String, String> runningPnlBySymbol;       // symbol → cumulative PnL

    // ── 资金 ──
    private String fundBalance;                           // 当前可用资金
    private List<String> processedTransferIds;            // 已处理的出入金 ID（去重）

    // ── 人工超控 ──
    private String manualOverrideAmount;                  // null = 无超控
    private String overrideId;
    private long overrideExpiryTimestamp;
    private String overrideReason;

    // ── 计算结果 ──
    private String calculatedTotalMargin;                 // 最新的总保证金
    private MergeState mergeState;                        // INITIALIZING / READY / STALE

    // ── 元数据 ──
    private long lastUpdateTimestamp;
    private int eventSequence;                            // 单调递增事件序号
}
```

### 7.2 存储实现

```java
public class MarginStateStore {

    private final KeyValueStore<String, AccountMarginState> store;

    public AccountMarginState get(String accountId) {
        AccountMarginState state = store.get(accountId);
        if (state == null) {
            state = new AccountMarginState();
            state.setAccountId(accountId);
            state.setMergeState(MergeState.INITIALIZING);
        }
        return state;
    }

    public void put(String accountId, AccountMarginState state) {
        state.setLastUpdateTimestamp(System.currentTimeMillis());
        state.setEventSequence(state.getEventSequence() + 1);
        store.put(accountId, state);
    }

    // 交易日切换：清除上一交易日的去重数据
    public void rollToNewBusinessDate(String accountId, String newBusinessDate) {
        AccountMarginState state = get(accountId);
        if (!newBusinessDate.equals(state.getBusinessDate())) {
            AccountMarginState newState = new AccountMarginState();
            newState.setAccountId(accountId);
            newState.setBusinessDate(newBusinessDate);
            newState.setFundBalance(state.getFundBalance()); // 资金余额跨日延续
            newState.setMergeState(MergeState.INITIALIZING);
            put(accountId, newState);
        }
    }
}
```

## 8. MarginMergeEngine — 多源合并引擎

### 8.1 核心流程

```java
public class MarginMergeEngine {

    private final MarginStateStore stateStore;
    private final PositionMarginCalculator positionMarginCalculator;
    private final ReferenceDataCache referenceDataCache;

    // ─── 处理 SOD Margin ───
    public AccountMarginState applySod(String accountId, SodMargin sod) {
        AccountMarginState state = stateStore.get(accountId);

        // 版本检查：忽略旧版本
        if (state.getSodVersion() > 0 && sod.getSodVersion() <= state.getSodVersion()) {
            log.warn("忽略旧版 SOD: account={}, currentVersion={}, receivedVersion={}",
                accountId, state.getSodVersion(), sod.getSodVersion());
            return state;
        }

        // 如果是修正版 SOD → 回滚当日的 PnL 累加？（PnL 是客观事实，不回滚）
        // 但需要重新基于新版 SOD 计算 PositionMargin
        state.setBusinessDate(sod.getBusinessDate());
        state.setSodVersion(sod.getSodVersion());
        state.setSodMarginByAsset(sod.getMarginByAssetMap());
        state.setSodTotalInitialMargin(sod.getTotalInitialMargin());

        // 触发全量重算
        fullRecalculate(state);

        // 状态转换
        if (state.getSodVersion() > 0 && !state.getPositions().isEmpty()) {
            state.setMergeState(MergeState.READY);
        }

        return stateStore.put(accountId, state);
    }

    // ─── 处理 Position 变更 ───
    public AccountMarginState applyPosition(String accountId, PositionResult position) {
        AccountMarginState state = stateStore.get(accountId);

        // 更新持仓快照
        for (PositionDetail detail : position.getPositionsList()) {
            PositionSnapshot snapshot = new PositionSnapshot();
            snapshot.setNetQty(detail.getNetQty());
            snapshot.setAvgPrice(detail.getAvgPrice());
            snapshot.setDirection(detail.getDirection().name());
            state.getPositions().put(detail.getSymbol(), snapshot);
        }

        // 全量重算
        fullRecalculate(state);

        state.setMergeState(MergeState.READY); // 有了持仓就可以算 position margin
        return stateStore.put(accountId, state);
    }

    // ─── 处理 P&L 更新 ───
    public AccountMarginState applyPnl(String accountId, PnlUpdate pnl) {
        AccountMarginState state = stateStore.get(accountId);

        // 累加 P&L
        String existing = state.getRunningPnlBySymbol().getOrDefault(pnl.getSymbol(), "0");
        BigDecimal newPnl = new BigDecimal(existing).add(new BigDecimal(pnl.getPnlAmount()));
        state.getRunningPnlBySymbol().put(pnl.getSymbol(), newPnl.toString());

        // 全量重算
        fullRecalculate(state);
        return stateStore.put(accountId, state);
    }

    // ─── 处理出入金 ───
    public AccountMarginState applyFundTransfer(String accountId, FundTransfer transfer) {
        AccountMarginState state = stateStore.get(accountId);

        // 去重：based on transfer_id
        if (state.getProcessedTransferIds().contains(transfer.getTransferId())) {
            return state; // 幂等
        }

        BigDecimal currentBalance = new BigDecimal(state.getFundBalance() != null
            ? state.getFundBalance() : "0");
        BigDecimal delta = new BigDecimal(transfer.getAmount());

        if (transfer.getType() == FundTransferType.DEPOSIT) {
            state.setFundBalance(currentBalance.add(delta).toString());
        } else {
            state.setFundBalance(currentBalance.subtract(delta).toString());
        }

        state.getProcessedTransferIds().add(transfer.getTransferId());

        fullRecalculate(state);
        return stateStore.put(accountId, state);
    }

    // ─── 处理人工超控 ───
    public AccountMarginState applyOverride(String accountId, MarginOverrideCommand cmd) {
        AccountMarginState state = stateStore.get(accountId);

        if (cmd.getAction() == OverrideAction.APPLY) {
            state.setManualOverrideAmount(cmd.getOverrideAmount());
            state.setOverrideId(cmd.getOverrideId());
            state.setOverrideExpiryTimestamp(cmd.getExpiryTimestamp());
            state.setOverrideReason(cmd.getReason());
        } else {
            // REVOKE
            state.setManualOverrideAmount(null);
            state.setOverrideId(null);
            state.setOverrideExpiryTimestamp(0);
            state.setOverrideReason(null);
        }

        fullRecalculate(state);
        return stateStore.put(accountId, state);
    }

    // ─── 全量重算 ───
    private void fullRecalculate(AccountMarginState state) {
        // ① 计算持仓保证金
        String positionMargin = positionMarginCalculator.calculate(state.getPositions());

        // ② 计算 P&L 影响
        String totalPnl = state.getRunningPnlBySymbol().values().stream()
            .map(BigDecimal::new)
            .reduce(BigDecimal.ZERO, BigDecimal::add)
            .toString();

        // ③ 计算可用资金调整
        String fundBalance = state.getFundBalance() != null ? state.getFundBalance() : "0";

        // ④ 公式：Margin = PositionMargin - PnLImpact - FundBalance ± Override
        BigDecimal margin = new BigDecimal(positionMargin)
            .subtract(new BigDecimal(totalPnl))
            .subtract(new BigDecimal(fundBalance));

        // ⑤ 应用人工超控
        if (state.getManualOverrideAmount() != null) {
            // 检查超控是否过期
            if (System.currentTimeMillis() > state.getOverrideExpiryTimestamp()) {
                log.warn("超控已过期，自动撤销: account={}, overrideId={}",
                    state.getAccountId(), state.getOverrideId());
                state.setManualOverrideAmount(null);
            } else {
                margin = margin.add(new BigDecimal(state.getManualOverrideAmount()));
            }
        }

        state.setCalculatedTotalMargin(margin.toString());
    }
}
```

### 8.2 并发安全

所有事件对同一 `account_id` 的操作必须串行。Kafka Streams 的 partition-level 处理天然保证这一点——同一 `account_id` 的所有事件进入同一分区，由同一 Stream Thread 处理。

对于定时任务（超控过期检查），使用 `punctuate` 机制：

```java
// 每 10 秒检查一次过期超控
schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
    try (KeyValueIterator<String, AccountMarginState> iter = store.all()) {
        while (iter.hasNext()) {
            AccountMarginState state = iter.next().value;
            if (state.getManualOverrideAmount() != null
                && timestamp > state.getOverrideExpiryTimestamp()) {
                state.setManualOverrideAmount(null);
                fullRecalculate(state);
                store.put(state.getAccountId(), state);
                // 发布超控过期事件到结果 Topic
                publisher.publishOverrideExpired(state);
            }
        }
    }
});
```

## 9. AbstractMarginCalculationModule — 拓扑骨架

```java
public abstract class AbstractMarginCalculationModule {

    protected final ReferenceDataCache referenceDataCache;
    protected final MarginMergeEngine mergeEngine;
    protected final PositionMarginCalculator positionMarginCalculator;
    protected final ResultPublisher resultPublisher;

    public Topology buildTopology(StreamsBuilder builder) {
        // ─── 5 路事件流 ───

        // ① margin.sod
        KStream<String, SodMargin> sodStream = builder.stream("harbour.unified.margin.sod",
            Consumed.with(Serdes.String(), marginSodSerde));

        // ② position.result
        KStream<String, PositionResult> positionStream = builder.stream("harbour.result.position",
            Consumed.with(Serdes.String(), positionResultSerde));

        // ③ pnl.intraday
        KStream<String, PnlUpdate> pnlStream = builder.stream("harbour.pnl.intraday",
            Consumed.with(Serdes.String(), pnlUpdateSerde));

        // ④ fund.transfer
        KStream<String, FundTransfer> fundStream = builder.stream("harbour.fund.transfer",
            Consumed.with(Serdes.String(), fundTransferSerde));

        // ⑤ margin.override.command
        KStream<String, MarginOverrideCommand> overrideStream =
            builder.stream("harbour.margin.override.command",
                Consumed.with(Serdes.String(), marginOverrideSerde));

        // ─── 合并所有流 → 统一 Processor ───
        KStream<String, MarginEvent> mergedStream = builder.stream(...)
            // 方案：每条流先包装成统一的 MarginEvent，再 merge
            ...

        // 实际做法：使用 5 个独立的 process() / transform() 共享同一个 StateStore
        MarginStateStore store = new MarginStateStore("margin-state-store");

        sodStream.process(() -> new MarginEventProcessor(MarginEventType.SOD_ARRIVED, mergeEngine),
            "margin-state-store");
        positionStream.process(() -> new MarginEventProcessor(MarginEventType.POSITION_CHANGED, mergeEngine),
            "margin-state-store");
        pnlStream.process(() -> new MarginEventProcessor(MarginEventType.PNL_UPDATED, mergeEngine),
            "margin-state-store");
        fundStream.process(() -> new MarginEventProcessor(MarginEventType.FUND_TRANSFERRED, mergeEngine),
            "margin-state-store");
        overrideStream.process(() -> new MarginEventProcessor(MarginEventType.OVERRIDE_APPLIED, mergeEngine),
            "margin-state-store");

        // ─── 结果流 ───
        // 在 Processor 内部，每次状态变更后调用 resultPublisher.publish()
        // publisher 写入 harbour.result.margin Topic

        return builder.build();
    }

    // 子类注入
    protected abstract PositionMarginCalculator createPositionMarginCalculator();
    protected abstract ExtensionResolver createExtensionResolver();
}
```

**MarginEventProcessor — 统一事件分发器**：

```java
public class MarginEventProcessor extends ContextualProcessor<String, Object, String, MarginResult> {

    private final MarginEventType eventType;
    private final MarginMergeEngine mergeEngine;
    private MarginStateStore store;

    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        this.store = new MarginStateStore(
            (KeyValueStore<String, AccountMarginState>) context.getStateStore("margin-state-store"));
    }

    @Override
    public void process(Record<String, Object> record) {
        String accountId = record.key();
        Object value = record.value();

        AccountMarginState newState = switch (eventType) {
            case SOD_ARRIVED       -> mergeEngine.applySod(accountId, (SodMargin) value);
            case POSITION_CHANGED  -> mergeEngine.applyPosition(accountId, (PositionResult) value);
            case PNL_UPDATED       -> mergeEngine.applyPnl(accountId, (PnlUpdate) value);
            case FUND_TRANSFERRED  -> mergeEngine.applyFundTransfer(accountId, (FundTransfer) value);
            case OVERRIDE_APPLIED,
                 OVERRIDE_REVOKED  -> mergeEngine.applyOverride(accountId, (MarginOverrideCommand) value);
        };

        // 转发结果到下游 Topic
        context.forward(record.withKey(accountId)
            .withValue(MarginResult.from(newState)));
    }
}
```

## 10. ResultPublisher — 结果输出

```java
public class MarginResultPublisher {

    private final KafkaProducer<String, MarginResult> producer;
    private final String resultTopic = "harbour.result.margin";

    public void publish(AccountMarginState state) {
        MarginResult result = MarginResult.newBuilder()
            .setAccountId(state.getAccountId())
            .setBusinessDate(state.getBusinessDate())
            .setTotalMargin(state.getCalculatedTotalMargin())
            .setPositionMargin(positionMarginCalculator.getLastResult())
            .setPnlImpact(getTotalPnl(state))
            .setFundBalance(state.getFundBalance() != null ? state.getFundBalance() : "0")
            .setManualOverrideAmount(state.getManualOverrideAmount())
            .setMergeState(state.getMergeState().name())
            .setLastUpdateTimestamp(state.getLastUpdateTimestamp())
            .addAllMarginByAsset(toMarginDetailList(state))
            .build();

        ProducerRecord<String, MarginResult> record = new ProducerRecord<>(
            resultTopic,
            state.getAccountId(),          // 按 account_id 分区
            result
        );
        record.headers().add("idempotency_key",
            (state.getAccountId() + ":" + state.getBusinessDate() + ":" + state.getEventSequence()).getBytes());

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                dlqRouter.route(result, exception);
            }
        });
    }
}
```

## 11. 故障恢复方案

### 11.1 恢复时序

```
场景：Margin 模块实例崩溃

恢复步骤：
  ① Kafka Streams 自动 rebalance，分区重新分配给健康实例
  ② 新实例从 Changelog Topic 恢复 MarginStateStore（秒级）
  ③ 从上次 committed offset 继续消费 5 路事件流
  ④ MarginMergeEngine 逐条处理积压事件
  ⑤ 每次全量重算 → 发布最新 MarginResult
  ⑥ 状态从 STALE → READY
```

### 11.2 降级策略

| 输入源 | 超时处理 | 降级行为 |
|--------|---------|---------|
| `margin.sod` | 等修正版 | 该账户保持 INITIALIZING，不发布结果 |
| `position.result` | 保持上次持仓 | 用最后已知持仓继续计算保证金，标记 STALE |
| `pnl.intraday` | 保持累计 PnL | P&L 不更新但保证金照常计算 |
| `fund.transfer` | 无影响 | 出入金不依赖其他源 |
| `override.command` | 无影响 | 超控不依赖其他源 |
| Reference Data REST | 重试 3 次 + 使用本地缓存 | 缓存未过期则继续使用，过期则告警 |

## 12. 市场专属实现示例

```java
// 中国市场保证金模块 — 约 50 行
public class ChinaMarginModule extends AbstractMarginCalculationModule {

    @Override
    protected PositionMarginCalculator createPositionMarginCalculator() {
        return new ChinaPositionMarginCalculator(referenceDataCache);
    }

    @Override
    protected ExtensionResolver createExtensionResolver() {
        return new ChinaMarginExtensionResolver();
    }
}

// 中国市场持仓保证金计算器
public class ChinaPositionMarginCalculator implements PositionMarginCalculator {

    private final ReferenceDataCache cache;

    @Override
    public String calculate(Map<String, PositionSnapshot> positions) {
        BigDecimal totalMargin = BigDecimal.ZERO;

        for (var entry : positions.entrySet()) {
            String symbol = entry.getKey();
            PositionSnapshot pos = entry.getValue();
            MarginParams params = cache.getInstrumentParams(symbol);

            BigDecimal qty = new BigDecimal(pos.getNetQty());
            BigDecimal multiplier = new BigDecimal(params.getContractMultiplier());
            BigDecimal marginRate = new BigDecimal(params.getMarginRate());

            // 中国市场：阶梯保证金
            // 持仓量越大，保证金率可能更高（根据 risk_class 查表）
            BigDecimal effectiveRate = getBracketedRate(params.getRiskClass(), qty.abs());

            BigDecimal positionMargin = qty.abs()
                .multiply(multiplier)
                .multiply(effectiveRate);

            totalMargin = totalMargin.add(positionMargin);
        }

        return totalMargin.toString();
    }

    private BigDecimal getBracketedRate(String riskClass, BigDecimal qty) {
        // 查中国保证金阶梯表
        return marginRuleCache.getBracketedRate(riskClass, qty);
    }
}
```

## 13. Class 清单与复用总结

### 13.1 完整 Class 清单

| 层级 | Class / Interface | 复用度 | 新市场需新建? | 说明 |
|------|------------------|:------:|:------------:|------|
| **骨架** | `AbstractMarginCalculationModule` | 100% | ❌ | Kafka Streams 拓扑骨架 |
| **引擎** | `MarginMergeEngine` | 100% | ❌ | 多源事件合并引擎 |
| **存储** | `MarginStateStore` | 100% | ❌ | RocksDB + Changelog |
| **存储** | `AccountMarginState` | 100% | ❌ | 每账户状态模型 |
| **接口** | `PositionMarginCalculator` | 100% | ❌ | 持仓保证金计算器接口 |
| **缓存** | `ReferenceDataCache` | 100% | ❌ | REST 参考数据缓存 |
| **发布** | `MarginResultPublisher` | 100% | ❌ | Kafka 结果输出 |
| **API** | `ManualOverrideHandler` | 100% | ❌ | 人工超控 REST 端点 |
| **处理器** | `MarginEventProcessor` | 100% | ❌ | 统一事件分发器 |
| **基础设施** | `SequenceManager` | 100% | ❌ | 幂等序列号 |
| **基础设施** | `DlqRouter` | 100% | ❌ | DLQ 路由 |
| **基础设施** | `MarginMetrics` | 100% | ❌ | Prometheus 指标 |
| **市场专属** | `{Market}MarginModule` | 0% | ✅ 新建 | ~20 行组装类 |
| **市场专属** | `{Market}PositionMarginCalculator` | 0% | ✅ 新建 | 市场保证金规则 |
| **市场专属** | `{Market}MarginExtensionResolver` | 0% | ✅ 按需新建 | 扩展解析 |

### 13.2 与 Position 模块的关键区别

| 维度 | Position 模块 | Margin 模块 |
|------|:------------:|:-----------:|
| **输入源数** | 2 (SOD + Intraday) | 5-6 (SOD + Position + PnL + Fund + Override + RefData) |
| **合并模式** | SOD 基线 + 增量累加 | 全量重算（每次事件触发） |
| **外部依赖** | 无 | Reference Data REST APIs |
| **人机交互** | 无 | Manual Override REST Endpoint |
| **状态复杂度** | 低（持仓 + 去重集合） | 高（持仓 + P&L + 资金 + 超控） |
| **新市场工作量** | 1-3 个 class | 2-3 个 class（计算器是主要工作） |

### 13.3 新市场接入工作量

| 产出物 | 数量 | 工作量 |
|--------|:----:|--------|
| `{Market}MarginModule`（组装类，~20 行） | 1 | 极小 |
| `{Market}PositionMarginCalculator`（保证金规则） | 1 | **主要工作** |
| `{Market}MarginExtensionResolver`（如有特有扩展） | 0-1 | 小 |
| **总计** | **2-3 个文件** | **1-2 天**（取决于保证金规则复杂度） |

> 其余 12 个通用 class / interface **全部复用，零修改**。

---

> **关联文档**：
> - `最新的文档.md` — 完整架构设计 V1.5
> - `calculation-module-architecture.md` — Position 计算模块 Class 架构
> - `adaptor-architecture.md` — Adaptor 内部 Class 架构
> - `设计图.md` — 平台架构设计图
>
> **更新日期**：2026-01-15