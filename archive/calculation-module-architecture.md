# Harbour 计算模块 Class 架构设计 — 以 Position 为例

| 属性 | 值 |
|------|-----|
| **文档版本** | V1.0 |
| **适用范围** | 开发、架构评审、新市场计算模块开发指南 |
| **前置阅读** | `最新的文档.md` 第 3.4 节（计算模块层）、第 3.7 节（SOD/Intraday 合并时序）、第 8 章（流处理语义） |

---

## 目录

1. [核心问题与设计目标](#1-核心问题与设计目标)
2. [整体架构概览](#2-整体架构概览)
3. [状态机设计](#3-状态机设计)
4. [SOD Consumer 层](#4-sod-consumer-层)
5. [Intraday Consumer 层](#5-intraday-consumer-层)
6. [State Store 层 — 账户状态持久化](#6-state-store-层--账户状态持久化)
7. [Merge Engine — SOD/Intraday 合并引擎](#7-merge-engine--sodintraday-合并引擎)
8. [Result Publisher — 结果输出层](#8-result-publisher--结果输出层)
9. [AbstractCalculationModule — 模板方法骨架](#9-abstractcalculationmodule--模板方法骨架)
10. [市场专属实现 — 以 ChinaPositionModule 为例](#10-市场专属实现--以-chinapositionmodule-为例)
11. [故障恢复全流程](#11-故障恢复全流程)
12. [Changelog 状态恢复机制](#12-changelog-状态恢复机制)
13. [Class 清单汇总](#13-class-清单汇总)

---

## 1. 核心问题与设计目标

### 1.1 问题定义

Position 计算模块需要从两个独立 Topic 读取数据，实时计算每个账户的当日总持仓：

```
harbour.unified.position.sod       ──→  日初持仓基线（全量快照）
harbour.unified.position.intraday  ──→  日内增量变动（单笔成交粒度）

                    两条流汇聚到同一个计算模块
                              │
                              ▼
                   实时日内总持仓结果 Proto
                              │
                              ▼
                  harbour.result.position Topic
```

### 1.2 设计约束（来自架构文档）

| 约束 | 来源 |
|------|------|
| SOD 和 Intraday 为两个独立 Topic | [3.3.3 SOD/Intraday 分 Topic 设计](最新的文档.md#333-sod-与-intraday-分-topic-设计深度解析) |
| 消息按 `account_id` 分区，同一账户有序 | [3.4.2 全局汇总分片策略](最新的文档.md#342-全局汇总分片策略) |
| 四种时序场景均需处理（SOD 先到 / Intraday 先到 / 并发 / SOD 修正） | [3.7.1 关键时序场景](最新的文档.md#371-关键时序场景) |
| `execution_id` 去重 | [3.7.2 去重策略](最新的文档.md#372-去重策略) |
| 流内部 Exactly-Once 语义 | [8.1 Exactly-Once 语义边界](最新的文档.md#81-exactly-once-语义边界明确范围杜绝预期偏差) |
| Changelog 秒级恢复 | [8.3 状态恢复](最新的文档.md#83-状态恢复本地状态--changelog-topic) |
| 通用模块只读核心字段，专属模块按需解析扩展 | [5.7.3 消费隔离原则](最新的文档.md#573-运行时消费隔离原则核心设计精髓) |

### 1.3 设计目标

- **双 Topic 双 Consumer**：SOD 和 Intraday 独立消费、独立 offset 管理
- **状态外存**：账户状态持久化到 RocksDB，通过 Changelog Topic 实现秒级故障恢复
- **合并引擎**：统一的状态机驱动，处理所有时序场景
- **市场隔离**：通用框架 100% 复用，市场差异化通过策略/插件注入
- **Exactly-Once**：Kafka Streams 事务保证消费-计算-生产原子性

---

## 2. 整体架构概览

### 2.1 拓扑图

```
   ┌─────────────────────┐       ┌──────────────────────┐
   │  SOD Topic           │       │  Intraday Topic       │
   │  (8 partitions)      │       │  (32 partitions)      │
   └──────────┬──────────┘       └──────────┬───────────┘
              │                             │
              │ Kafka Streams               │ Kafka Streams
              │ consume                     │ consume
              ▼                             ▼
   ┌──────────────────────────────────────────────────────────┐
   │                   SodIntradayProcessor                    │
   │                   (同一个 Kafka Streams 拓扑)              │
   │                                                          │
   │  ┌──────────────┐              ┌───────────────────┐    │
   │  │ SOD Branch    │              │ Intraday Branch    │    │
   │  │               │              │                    │    │
   │  │ ① 反序列化    │              │ ① 反序列化         │    │
   │  │ ② Schema 校验 │              │ ② Schema 校验      │    │
   │  │ ③ 版本比较    │              │ ③ execution_id 去重│    │
   │  │ ④ → MergeEng  │              │ ④ → MergeEng       │    │
   │  └──────┬───────┘              └────────┬──────────┘    │
   │         │                               │                │
   │         └───────────┬───────────────────┘                │
   │                     │                                    │
   │                     ▼                                    │
   │         ┌──────────────────────┐                         │
   │         │   MergeEngine        │                         │
   │         │   (状态机核心)        │                         │
   │         │                      │                         │
   │         │  根据 MergeOperation │                         │
   │         │  + 当前账户状态      │                         │
   │         │  → 读取 StateStore   │                         │
   │         │  → 执行合并逻辑      │                         │
   │         │  → 更新 StateStore   │                         │
   │         │  → 输出 ResultProto  │                         │
   │         └──────────┬───────────┘                         │
   │                    │                                      │
   │                    │ Read / Write                         │
   │                    ▼                                      │
   │         ┌──────────────────────┐                         │
   │         │   StateStore         │                         │
   │         │   (RocksDB)          │                         │
   │         │                      │                         │
   │         │   Key: accountId     │                         │
   │         │   Value: AccountPos  │                         │
   │         │   Changelog → Kafka  │                         │
   │         └──────────────────────┘                         │
   │                    │                                      │
   │                    │ 计算结果                             │
   │                    ▼                                      │
   │         ┌──────────────────────┐                         │
   │         │   ResultPublisher    │                         │
   │         │   → result Topic     │                         │
   │         └──────────────────────┘                         │
   └──────────────────────────────────────────────────────────┘
```

### 2.2 Kafka Streams vs 裸 Consumer 选型

| 维度 | Kafka Streams | 裸 KafkaConsumer | 选择 |
|------|:------------:|:----------------:|:----:|
| 双 Topic 消费 + 合并 | 原生支持多 source topic → 统一 processor | 需要手动管理两个 Consumer 协作 | ✅ Streams |
| Exactly-Once 语义 | 内置事务（`exactly_once_v2`） | 手工实现 offset + 结果事务 | ✅ Streams |
| Changelog 状态恢复 | 自动 changelog topic | 需要手动写 changelog | ✅ Streams |
| RocksDB 状态存储 | 内置，自动管理 | 需要自行集成 | ✅ Streams |
| Rebalance 处理 | 自动 | 手动处理 | ✅ Streams |
| 分区感知状态 | 同一 partition 串行处理，天然线程安全 | 需要手动保证 | ✅ Streams |
| **双 Topic 分区数不同** | ⚠️ SOD 8 / Intraday 32 分区无法直接 `cogroup` | 灵活，可手动协调 | ⚠️ 需要特殊处理 |

> **关键问题**：SOD Topic (8 分区) 和 Intraday Topic (32 分区) 分区数不同。Kafka Streams 的多 source topology 要求 co-partition，分区数不同的流无法直接 join/cogroup。

### 2.3 分区数不匹配的解决方案

```
方案 A：让 SOD 和 Intraday 分区数对齐（SOD 也改为 32）
  ✅ Kafka Streams 可以直接 cogroup，架构最简洁
  ❌ SOD 数据量极小，32 分区造成大量空分区

方案 B：SOD 用独立的 GlobalKTable（全局复制到每个实例）
  ✅ 每个 Intraday 分区都能本地读取 SOD 基线
  ❌ SOD 数据量大时内存开销高
  ✅ SOD 数据量小（每日一次全量快照），内存开销可接受

方案 C：手动协调两个 Consumer，不依赖 Kafka Streams 的 join
  ✅ 最大灵活性
  ❌ 需要自己实现 Exactly-Once 和 Changelog

推荐：方案 B
  SOD (8分区) → GlobalKTable（每个 Stream 实例全量缓存）
  Intraday (32分区) → KStream（正常消费）
  在 flatTransform 中合并
```

### 2.4 最终架构：Kafka Streams + GlobalKTable

```
Intraday Topic (32 分区, KStream)
       │
       │ flatTransform (有状态处理)
       │
       ├── 读取 GlobalKTable<SOD 基线>
       ├── 读取 StateStore<账户运行时状态>
       ├── 执行 MergeEngine 合并逻辑
       ├── 更新 StateStore
       └── 输出 ResultProto 到结果 Topic
```

---

## 3. 状态机设计

### 3.1 状态定义

```java
public enum AccountMergeState {
    /**
     * SOD 未就绪，仅累积了 Intraday 增量。
     * 查询返回临时结果 + 状态标记。
     */
    INITIALIZING,

    /**
     * SOD 已就绪，增量合并正常进行。
     * 查询返回最终结果。
     */
    READY,

    /**
     * 超过阈值时间未收到任何数据。
     * 返回最后已知结果 + 告警标记。
     */
    STALE
}
```

### 3.2 状态转移图

```
                    ┌───────────────┐
                    │  交易日开始    │
                    │  状态 = null   │
                    └───────┬───────┘
                            │
              ┌─────────────┴─────────────┐
              │                           │
              ▼                           ▼
    ┌──────────────────┐        ┌──────────────────┐
    │ SOD 先到          │        │ Intraday 先到     │
    │                  │        │                  │
    │ 建立 SOD 基线     │        │ 缓存 Intraday     │
    │ 回放缓存的        │        │ 状态 = INITIALIZING│
    │ Intraday (如有)   │        │                  │
    │ 状态 = READY      │        │                  │
    └────────┬─────────┘        └────────┬─────────┘
             │                           │
             │                           │ SOD 到达
             │                           │ 回放缓存的 Intraday
             │                           │ 状态 → READY
             │                           │
             ▼                           ▼
    ┌──────────────────────────────────────────┐
    │              状态 = READY                 │
    │                                          │
    │  Intraday 到达 → 增量累加                  │
    │  SOD 修正版到达 → 回滚增量 → 新基线重建     │
    │                                          │
    │        长时间无数据                        │
    │              │                           │
    │              ▼                           │
    │     状态 = STALE                          │
    │     返回最后已知结果 + 告警                 │
    └──────────────────────────────────────────┘
```

### 3.3 MergeOperation 定义

```java
/**
 * 合并引擎的输入操作类型。
 * 由 Consumer 层根据消息类型（SOD / Intraday）构造。
 */
public enum MergeOperationType {
    SOD_ESTABLISH,     // SOD 首次到达，建立基线
    SOD_CORRECTION,    // SOD 修正版到达（sod_version 更高）
    INTRA_ACCUMULATE   // Intraday 增量累加
}

public class MergeOperation {
    private MergeOperationType type;
    private String accountId;
    private String businessDate;
    private UnifiedPosition proto;  // 原始消息（SOD 或 Intraday）
    private long eventTimestamp;    // 事件到达时间
}
```

---

## 4. SOD Consumer 层

### 4.1 职责

- 消费 `harbour.unified.position.sod` Topic
- SOD 数据量小（每天一次），构建为 **GlobalKTable**
- GlobalKTable 使 SOD 基线在全部分区实例中可见
- 每次更新时通知 MergeEngine 处理 SOD_CORRECTION 场景

### 4.2 实现

```java
public class SodConsumerConfig {

    /**
     * 将 SOD Topic 构建为 GlobalKTable。
     *
     * Key: account_id (String)
     * Value: UnifiedPosition (Protobuf bytes)
     */
    public static GlobalKTable<String, UnifiedPosition> buildGlobalTable(
            StreamsBuilder builder,
            String sodTopic,
            Serde<String> keySerde,
            Serde<UnifiedPosition> valueSerde) {

        return builder.globalTable(
            sodTopic,
            Materialized.<String, UnifiedPosition, KeyValueStore<Bytes, byte[]>>
                as("sod-baseline-store")
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde)
        );
    }
}
```

### 4.3 SOD 消息处理要点

```java
// GlobalKTable 自动处理：
// ① 同一 account_id 的新 SOD 覆盖旧 SOD（等价于 sod_version 语义）
// ② 所有 Stream 实例都有完整副本（全量缓存）
//
// 在 MergeEngine 中使用时：
// GlobalKTable 的变更会触发下游重新处理
// → 这正是 SOD_CORRECTION 场景的触发机制
```

---

## 5. Intraday Consumer 层

### 5.1 职责

- 消费 `harbour.unified.position.intraday` Topic
- 高吞吐（32 分区，交易时段持续消费）
- 每条消息转发到 MergeEngine
- 处理 `execution_id` 去重

### 5.2 实现

```java
public class IntradayConsumerConfig {

    public static KStream<String, UnifiedPosition> buildStream(
            StreamsBuilder builder,
            String intradayTopic,
            Serde<String> keySerde,
            Serde<UnifiedPosition> valueSerde) {

        return builder.stream(
            intradayTopic,
            Consumed.with(keySerde, valueSerde)
        );
    }
}
```

### 5.3 去重过滤器

```java
/**
 * Intraday 去重：在进入 MergeEngine 之前过滤重复消息。
 * 去重集合维护在 StateStore 中（复用 AccountPositionState 的 dedupSet）。
 */
public class IntradayDedupFilter {

    private final PositionStateStore stateStore;

    /**
     * 检查 execution_id 是否已处理。
     * 已处理 → 跳过（返回 null，Kafka Streams 会过滤掉）
     * 未处理 → 记录到去重集合，继续处理
     */
    public UnifiedPosition filter(UnifiedPosition intraday) {
        String accountId = intraday.getAccountId();
        String executionId = intraday.getExecutionId();

        AccountPositionState state = stateStore.get(accountId);
        if (state != null && state.getDedupSet().contains(executionId)) {
            // 重复消息，丢弃
            return null;
        }
        return intraday;
    }
}
```

---

## 6. State Store 层 — 账户状态持久化

### 6.1 数据结构

```java
/**
 * 每个账户的运行时状态。
 * 存储在 Kafka Streams 的 RocksDB StateStore 中。
 * Key: accountId (String)
 * Value: AccountPositionState (序列化为 JSON/Protobuf)
 */
public class AccountPositionState {

    // ── 标识字段 ──
    private String accountId;
    private String businessDate;         // 当前交易日 YYYYMMDD

    // ── SOD 基线 ──
    /**
     * SOD 基线持仓快照。
     * Key: symbol
     * Value: PositionSnapshot (qty, avg_price, sod_version)
     */
    private Map<String, PositionSnapshot> sodBaseline;

    // ── Intraday 增量 ──
    /**
     * 日内增量缓存（SOD 未就绪前暂存，就绪后回放）。
     * 有序列表，按 arrival_time 排序。
     */
    private List<IntradayDelta> pendingIntradayDeltas;

    // ── 去重集合 ──
    /**
     * 当日已处理的 execution_id 集合。
     * 用于 SOD/Intraday 重叠窗口去重。
     * 每个交易日清空重建。
     */
    private Set<String> dedupSet;

    // ── 版本控制 ──
    /** 当前 SOD 版本号，用于检测修正重发 */
    private int currentSodVersion;

    // ── 状态标记 ──
    private AccountMergeState mergeState;  // INITIALIZING / READY / STALE
    private long lastUpdateTimestamp;      // 最后数据到达时间

    // ── 计算结果缓存 ──
    /**
     * 当前合并后的总持仓结果。
     * Key: symbol
     * Value: MergedPosition (总 qty, avg_price, sod_qty, today_qty)
     */
    private Map<String, MergedPosition> currentPositions;
}
```

### 6.2 子数据结构

```java
/** SOD 基线中的单条持仓快照 */
public class PositionSnapshot {
    private String symbol;
    private String sodQty;       // 昨仓（日初快照中的持仓量）
    private String sodAvgPrice;  // 日初均价
    private int sodVersion;      // 该条记录的 SOD 版本号
}

/** 日内单笔增量 */
public class IntradayDelta {
    private String executionId;
    private String symbol;
    private String deltaQty;       // 增量（正=开仓/加仓，负=平仓/减仓）
    private String tradePrice;     // 成交价
    private long arrivalTimestamp; // 到达时间（用于排序回放）
    private UnifiedPosition rawProto; // 保留原始消息（含扩展字段）
}

/** 合并后的总持仓 */
public class MergedPosition {
    private String symbol;
    private String totalQty;      // 总持仓 = sodQty + Σ(deltaQty)
    private String sodQty;        // 昨仓分量
    private String todayQty;      // 今仓分量 = Σ(deltaQty)
    private String avgPrice;      // 加权均价
    private long lastUpdateTime;  // 最后更新时间
}
```

### 6.3 StateStore 操作接口

```java
public class PositionStateStore {

    private final KeyValueStore<String, AccountPositionState> store;

    // ── 读操作 ──
    public AccountPositionState get(String accountId) {
        return store.get(accountId);
    }

    // ── 写操作 ──
    public void put(String accountId, AccountPositionState state) {
        store.put(accountId, state);
    }

    // ── 交易日切换 ──
    /**
     * 检测 businessDate 变更，清空旧状态。
     * 在每个消息处理前调用。
     */
    public AccountPositionState ensureBusinessDate(
            String accountId, String newBusinessDate) {

        AccountPositionState state = get(accountId);
        if (state == null || !newBusinessDate.equals(state.getBusinessDate())) {
            // 新交易日，创建空状态
            state = new AccountPositionState();
            state.setAccountId(accountId);
            state.setBusinessDate(newBusinessDate);
            state.setMergeState(AccountMergeState.INITIALIZING);
            state.setDedupSet(new HashSet<>());
            state.setSodBaseline(new HashMap<>());
            state.setPendingIntradayDeltas(new ArrayList<>());
            state.setCurrentPositions(new HashMap<>());
            state.setCurrentSodVersion(-1);
            put(accountId, state);
        }
        return state;
    }

    // ── 去重检查 + 记录 ──
    public boolean isDuplicate(String accountId, String executionId) {
        AccountPositionState state = get(accountId);
        return state != null && state.getDedupSet().contains(executionId);
    }

    public void markProcessed(String accountId, String executionId) {
        AccountPositionState state = get(accountId);
        if (state != null) {
            state.getDedupSet().add(executionId);
            put(accountId, state);
        }
    }
}
```

---

## 7. Merge Engine — SOD/Intraday 合并引擎

### 7.1 引擎入口

```java
/**
 * SOD/Intraday 合并引擎。
 * 无状态（所有状态存储在 StateStore 中），纯逻辑。
 */
public class SodIntradayMergeEngine {

    private final PositionStateStore stateStore;
    private final GlobalKTable<String, UnifiedPosition> sodTable;

    /**
     * 处理一条合并操作（SOD 或 Intraday）。
     * 返回 Optional：有结果时返回，无结果时为空（如去重跳过）。
     */
    public Optional<UnifiedResultPosition> process(MergeOperation operation) {
        return switch (operation.getType()) {
            case SOD_ESTABLISH   -> handleSodEstablish(operation);
            case SOD_CORRECTION  -> handleSodCorrection(operation);
            case INTRA_ACCUMULATE -> handleIntraAccumulate(operation);
        };
    }
}
```

### 7.2 SOD 首次到达处理

```java
private Optional<UnifiedResultPosition> handleSodEstablish(MergeOperation op) {
    String accountId = op.getAccountId();
    String businessDate = op.getBusinessDate();
    UnifiedPosition sod = op.getProto();

    // ① 切换到当前交易日
    AccountPositionState state = stateStore.ensureBusinessDate(accountId, businessDate);

    // ② 建立 SOD 基线
    int sodVersion = extractSodVersion(sod);
    state.setCurrentSodVersion(sodVersion);
    state.getSodBaseline().put(
        sod.getSymbol(),
        new PositionSnapshot(
            sod.getSymbol(),
            sod.getQuantity(),      // SOD 中是全量持仓
            sod.getAvgPrice(),
            sodVersion
        )
    );
    // ③ 记录 SOD 的 execution_id 到去重集合（防止与 Intraday 重叠）
    state.getDedupSet().add(sod.getExecutionId());

    // ④ 回放缓存的 Intraday 增量
    replayPendingIntraday(state);

    // ⑤ 状态 → READY（SOD 已就绪）
    state.setMergeState(AccountMergeState.READY);
    state.setLastUpdateTimestamp(System.currentTimeMillis());
    stateStore.put(accountId, state);

    // ⑥ 输出当前结果
    return Optional.of(buildResult(state));
}
```

### 7.3 SOD 修正重发处理

```java
private Optional<UnifiedResultPosition> handleSodCorrection(MergeOperation op) {
    String accountId = op.getAccountId();
    UnifiedPosition sod = op.getProto();
    int newVersion = extractSodVersion(sod);

    AccountPositionState state = stateStore.get(accountId);
    if (state == null) {
        // 修正版比首次版先到（极端情况），按首次处理
        return handleSodEstablish(op);
    }

    // ① 版本比较：只处理更高版本
    if (newVersion <= state.getCurrentSodVersion()) {
        return Optional.empty();  // 旧版本，跳过
    }

    // ② 回滚当日增量：清空 currentPositions 中的 todayQty 分量
    rollbackIntradayDeltas(state);

    // ③ 用新版 SOD 重建基线
    state.getSodBaseline().put(
        sod.getSymbol(),
        new PositionSnapshot(sod.getSymbol(), sod.getQuantity(),
                             sod.getAvgPrice(), newVersion)
    );
    state.setCurrentSodVersion(newVersion);
    state.getDedupSet().add(sod.getExecutionId());

    // ④ 重新回放缓存的 Intraday（用新的基线）
    replayPendingIntraday(state);

    // ⑤ 状态 → READY
    state.setMergeState(AccountMergeState.READY);
    state.setLastUpdateTimestamp(System.currentTimeMillis());
    stateStore.put(accountId, state);

    return Optional.of(buildResult(state));
}
```

### 7.4 Intraday 增量累加处理

```java
private Optional<UnifiedResultPosition> handleIntraAccumulate(MergeOperation op) {
    String accountId = op.getAccountId();
    String businessDate = op.getBusinessDate();
    UnifiedPosition intraday = op.getProto();

    // ① 去重检查
    if (stateStore.isDuplicate(accountId, intraday.getExecutionId())) {
        return Optional.empty();  // 重复消息
    }

    // ② 切换到当前交易日
    AccountPositionState state = stateStore.ensureBusinessDate(accountId, businessDate);

    // ③ 根据状态决定处理方式
    if (state.getMergeState() == AccountMergeState.READY) {
        // SOD 已就绪 → 直接累加
        accumulateIntraday(state, intraday);

    } else {
        // SOD 未就绪 → 缓存到 pendingIntradayDeltas
        IntradayDelta delta = IntradayDelta.from(intraday);
        state.getPendingIntradayDeltas().add(delta);
    }

    // ④ 标记已处理
    stateStore.markProcessed(accountId, intraday.getExecutionId());
    state.setLastUpdateTimestamp(System.currentTimeMillis());

    // ⑤ 输出结果
    return Optional.of(buildResult(state));
}
```

### 7.5 核心合并逻辑

```java
/**
 * 将一笔 Intraday 增量累加到当前持仓。
 */
private void accumulateIntraday(AccountPositionState state, UnifiedPosition intraday) {
    String symbol = intraday.getSymbol();

    // 获取或创建该 symbol 的合并持仓
    MergedPosition merged = state.getCurrentPositions()
        .computeIfAbsent(symbol, k -> {
            MergedPosition mp = new MergedPosition();
            mp.setSymbol(symbol);

            // 从 SOD 基线初始化 sodQty
            PositionSnapshot baseline = state.getSodBaseline().get(symbol);
            if (baseline != null) {
                mp.setSodQty(baseline.getSodQty());
            } else {
                mp.setSodQty("0");
            }
            mp.setTotalQty(mp.getSodQty());
            mp.setTodayQty("0");
            return mp;
        });

    // 累加增量
    BigDecimal total = new BigDecimal(merged.getTotalQty());
    BigDecimal delta = new BigDecimal(intraday.getQuantity());  // quantity 可能带正负号
    BigDecimal newTotal = total.add(delta);

    BigDecimal today = new BigDecimal(merged.getTodayQty());
    BigDecimal newToday = today.add(delta);

    merged.setTotalQty(newTotal.toPlainString());
    merged.setTodayQty(newToday.toPlainString());

    // 更新加权均价
    updateWeightedAvgPrice(merged, intraday.getAvgPrice(), delta);

    merged.setLastUpdateTime(System.currentTimeMillis());
}

/**
 * 回放缓存的 Intraday 增量。
 * 在 SOD 到达后、SOD 修正后调用。
 */
private void replayPendingIntraday(AccountPositionState state) {
    List<IntradayDelta> pending = state.getPendingIntradayDeltas();
    if (pending.isEmpty()) return;

    // 按到达时间排序后顺序回放
    pending.stream()
        .sorted(Comparator.comparingLong(IntradayDelta::getArrivalTimestamp))
        .forEach(delta -> {
            // 使用缓存的原始 Proto，但跳过已去重的
            if (!state.getDedupSet().contains(delta.getExecutionId())) {
                accumulateIntraday(state, delta.getRawProto());
                state.getDedupSet().add(delta.getExecutionId());
            }
        });

    pending.clear();
}

/**
 * 回滚当日增量（SOD 修正场景）。
 */
private void rollbackIntradayDeltas(AccountPositionState state) {
    for (MergedPosition merged : state.getCurrentPositions().values()) {
        // 总持仓回退到昨仓
        merged.setTotalQty(merged.getSodQty());
        merged.setTodayQty("0");
    }
}
```

### 7.6 结果构建

```java
/**
 * 从当前状态构建 UnifiedResultPosition Proto。
 */
private UnifiedResultPosition buildResult(AccountPositionState state) {

    UnifiedResultPosition.Builder result = UnifiedResultPosition.newBuilder()
        .setAccountId(state.getAccountId())
        .setBusinessDate(state.getBusinessDate())
        .setMergeState(mapMergeState(state.getMergeState()))
        .setLastUpdateTimestamp(state.getLastUpdateTimestamp());

    // 逐 symbol 填充持仓数据
    for (MergedPosition merged : state.getCurrentPositions().values()) {
        PositionDetail detail = PositionDetail.newBuilder()
            .setSymbol(merged.getSymbol())
            .setTotalQty(merged.getTotalQty())
            .setSodQty(merged.getSodQty())
            .setTodayQty(merged.getTodayQty())
            .setAvgPrice(merged.getAvgPrice())
            .build();
        result.addPositions(detail);
    }

    return result.build();
}
```

---

## 8. Result Publisher — 结果输出层

### 8.1 职责

- 将合并后的 `UnifiedResultPosition` 发布到 `harbour.result.position` Topic
- 按 `account_id` 分区（保持与输入一致）
- 携带幂等键
- 注入 traceparent

### 8.2 实现

```java
public class ResultPublisher {

    private static final String RESULT_TOPIC = "harbour.result.position";

    private final SequenceManager sequenceManager;

    /**
     * 发布计算结果到结果 Topic。
     * 在 Kafka Streams 拓扑中作为 sink node。
     */
    public KeyValue<String, UnifiedResultPosition> publish(
            UnifiedResultPosition result, String moduleSource) {

        // ① 构造幂等键
        String idempotencyKey = IdempotencyKey.of(
            moduleSource,                    // 如 "CHINA_POSITION_MODULE"
            result.getAccountId(),
            result.getBusinessDate(),
            sequenceManager.next(moduleSource,
                                 result.getAccountId(),
                                 result.getBusinessDate())
        );
        result = result.toBuilder()
            .setIdempotencyKey(idempotencyKey)
            .build();

        // ② 按 account_id 作为 Key（保证与输入相同的分区策略）
        return KeyValue.pair(result.getAccountId(), result);
    }
}
```

### 8.3 Kafka Streams Sink 配置

```java
// 在拓扑中注册 Sink
intradayStream
    .flatTransform(...)  // MergeEngine 处理
    .filter((key, result) -> result != null)
    .map((key, result) -> publisher.publish(result, moduleSource))
    .to(RESULT_TOPIC, Produced.with(
        Serdes.String(),
        ProtoSerde.unifiedResultPosition()
    ));
```

---

## 9. AbstractCalculationModule — 模板方法骨架

### 9.1 抽象基类

```java
/**
 * 计算模块抽象基类。
 * 定义 Kafka Streams 拓扑骨架，子类通过注入实现市场差异。
 *
 * @param <T> 市场专属扩展类型（MarketSpecificConfig）
 */
public abstract class AbstractPositionCalculationModule {

    // ── 抽象方法：子类实现市场差异 ──
    /** 模块标识，用于幂等键 source 字段 */
    protected abstract String moduleSource();

    /** SOD Topic 名称 */
    protected abstract String sodTopic();

    /** Intraday Topic 名称 */
    protected abstract String intradayTopic();

    /** 结果 Topic 名称 */
    protected abstract String resultTopic();

    /** 市场专属的扩展解析器列表 */
    protected abstract List<ExtensionResolver> extensionResolvers();

    /** 市场专属的持仓处理规则（如中国今仓/昨仓逻辑） */
    protected abstract List<PositionProcessingRule> processingRules();

    // ── 模板方法：构建 Kafka Streams 拓扑 ──
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // ── Step 1: 构建 SOD GlobalKTable ──
        GlobalKTable<String, UnifiedPosition> sodTable =
            SodConsumerConfig.buildGlobalTable(
                builder, sodTopic(),
                Serdes.String(),
                ProtoSerde.unifiedPosition()
            );

        // ── Step 2: 构建 Intraday KStream ──
        KStream<String, UnifiedPosition> intradayStream =
            IntradayConsumerConfig.buildStream(
                builder, intradayTopic(),
                Serdes.String(),
                ProtoSerde.unifiedPosition()
            );

        // ── Step 3: 注入市场依赖 ──
        PositionStateStore stateStore = createStateStore();
        SodIntradayMergeEngine mergeEngine = createMergeEngine(stateStore, sodTable);
        ResultPublisher publisher = createPublisher();

        // ── Step 4: 处理管线 ──
        intradayStream
            // ④-a: Schema Registry 校验
            .mapValues(this::schemaValidate)
            // ④-b: 前置处理（市场特有）
            .flatMapValues(this::preProcess)
            // ④-c: 构建 MergeOperation → 提交到 MergeEngine
            .flatTransform(() -> new PositionMergeTransformer(
                mergeEngine, stateStore, sodTable
            ), Named.as("position-merge"), stateStore.storeName())
            // ④-d: 后置处理（告警、指标采集）
            .mapValues(this::postProcess)
            // ④-e: 发布到结果 Topic
            .map((key, result) -> publisher.publish(result, moduleSource()))
            .to(resultTopic(), Produced.with(
                Serdes.String(),
                ProtoSerde.unifiedResultPosition()
            ));

        return builder.build();
    }

    // ── 钩子方法：子类可覆写 ──
    protected UnifiedPosition schemaValidate(UnifiedPosition proto) {
        // Schema Registry 校验（基类实现，子类通常不覆写）
        return proto;
    }

    protected List<UnifiedPosition> preProcess(UnifiedPosition intraday) {
        // 市场扩展解析 + 自定义规则
        List<UnifiedPosition> expanded = new ArrayList<>();
        for (ExtensionResolver resolver : extensionResolvers()) {
            if (resolver.appliesTo(intraday)) {
                expanded.add(resolver.resolve(intraday));
            }
        }
        return expanded.isEmpty() ? List.of(intraday) : expanded;
    }

    protected UnifiedResultPosition postProcess(UnifiedResultPosition result) {
        // 指标采集、告警触发、stale 账户标记
        return result;
    }

    // ── 工厂方法 ──
    protected PositionStateStore createStateStore() {
        return new PositionStateStore();
    }

    protected SodIntradayMergeEngine createMergeEngine(
            PositionStateStore stateStore,
            GlobalKTable<String, UnifiedPosition> sodTable) {
        return new SodIntradayMergeEngine(stateStore, sodTable);
    }

    protected ResultPublisher createPublisher() {
        return new ResultPublisher(new SequenceManager());
    }

    // ── 启动方法 ──
    public void start() {
        Properties props = buildStreamsProperties();
        Topology topology = buildTopology();
        KafkaStreams streams = new KafkaStreams(topology, props);

        // 注册 JVM shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // 注册状态变更监听（用于健康检查）
        streams.setStateListener((newState, oldState) -> {
            log.info("State transition: {} -> {}", oldState, newState);
            if (newState == KafkaStreams.State.ERROR) {
                alertManager.fireP0("Position module state transition to ERROR");
            }
        });

        streams.start();
    }

    private Properties buildStreamsProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, moduleSource() + "-position-v1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
                  StreamsConfig.EXACTLY_ONCE_V2);
        // Changelog 配置
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/data/kafka-streams/" + moduleSource());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);  // 100ms 提交间隔
        return props;
    }
}
```

### 9.2 PositionMergeTransformer — Kafka Streams Transformer

```java
/**
 * Kafka Streams flatTransform 实现。
 * 桥接 KStream 处理模型与 MergeEngine。
 *
 * 核心逻辑：
 *  ① 从 GlobalKTable 查询当前 SOD 基线
 *  ② 确定这条消息是 SOD 变更还是 Intraday 到达
 *  ③ 构造 MergeOperation → 调用 MergeEngine
 *  ④ 输出计算结果（或跳过）
 */
public class PositionMergeTransformer
        implements Transformer<String, UnifiedPosition,
                               KeyValue<String, UnifiedResultPosition>> {

    private final SodIntradayMergeEngine mergeEngine;
    private final PositionStateStore stateStore;
    private final GlobalKTable<String, UnifiedPosition> sodTable;

    private KeyValueStore<String, AccountPositionState> kvStore;
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.kvStore = context.getStateStore(stateStore.storeName());
        this.stateStore.setStore(this.kvStore);
    }

    @Override
    public KeyValue<String, UnifiedResultPosition> transform(
            String key, UnifiedPosition position) {

        String accountId = position.getAccountId();
        String businessDate = position.getBusinessDate();

        // ① 从 GlobalKTable 查询当前 SOD 基线（判断 SOD 是否已到）
        UnifiedPosition sodBaseline = sodTable.get(accountId);

        // ② 构造 MergeOperation
        MergeOperation operation = buildOperation(position, sodBaseline);

        // ③ 调用合并引擎
        Optional<UnifiedResultPosition> result = mergeEngine.process(operation);

        // ④ 返回结果（或 null 表示跳过）
        return result.map(r -> KeyValue.pair(accountId, r)).orElse(null);
    }

    private MergeOperation buildOperation(
            UnifiedPosition position, UnifiedPosition sodBaseline) {

        // 判断消息类型：SOD 还是 Intraday？
        if (isSodMessage(position)) {
            AccountPositionState state = stateStore.get(position.getAccountId());
            if (state == null || state.getCurrentSodVersion() < extractSodVersion(position)) {
                return MergeOperation.of(
                    state == null ? MergeOperationType.SOD_ESTABLISH
                                  : MergeOperationType.SOD_CORRECTION,
                    position
                );
            } else {
                // 旧版本 SOD，跳过
                return MergeOperation.skip();
            }
        } else {
            return MergeOperation.of(MergeOperationType.INTRA_ACCUMULATE, position);
        }
    }

    private boolean isSodMessage(UnifiedPosition position) {
        // SOD 消息的特征：source 为 "GMI_DB" 或 header 中标记为 SOD
        return "GMI_DB".equals(position.getSource());
    }

    @Override
    public void close() { /* no-op */ }
}
```

---

## 10. 市场专属实现 — 以 ChinaPositionModule 为例

### 10.1 中国市场专属扩展解析

```java
/**
 * 中国市场持仓计算模块。
 * 只覆写注入组件，不覆写拓扑逻辑。
 */
public class ChinaPositionModule extends AbstractPositionCalculationModule {

    @Override
    protected String moduleSource() {
        return "CHINA_POSITION_MODULE";
    }

    @Override
    protected String sodTopic() {
        return "harbour.unified.position.sod";
    }

    @Override
    protected String intradayTopic() {
        return "harbour.unified.position.intraday";
    }

    @Override
    protected String resultTopic() {
        return "harbour.result.position";
    }

    @Override
    protected List<ExtensionResolver> extensionResolvers() {
        return List.of(
            new ChinaPositionExtensionResolver()
        );
    }

    @Override
    protected List<PositionProcessingRule> processingRules() {
        return List.of(
            new ChinaTodaySodPositionRule(),  // 今仓/昨仓区分
            new ChinaCoveredOptionRule()       // 备兑标记处理
        );
    }
}

// ── 市场专属扩展解析器 ──
public class ChinaPositionExtensionResolver implements ExtensionResolver {

    @Override
    public boolean appliesTo(UnifiedPosition position) {
        return position.containsExtensions(
            ExtensionKey.EXT_CHINA_POSITION.name()
        );
    }

    @Override
    public UnifiedPosition resolve(UnifiedPosition position) {
        ChinaPositionExt ext = position.getExtensionsMap()
            .get(ExtensionKey.EXT_CHINA_POSITION.name())
            .unpack(ChinaPositionExt.class);

        // 中国市场特有逻辑：今仓/昨仓分离
        // 将扩展字段提升为计算时可用的上下文
        // （这里只是读取、校验，不做聚合 — 聚合在 MergeEngine 中完成）
        return position.toBuilder()
            .putMetadata("china_today_qty", ext.getTodayQty())
            .putMetadata("china_sod_qty", ext.getSodQty())
            .putMetadata("china_is_covered", String.valueOf(ext.getIsCovered()))
            .build();
    }
}

// ── 市场专属处理规则 ──
public class ChinaTodaySodPositionRule implements PositionProcessingRule {

    @Override
    public boolean appliesTo(UnifiedPosition position) {
        return "CHINA_GENIE".equals(position.getSource());
    }

    @Override
    public void apply(AccountPositionState state, UnifiedPosition intraday) {
        // 中国市场：今仓增量只影响 todayQty，sodQty 不变
        // 此逻辑在 accumulateIntraday 中已通过 delta.add(todayQty) 实现
        // 这里可以做额外的中国市场校验
    }
}
```

### 10.2 也适用于无扩展的简单市场

```java
/**
 * 韩国市场持仓计算模块 — 无特有扩展。
 * 最简实现，只需覆写标识和 Topic。
 */
public class KoreaPositionModule extends AbstractPositionCalculationModule {

    @Override
    protected String moduleSource() { return "KOREA_POSITION_MODULE"; }

    @Override
    protected String sodTopic() { return "harbour.unified.position.sod"; }

    @Override
    protected String intradayTopic() { return "harbour.unified.position.intraday"; }

    @Override
    protected String resultTopic() { return "harbour.result.position"; }

    @Override
    protected List<ExtensionResolver> extensionResolvers() {
        return List.of();  // 韩国无特有扩展
    }

    @Override
    protected List<PositionProcessingRule> processingRules() {
        return List.of();  // 韩国无特殊处理规则
    }
}
```

---

## 11. 故障恢复全流程

### 11.1 正常故障恢复（秒级）

```
场景：实例 2（负责 partition 4-7）挂了 5 分钟后重启

恢复步骤：
  ① Kafka Streams 检测到 partition 重新分配
  ② Changelog Topic 回放：从上次 committed offset 恢复 StateStore（秒级）
     → SOD 基线数据恢复
     → dedupSet 恢复
     → currentPositions 恢复
     → pendingIntradayDeltas 恢复
  ③ Intraday Topic 追上：从上次 committed offset 消费丢失的 5 分钟数据（分钟级）
  ④ 状态达到 READY，正常服务

总恢复耗时 = Changelog 回放（< 5s）+ Intraday 追上（< 30s for 5 分钟积压）
```

### 11.2 交易日切换恢复

```
场景：跨交易日，StateStore 中的旧交易日数据需要清空

处理方式：
  ① PositionStateStore.ensureBusinessDate() 在每个消息处理前检查
  ② 检测到 businessDate 变更 → 自动创建新状态，旧状态被覆盖
  ③ Changelog Topic 会记录这次状态变更
  ④ 如果切换过程中崩溃，重启后 Changelog 回放 → 新状态恢复
```

### 11.3 SOD 数据源故障

```
场景：GMI DB 故障，SOD 数据延迟到交易时段才到达

处理方式（已在 MergeEngine 中实现）：
  ① Intraday 先到 → 缓存到 pendingIntradayDeltas（状态 = INITIALIZING）
  ② SOD 延迟到达 → 回放缓存的 Intraday → 状态 → READY
  ③ 下游查询接口看到 INITIALIZING 状态，返回临时结果
```

### 11.4 SOD 全量重发

```
场景：GMI 修正了整个交易日的 SOD 数据

处理方式：
  ① 每条 SOD 消息的 sod_version 递增
  ② MergeEngine 检测到高版本 → 回滚当日增量 → 新基线重建
  ③ 这个过程对每个 account 独立，互不影响
```

---

## 12. Changelog 状态恢复机制

### 12.1 Changelog 配置

```java
// Kafka Streams 自动管理 Changelog Topic
// 配置：
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
          StreamsConfig.EXACTLY_ONCE_V2);  // 事务性写入 Changelog

// Changelog Topic 命名：{application_id}-{store_name}-changelog
// 例：CHINA_POSITION_MODULE-position-v1-position-state-store-changelog

// Changelog Topic 属性（自动创建）：
//   cleanup.policy = compact（同 key 只保留最新版本）
//   replication.factor = 3（与 Kafka 集群一致）
//   retention.ms = -1（无限保留，由 Streams 管理清理）
```

### 12.2 状态恢复流程

```
实例重启
    │
    ▼
KafkaStreams 初始化
    │
    ├── ① 检测本地 RocksDB 状态是否完整
    │     完整 → 直接启动
    │     不完整（磁盘故障、新节点） → 继续 ②
    │
    ├── ② 从 Changelog Topic 回放状态
    │     读取 {store_name}-changelog 中的所有 key
    │     Key: accountId → Value: AccountPositionState (序列化)
    │     写入本地 RocksDB
    │     耗时：< 5s（取决于账户数量）
    │
    ├── ③ 从 Standby Tasks 恢复 (如果有)
    │     Kafka Streams 的 standby replicas 持有同步副本
    │     本地恢复 → 秒级
    │
    └── ④ 恢复完成 → 继续消费 Intraday Topic
          从 last committed offset 开始
```

### 12.3 状态迁移（版本兼容）

```java
/**
 * 当 AccountPositionState 结构变更时（如新增字段），
 * 需要处理 Changelog 中旧版本数据的反序列化。
 */
public class PositionStateSerde implements Serde<AccountPositionState> {

    @Override
    public AccountPositionState deserialize(String topic, byte[] data) {
        try {
            // 尝试最新版本反序列化
            return ProtoUtils.parse(AccountPositionState.proto(), data);
        } catch (Exception e) {
            // 降级：尝试 V1 版本 → 迁移到 V2
            AccountPositionStateV1 v1 = ProtoUtils.parse(
                AccountPositionStateV1.proto(), data
            );
            return migrate(v1);  // 向上迁移
        }
    }

    private AccountPositionState migrate(AccountPositionStateV1 v1) {
        // V1 → V2 迁移逻辑
        // 如：新增字段 currentSodVersion = v1.getSodVersion()
    }
}
```

---

## 13. Class 清单汇总

### 13.1 通用框架（所有市场复用）

| 层级 | Class / Interface | 复用度 | 说明 |
|------|------------------|:------:|------|
| **接口** | `ExtensionResolver` | 100% | 扩展字段解析契约 |
| **接口** | `PositionProcessingRule` | 100% | 市场专属规则契约 |
| **状态机** | `AccountMergeState` | 100% | 状态枚举 |
| **状态机** | `MergeOperation` / `MergeOperationType` | 100% | 合并操作定义 |
| **Consumer** | `SodConsumerConfig` | 100% | SOD GlobalKTable 构建器 |
| **Consumer** | `IntradayConsumerConfig` | 100% | Intraday KStream 构建器 |
| **Consumer** | `IntradayDedupFilter` | 100% | Intraday 去重过滤器 |
| **State** | `PositionStateStore` | 100% | 状态存储 CRUD |
| **State** | `AccountPositionState` | 100% | 账户状态数据结构 |
| **State** | `PositionSnapshot` | 100% | SOD 快照子结构 |
| **State** | `IntradayDelta` | 100% | 日内增量子结构 |
| **State** | `MergedPosition` | 100% | 合并结果子结构 |
| **State** | `PositionStateSerde` | 100% | 状态序列化/反序列化 |
| **Engine** | `SodIntradayMergeEngine` | 100% | 合并引擎核心逻辑 |
| **Publisher** | `ResultPublisher` | 100% | 结果发布器 |
| **Publisher** | `SequenceManager` | 100% | 幂等序列号管理 |
| **骨架** | `AbstractPositionCalculationModule` | 100% | 模板方法拓扑构建 |
| **Transformer** | `PositionMergeTransformer` | 100% | Kafka Streams Transformer |

### 13.2 市场专属实现（每个市场新建）

| 层级 | Class | 说明 |
|------|-------|------|
| **组装类** | `{Market}PositionModule` | extends AbstractPositionCalculationModule，注入组件（~20 行） |
| **扩展解析** | `{Market}ExtensionResolver` | 实现 ExtensionResolver，解析市场特有扩展（~30 行，如无扩展可省略） |
| **处理规则** | `{Market}ProcessingRule` | 实现 PositionProcessingRule，市场特有持仓逻辑（~30 行，如无特殊规则可省略） |

### 13.3 新市场接入工作量

新市场接入时，需要新建的 class 为 **1-3 个**：

| 产出物 | 数量 | 工作量 |
|--------|:----:|--------|
| `{Market}PositionModule`（组装类，覆写注入） | 1 | 极小（~20 行） |
| `{Market}ExtensionResolver`（如有市场特有扩展） | 0-1 | 小（~30 行） |
| `{Market}ProcessingRule`（如有特殊持仓规则） | 0-1 | 小（~30 行） |
| **总计** | **1-3 个文件** | **< 半天** |

> 其余 18 个通用 class / interface **全部复用，零修改**。

---

> **关联文档**：
> - `最新的文档.md` — 完整架构设计 V1.4
> - `adaptor-architecture.md` — Adaptor 内部 Class 架构
> - `设计图.md` — 平台架构设计图
>
> **更新日期**：2026-01-15
