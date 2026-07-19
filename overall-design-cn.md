# Harbour 统一持仓与保证金中台 — 完整架构设计

## 目录

1. [项目概述](#1-项目概述)
   - 1.1 [项目定位](#11-项目定位)
   - 1.2 [解决的核心痛点](#12-解决的核心痛点)
   - 1.3 [整体架构理念](#13-整体架构理念)
   - 1.4 [设计原则](#14-设计原则)
2. [整体分层架构与职责](#2-整体分层架构与职责)
   - 2.1 [上游异构数据源层](#21-上游异构数据源层)
   - 2.2 [适配器标准化层](#22-适配器标准化层-adaptor)
   - 2.3 [Kafka 统一消息总线层](#23-kafka-统一消息总线层) *（2.3.3：SOD 与 Intraday 分 Topic 设计）*
   - 2.4 [计算模块层](#24-计算模块层核心业务层) *（2.4.4：目标市场实时持仓计算）*
   - 2.5 [下游结果输出层](#25-下游结果输出层)
   - 2.6 [查询服务层（CQRS）](#26-查询服务层query-service--cqrs-读写分离)
3. [端到端完整数据流](#3-端到端完整数据流)
4. [统一数据模型设计](#4-统一数据模型设计核心--扩展机制)
   - 4.1 [设计背景](#41-设计背景)
   - 4.2 [扩展机制设计原则](#42-扩展机制设计原则)
   - 4.3 [技术方案：map<string, Any> 扩展槽](#43-技术方案mapstring-googleprotobufany-扩展槽)
   - 4.4 [方案选型对比](#44-方案选型对比)
   - 4.5 [扩展机制治理规范](#45-扩展机制治理规范)
   - 4.6 [Schema Registry](#46-schema-registry消息-schema-全生命周期治理)
   - 4.7 [深度解析：核心归核心，扩展归扩展](#47-深度解析核心归核心扩展归扩展)
5. [查询服务（CQRS）](#5-查询服务层cqrs-读写分离)
   - 5.1 [建设背景](#51-建设背景)
   - 5.2 [架构链路](#52-架构链路)
   - 5.3 [核心能力](#53-核心能力)
   - 5.4 [价值](#54-价值)
6. [SOD 与日内增量数据合并时序](#6-sod-与日内增量数据合并时序)
   - 6.1 [关键时序场景](#61-关键时序场景)
   - 6.2 [去重策略](#62-去重策略)
   - 6.3 [状态标记](#63-状态标记)
7. [模块间数据管线](#7-模块间数据管线)
   - 7.1 [为什么不用单一模块？](#71-为什么不用单一模块)
   - 7.2 [数据流细节](#72-数据流细节)
   - 7.3 [顺序保证](#73-顺序保证)
   - 7.4 [故障隔离](#74-故障隔离)
   - 7.5 [通用模式](#75-通用模式)
8. [新市场接入](#8-新市场接入与代码复用规则)
   - 8.1 [复用与新增明细](#81-复用与新增明细)
   - 8.2 [新市场接入工作量总结](#82-新市场接入工作量总结)

---

## 1. 项目概述

### 1.1 项目定位

Harbour 是面向多市场、多交易所的**统一持仓与保证金计算中台**。平台统一对接各区域异构数据源，标准化数据格式、计算逻辑与接口规范，为下游交易、结算、风控、监控系统提供一致、可靠的持仓与保证金数据。

### 1.2 解决的核心痛点

- 各交易所/市场数据源协议、字段、业务语义差异大，跨市场接入重复适配成本极高；
- 传统架构为每个市场独立开发全套代码，重复工作和维护成本高；
- 下游系统需要对接多套异构接口，联调和运维负担重；
- 原有架构无法支撑 多级子账户统一管理、跨市场统一风控计算。

### 1.3 整体架构理念

> **统一数据模型 + 隔离适配器层 + 可复用计算模块 + Kafka 流驱动架构 + CQRS 查询服务 + 标准化扩展机制**

---

## 2. 整体分层架构与职责

平台从上至下共分为 **6 个逻辑独立层级**，每层职责单一、边界清晰，支持独立开发、测试、部署、扩缩容。

```
                         ┌──────────────────────────────────────┐
                         │         上游异构数据源层              │
                         │  FTP · Kafka · REST · Database      │
                         └────────────────┬─────────────────────┘
                                          ▼
                         ┌──────────────────────────────────────┐
                         │        适配器标准化层 (Adaptor)        │
                         │  Parse → Clean → Derive → Wrap →    │
                         │  OctaneMessage { key, body, ... }   │
                         └────────────────┬─────────────────────┘
                                          ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │                  Kafka 统一消息总线层                                │
  │                                                                     │
  │  所有 Topic 传输 OctaneMessage (RoutingKey + Protobuf body)         │
  │  position.sod · position.intraday · margin.sod · margin.intraday    │
  │  能力：重放 · 回填 · 断点续传 · 分区并行消费                          │
  └────────────────────────────────┬────────────────────────────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    ▼                             ▼
  ┌─────────────────────────────────┐  ┌─────────────────────────────────┐
  │  持仓计算服务 (Position Calc)    │  │  保证金计算服务 (Margin Calc)     │
  │  (独立微服务)                    │  │  (独立微服务)                    │
  │                                  │  │                                  │
  │  • SOD + Intraday 合并           │  │  • 保证金规则引擎                │
  │  • 市场专属扩展解析              │  │  • 参考数据缓存                  │
  │  • 子账户聚合                    │  │    (费率、 haircut、参数)         │
  │  • 全局跨市场汇总                │  │  • 人工超控支持                  │
  │  • execution_id 去重             │  │  • 多源聚合                      │
  │  • 状态: INITIALIZING/READY/     │  │                                  │
  │    STALE                         │  │                                  │
  └───────────────┬─────────────────┘  └────────┬──────────────┬───────────┘
                  │ publish                      │              │ publish
                  ▼                              │              ▼
  ┌──────────────────────────────┐              │    ┌──────────────────────────────┐
  │  harbour.result.position      │──────────────┘    │  harbour.result.margin        │
  │  (Kafka, keyed by account_id) │                   │  (Kafka, keyed by account_id) │
  └──────────────┬───────────────┘                   └──────────────┬───────────────┘
                 │                                                  │
                 ▼                                                  ▼
  ┌──────────────────┐                                 ┌──────────────────┐
  │  Query Service    │                                 │  Downstream       │
  │  (CQRS)           │                                 │  Systems          │
  │                   │                                 │                  │
  │  RocksDB · REST   │                                 │  通过 Query       │
  │  gRPC             │                                 │  Service 访问     │
  └──────────────────┘                                 └──────────────────┘
```

### 2.1 上游异构数据源层

汇聚所有业务市场原始数据，不同市场使用不同协议、结构与语义：

| 数据源类型 | 示例 | 协议 |
|-----------------|---------|----------|
| 实时流 | Kafka topic | Kafka |
| 批量文件 | FTP/SFTP 下载 | FTP |
| REST API | HTTP 端点 | HTTP |
| 数据库 | SQL 查询/快照 | DB |

### 2.2 适配器标准化层 (Adaptor)

每个市场/数据源对应独立的适配器服务，负责原始数据解析、清洗、标准化、格式转换。

#### 2.2.1 现有适配器清单

- **SOD 适配器** — 从数据库或 FTP 读取日终持仓快照
- **流式适配器** — 从 Kafka 或 REST API 消费实时成交流

#### 2.2.2 核心职责

解析上游原始数据，完成清洗、字段映射、语义封装，包装为 `OctaneMessage` 信封，发布至 Kafka 消息总线。

#### 2.2.3 允许执行的操作（白名单）

> 适配器在单条消息范围内操作。允许单消息内的计算推导（如方向 × 数量 → 持仓变动量）。跨消息、跨账户的逻辑应归属计算层，不在适配器中处理。

- 字段映射、数据类型转换、时间戳统一格式化；
- 基于数据完整性规则过滤脏数据（空值、非法枚举、越界数据）；
- 从单条原始成交流水推导统一模型必需基础字段（如根据成交方向 + 数量计算持仓变动）；
- 将市场私有业务语义打包为标准扩展消息 (`extensions`)。

#### 2.2.4 应由计算层处理的操作（非适配器职责）

- 保证金、盈亏、风险指标的计算；
- 跨多条记录、跨账户的持仓聚合、对冲合并、头寸归并；
- 子账户层级关系推导。

> **约束落地**：通过 Code Review + 自动化检查，保持边界清晰。

### 2.3 Kafka 统一消息总线层

平台核心数据枢纽，承担数据传输、缓冲、回放、回填、故障恢复能力。所有链路仅传输 `OctaneMessage` — 统一信封包裹 Protobuf 编码的业务载荷。每个生产者和消费者都读写这唯一的消息类型；信封中的 `RoutingKey` 字段决定反序列化 `body` 时使用哪个 schema。

#### 2.3.1 标准 Topic 划分（按数据类型 + 时间维度）

| Topic | 说明 |
|-------|-------------|
| `harbour.unified.position.sod` | 日初快照持仓数据 |
| `harbour.unified.position.intraday` | 日内增量实时持仓数据 |
| `harbour.unified.margin.sod` | 日初快照保证金数据 |
| `harbour.unified.margin.intraday` | 日内增量实时保证金数据 |

#### 2.3.2 基础能力

支持数据重放、历史回填、故障断点续传；所有 Topic 数据结构全局统一，**不区分市场**。

#### 2.3.3 深度解析：SOD 与 Intraday 分 Topic 设计

> 为什么 SOD 和 Intraday 必须是两个 Topic？本节从 6 个维度论证分开设计的必要性。

**两种数据的本质差异**：

| 属性 | SOD (Start of Day) | Intraday (日内增量) |
|-----------|-------------------|----------|
| **来源** | 数据库快照或 FTP 文件 | 实时适配器 (OSM 成交流等) |
| **频率** | 每天 1 次（开盘前） | 交易时段持续产生 |
| **体量** | 全量快照（所有账户一次性） | 单笔成交粒度 |
| **语义** | 完整持仓基线 | 增量变动 |
| **延迟要求** | 分钟级可容忍 | 毫秒级 |
| **可靠性** | 错了可以等修正版重发 | 丢了就是永久丢失 |
| **恢复** | 重放今天的 SOD 即可 | 需要 SOD + 重放当天所有 Intraday |

---

**维度一：消费语义完全不同**

SOD 是"建基线"，Intraday 是"增量累加"。如果混在一个 Topic，消费者每条消息都要做类型判断：

```
混合 Topic（❌ 反模式）：
  [SOD  account=A01 qty=100]  ← 基线
  [INT  account=A01 delta=+2] ← 增量
  [INT  account=A02 delta=-5]
  [SOD  account=A02 qty=50]   ← A02 基线出现在增量之后？乱序！
  [INT  account=A01 delta=-1]
  [SOD  account=A01 qty=120]  ← 是修正版 SOD 还是重复消息？
```

消费者被迫维护复杂状态机：这条是 SOD 还是 Intraday？该账户的 SOD 到了吗？这是修正版吗？所有逻辑混在一起。

**分开后**：消费者订阅两个 Topic，SOD Consumer 只关心建基线，Intraday Consumer 只关心增量累加，职责清晰。

---

**维度二：吞吐量特征冲突**

```
                        开盘前 5 分钟          交易时段（4 小时）
                        ──────────────        ──────────────────
SOD Topic               全量 burst            几乎无流量
                        (数千账户 × 几十条持仓)
Intraday Topic          无流量                持续均匀流
                        (还没开盘)             (每秒数百条)
```

如果混在一起，Kafka 分区设计陷入两难：分区数按 SOD 峰值设计则浪费资源，按 Intraday 稳态设计则开盘时积压。分开后可独立调优。

---

**维度三：保留策略不同**

| 策略 | SOD | Intraday |
|--------|-----|----------|
| **保留期** | 30 天（合规审计需要日初快照） | 7 天（可从 SOD + 后续 Intraday 重建） |
| **压缩策略** | `cleanup.policy=compact`（同 key 只保留最新版本） | `cleanup.policy=delete`（每条增量都有意义，禁止压缩） |
| **删除策略** | 按天整批删除 | 按时间窗口滚动 |

Kafka Topic 的 `retention.ms` 和 `cleanup.policy` 是 Topic 级别的，混在一起只能取两者交集 → 要么 SOD 保留不够、要么 Intraday 浪费存储。

---

**维度四：故障恢复路径完全不同**

```
场景：计算模块挂了 30 分钟，需要恢复

如果分开：
  ① SOD Consumer：从 Changelog 恢复基线状态（秒级）
  ② Intraday Consumer：从上次 committed offset 继续消费 30 分钟的数据（分钟级）
  ③ 合并：基线 + 增量 → 正常状态
  两条链路独立，互不干扰

如果混在一起：
  必须从唯一 Topic 的某个 offset 开始重放
  → 无法跳过 SOD 消息
  → 无法区分哪些是增量、哪些是基线
  → 恢复逻辑极其复杂
```

这与  Changelog 状态恢复机制配合：分开的 Topic 才能独立做 Changelog。

---

**维度五：分区键与分区数策略不同**

| 属性 | SOD | Intraday |
|-----------|-----|----------|
| **分区键** | `account_id` | `account_id` |
| **分区数** | 8（数据量小，一次性） | 32（按交易时段吞吐量设计） |
| **消息 Key** | `account_id:business_date` | `execution_id` |

虽然都用 `account_id` 分区以保证 co-partition（同一账户的 SOD 和 Intraday 进入同一消费者实例），但分区数不同是有道理的 — SOD 数据量小可以少分区减少开销，Intraday 需要多分区支撑并发消费。分开才能独立调整分区数。

---

**维度六：消息版本与去重语义不同**

| 语义 | SOD | Intraday |
|----------|-----|----------|
| **版本控制** | `sod_version` 递增（修正重发） | 无版本概念 |
| **重复处理** | 同一 `account_id + business_date` 的新版 SOD **覆盖**旧版 | `execution_id` 全局唯一，重复消息**跳过** |
| **消费者行为** | 收到新版 SOD → 回滚当日增量 → 用新版 SOD 重建基线 | 查询去重集合 → 命中则丢弃 |

两种完全不同的去重/版本语义，混在一起消费者要同时维护两套逻辑。

---

**Topic 规划总表**：

```
harbour.unified.position.sod
  ├── 分区键：account_id
  ├── 分区数：8（可调）
  ├── 保留：30 天
  ├── 压缩：cleanup.policy=compact（同 key 保留最新版本）
  ├── 语义：日初基线快照
  └── 版本：sod_version 递增

harbour.unified.position.intraday
  ├── 分区键：account_id
  ├── 分区数：32（可调，按吞吐量）
  ├── 保留：7 天
  ├── 压缩：cleanup.policy=delete（不压缩，每条有意义）
  ├── 语义：日内增量变动
  └── 去重：execution_id

harbour.unified.margin.sod        ← 同上（position → margin）
harbour.unified.margin.intraday   ← 同上（position → margin）
```

---

**唯一不需要"分"的场景**：如果某个市场只有 Intraday 没有 SOD（该市场不提供日初快照，全靠实时流累积），则其 Intraday Topic 设计不变，只是没有对应的 SOD Topic。不存在需要"合成一个 Topic"的情况。

**结论**：SOD 和 Intraday **必须是**两个 Topic。这是以数据语义为边界的设计决策，不是拍脑袋。

| 维度 | 分开 | 合在一起 |
|-----------|:---------:|:--------:|
| 消费语义清晰度 | ✅ 两个 Consumer 各司其职 | ❌ 单一 Consumer 被迫做类型判断 |
| 吞吐量调优 | ✅ SOD/Intraday 独立扩分区 | ❌ 峰值和稳态无法兼顾 |
| 保留策略 | ✅ SOD 30天压缩 / Intraday 7天不压缩 | ❌ 只能取交集 |
| 故障恢复 | ✅ 两条链路独立恢复 | ❌ 恢复逻辑极其复杂 |
| 分区策略 | ✅ 独立调整分区数与分区键语义 | ❌ 互相掣肘 |
| 版本/去重语义 | ✅ 各自独立管理 | ❌ 两套逻辑混在一起 |
| 运维复杂度 | ✅ 职责分离，问题定位快 | ❌ 一条 Topic 承载两种语义 |

### 2.4 计算模块层（业务核心层）

> 计算模块的完整 Class 架构、状态机实现、Kafka Streams 拓扑、故障恢复等详细设计见独立文档：
> - Position 计算模块：**`calculation-module-architecture.md`**
> - Margin 计算模块（多源聚合 + 人工超控 + 参考数据缓存）：**`margin-module-architecture.md`**

所有计算模块仅订阅 Kafka 统一流，与上游数据源完全解耦。完成业务计算后，结果包装为 `OctaneMessage` 输出至结果类 Kafka Topic。

**持仓计算与保证金计算是两个独立微服务** — 独立进程、独立部署、独立扩缩容。它们永远不同进程、不直连 RPC 通信：持仓计算将结果发布至 `harbour.result.position`，保证金计算从该 Kafka Topic 消费（见 [§3.8](#7-模块间数据管线)）。

#### 2.4.1 模块分类

| 类型 | 模块 | 部署模式 |
|------|--------|----------------|
| 市场专属计算模块 | 中国持仓/保证金、印度、韩国、港交所模块 | 各市场独立部署，按市场吞吐量独立扩容 |
| 全局跨市场汇总模块 | 全市场持仓、保证金统一聚合汇总模块 | Consumer Group 多实例，按 `account_id` 分区水平扩展 |

#### 2.4.2 全局汇总分片策略

各市场计算模块输出的结果 Topic 统一使用 `account_id` 作为 Kafka 分区键，保证同一账户的跨市场结果消息全部落入同一分区。全局汇总模块以 Consumer Group 多实例模式消费，每个实例负责若干分区，在实例内部完成该账户粒度的跨市场聚合。

```
市场模块 A ──→ 结果 Topic (partition=hash(account_id))
市场模块 B ──→ 结果 Topic (partition=hash(account_id))
                              │
            ┌─────────────────┼─────────────────┐
            ▼                 ▼                 ▼
    汇总实例 1          汇总实例 2          汇总实例 3
    (分区 0-3)         (分区 4-7)         (分区 8-11)
```

- **按账户有序**：同一 `account_id` 始终由同一分区同一实例处理，保证聚合的严格顺序性；
- **水平扩展**：新增实例时增加分区数并 Rebalance，吞吐线性增长；
- **故障隔离**：单实例故障仅影响其负责的分区账户，其余分区不受影响；
- **跨账户全局指标**（如全市场总持仓）：由 Query Service 的物化视图做跨账户 `SUM` 聚合，或增加一层轻量级全局聚合层（消费每个分区的聚合结果做二次汇总），不在流处理层实现。

全局结果 Topic 同样按 `account_id` 分区，下游 Query Service 按相同分区键消费，写入各层缓存。

#### 2.4.3 核心计算能力

- 多级子账户持仓、保证金聚合计算；
- 日初快照 (SOD) 与日内增量数据合并计算；
- 期货、期权全品类持仓计算，期权冻结持仓、保证金优惠逻辑；
- 各市场专属保证金规则计算（阶梯保证金、SPAN、交易所特定规则等）；
- 跨市场统一风险指标统计。

#### 2.4.4 示例：实时持仓计算流程

以下是一个具体的持仓计算管线，展示抽象的分层架构如何映射到实现：

```
第一步：加载 T-1 SOD 基线
  ┌─────────────────────────────────────────────────────┐
  │  T-1 SOD 持仓文件（FTP 或数据库快照）                  │
  │  ↓ 过滤：保留相关记录                                 │
  │  ↓ 按 key 聚合：账户 + 合约代码 + 多/空                │
  │  ↓ 结果：每个 key 的 T-1 SOD 持仓基线                  │
  └─────────────────────────────────────────────────────┘

第二步：将 T-1 成交补充到基线上
  ┌─────────────────────────────────────────────────────┐
  │  Kafka Topic：T-1 所有成交消息                         │
  │  ↓ 按相同 key 聚合                                     │
  │    (账户 + 合约代码 + 多/空)                            │
  │  ↓ 合并到 T-1 SOD 基线上                               │
  │  ↓ 结果：T 日 SOD 持仓数据                             │
  └─────────────────────────────────────────────────────┘

第三步：实时日内累加
  ┌─────────────────────────────────────────────────────┐
  │  Kafka Topic：日内实时成交消息                          │
  │  ↓ 按相同 key 聚合                                     │
  │  ↓ 累加到 T 日 SOD 持仓上                               │
  │  ↓ 结果：实时持仓（持续更新）                            │
  └─────────────────────────────────────────────────────┘
```

关键设计点：

- **三段式管线**：SOD 基线 → T-1 成交补充 → 日内实时累加；
- **统一聚合 key**：`账户 + 合约代码 + 多/空` 在三个阶段保持一致；
- **SOD 漂移修正**：文件或数据库中的 T-1 SOD 可能是过时快照，将 T-1 成交补充上去可在日内消息到达前修正基线；
- **实时最终态**：日内累加开始后，每笔成交到达即更新持仓。

### 2.5 下游结果输出层

计算完成的结果，包装为 `OctaneMessage`，发布至结果 Kafka Topic（`harbour.result.position`、`harbour.result.margin`）。结果 Topic 有且仅有**两个消费者**：

1. **保证金计算服务** — 消费 `harbour.result.position` 作为其持仓输入（见 [§3.8](#7-模块间数据管线)）；
2. **Query Service (CQRS)** — 消费**两个**结果 Topic 并构建物化视图。

所有下游业务系统**统一通过 Query Service 的 REST/gRPC 接口**获取持仓和保证金数据 — 下游系统不直接消费结果 Kafka Topic：

- **OSM** — 订单结算管理系统
- **Aviator** — 保证金/持仓监控 UI
- **FOX** — 持仓查询系统

这一单一入口的约束确保所有下游读取的是同一版本数据（同一 offset、同一快照），消除跨系统数据不一致。

### 2.6 查询服务层（Query Service / CQRS 读写分离）

为同步查询场景设计，解决纯流式架构无法满足低延迟点查的问题，读写链路物理隔离。详细规则见[第 5 章](#5-查询服务层cqrs-读写分离)。

## 3. 端到端完整数据流

1. 各市场生成原始业务数据（协议、结构互不相同）；
2. 对应适配器解析原始数据，完成清洗、字段推导、市场语义封装；
3. 适配器将数据包装为 `OctaneMessage` 信封（`RoutingKey` 按消息类型设置，`body` 包含 Protobuf 编码的核心字段 + 扩展字段）；
4. `OctaneMessage` 信封发布至对应 Kafka 统一总线 Topic（`position.*`、`margin.*`）；
5. **持仓计算模块** 订阅 Kafka 流，执行 SOD + Intraday 合并、子账户聚合、市场专属持仓逻辑；
6. 持仓结果包装为 `OctaneMessage` 发布至 `harbour.result.position`；
7. **保证金计算模块** 消费 `harbour.result.position` 及参考数据、行情数据，执行保证金规则，发布结果至 `harbour.result.margin`；
8. 两个结果 Topic（`harbour.result.position`、`harbour.result.margin`）均由 Query Service 消费并构建物化视图；下游系统（OSM、Aviator、FOX）通过 Query Service REST/gRPC 接口获取持仓和保证金数据 — 下游不直接消费结果 Topic。

---

## 4. 统一数据模型设计（核心 + 扩展机制）

> **传输层说明**：所有 Kafka 消息使用 `OctaneMessage` 信封（详见 `knowledge/octane-message.md`）。以下数据模型定义的是 `OctaneMessage.body` 中承载的 Protobuf schema。信封提供路由、版本和分区能力 — 与业务 schema 设计正交。

### 4.1 设计背景

多市场存在大量市场特有业务语义：

| 场景 | 特有语义 |
|----------|-------------------|
| 部分权益市场 | 今仓/昨仓区分、备兑标记 |
| 部分衍生品市场 | SPAN 参数组、组合价差持仓信息 |
| 期权（跨市场） | 冻结量、备兑冻结量 |

- 若将所有市场字段**平铺至核心模型** → 模型臃肿、语义污染、新增市场必须修改核心结构；
- 若直接**丢弃特有字段** → 计算层无法完成业务逻辑。

### 4.2 扩展机制设计原则

| 原则 | 说明 |
|-----------|-------------|
| **核心归核心** | 全市场通用字段放入核心消息，结构永久稳定，不因市场迭代变更 |
| **扩展归扩展** | 市场特有语义独立存放，不侵入核心字段 |
| **隔离消费** | 通用模块仅读取核心字段，市场专属模块按需解析扩展消息 |
| **治理闭环** | 扩展名称、类型统一注册，适配器、CI 强制校验，杜绝非法注入 |

### 4.3 技术方案：`map<string, google.protobuf.Any>` 扩展槽

采用 Protobuf3 + `google.protobuf.Any` 实现类型安全、零侵入扩展。

#### 4.3.1 核心消息定义（永久不变）

```protobuf
// unified_position.proto
syntax = "proto3";
import "google/protobuf/any.proto";
import "harbour/common/enums.proto";

message UnifiedPosition {
    // 全局核心公共字段（所有市场通用）
    string execution_id = 1;        // 全局唯一成交标识，用于去重与审计追踪
    string account_id = 2;
    string sub_account_id = 3;
    string symbol = 4;
    string mic = 5;                 // ISO 市场编码
    string venue = 6;               // 同一 MIC 下可能有多个交易场所
    PositionDirection direction = 7;
    string quantity = 8;            // 金额/数量以字符串传输，避免浮点精度丢失
    string avg_price = 9;
    int64 timestamp = 10;           // 物理时间戳（成交发生时刻）
    string business_date = 11;      // 交易日（YYYYMMDD），与 timestamp 解耦，对 SOD 合并至关重要
    PositionType type = 12;         // 品种类型：期货/期权
    string source = 13;             // 数据来源适配器标识，用于问题回溯

    // 唯一统一扩展入口
    map<string, google.protobuf.Any> extensions = 20;
}
```

#### 4.3.2 市场/业务独立扩展消息

按市场、业务维度拆分扩展文件，完全解耦：

```protobuf
// china_extensions.proto — 目标市场扩展
syntax = "proto3";
package harbour.ext;

message ChinaPositionExt {
    string today_qty = 1;       // 今仓
    string sod_qty = 2;         // 昨仓
    bool is_covered = 3;        // 备兑标记
}
```

```protobuf
// india_extensions.proto — 印度市场扩展
syntax = "proto3";
package harbour.ext;

message IndiaPositionExt {
    string span_param_group = 1;
    repeated string combined_spreads = 2;
}
```

```protobuf
// option_extensions.proto — 期权通用扩展（跨市场复用）
syntax = "proto3";
package harbour.ext;

message OptionExt {
    string frozen_qty = 1;
    string today_frozen_qty = 2;
    string sod_frozen_qty = 3;
}
```

#### 4.3.3 模块消费规则（代码示例）

**通用聚合模块**：仅使用核心字段，完全不感知扩展

```java
UnifiedPosition pos = ...;
BigDecimal totalQty = new BigDecimal(pos.getQuantity());
// 执行通用子账户合并、全局汇总逻辑
```

**市场专属模块**：按需强类型解析扩展消息

```java
// 目标市场计算模块示例
if (pos.getExtensionsMap().containsKey("china")) {
    Any any = pos.getExtensionsMap().get("china");
    ChinaPositionExt ext = any.unpack(ChinaPositionExt.class);
    // 使用今仓、昨仓、备兑标记计算保证金
}
```

#### 4.3.4 扩展键枚举安全层（编译期防护）

`map<string, Any>` 的 Key 是自由字符串，拼写错误（`"china"` vs `"chian"`）是运行时 bug。在此之上增加枚举注册表，将 Key 映射为整数枚举，适配器与计算模块通过枚举常量引用扩展，拼写错误在编译期即可发现：

```protobuf
// extension_key.proto
syntax = "proto3";
package harbour.ext;

enum ExtensionKey {
    EXT_UNKNOWN = 0;            // 保留，禁止使用
    EXT_CHINA_POSITION = 1;
    EXT_INDIA_POSITION = 2;
    EXT_OPTION = 3;
}
```

适配器写入时使用枚举名作为 map key：

```java
// 编译期安全：ExtensionKey.EXT_CHINA_POSITION.name() → "EXT_CHINA_POSITION"
pos.putExtensions(
    ExtensionKey.EXT_CHINA_POSITION.name(),
    Any.pack(chinaExt)
);
```

计算模块读取时同样使用枚举常量，避免魔法字符串：

```java
if (pos.containsExtensions(ExtensionKey.EXT_CHINA_POSITION.name())) {
    ChinaPositionExt ext = pos.getExtensionsMap()
        .get(ExtensionKey.EXT_CHINA_POSITION.name())
        .unpack(ChinaPositionExt.class);
}
```

> **注意**：枚举 Key 是 `map<string, Any>` 之上的**命名规范层**，不改变 wire format，向后兼容。枚举定义表本身即注册中心的核心数据结构。

### 4.4 方案选型对比

| 方案 | 结论 | 原因 |
|----------|:-------:|--------|
| 平铺 `optional` 字段 | ❌ 淘汰 | 模型膨胀、语义污染、新增市场必须改核心结构 |
| `oneof` 单选分支 | ❌ 淘汰 | 新增市场仍需修改核心消息，稳定性不足 |
| `map<string, Any>` | ✅ 采用 | 核心永久稳定、扩展独立、类型安全、新增市场零侵入 |

### 4.5 扩展机制治理规范

#### 扩展注册中心

维护统一注册表 (YAML)，记录扩展 Key 与 Protobuf 类型映射，所有扩展必须先注册再使用：

```yaml
# extensions_registry.yaml
# string_key: [enum_key, proto_message_type]
extensions:
  china:   [EXT_CHINA_POSITION, harbour.ext.ChinaPositionExt]
  india:   [EXT_INDIA_POSITION, harbour.ext.IndiaPositionExt]
  option:  [EXT_OPTION,        harbour.ext.OptionExt]
```

#### 治理规则

- **适配器强制校验**：禁止使用未注册的 string Key 或枚举 Key、禁止塞入非 Protobuf 裸数据；写入时必须同时使用枚举常量引用（`ExtensionKey.EXT_XXX.name()`），CI 扫描禁止裸字符串调用 `putExtensions("china", ...)`；
- **依赖声明**：每个市场模块必须文档标注依赖的扩展 Key（string + enum）、消息类型；
- **CI 自动化检查**：流水线校验扩展 Key 已注册、枚举引用有效、类型合法，非法代码禁止合并；
- **版本兼容**：扩展消息遵循 Protobuf 向前兼容规则，迭代不影响核心与其他市场；新增扩展 Key 时同步追加 `ExtensionKey` 枚举值与注册表条目。

### 4.6 Schema Registry：消息 Schema 全生命周期治理

YAML 扩展注册表（见 §4.5）仅管理扩展 Key ↔ 消息类型的映射关系，**不管理** Schema 二进制兼容性。在多团队协作下（适配器团队独立迭代扩展消息、计算模块团队独立升级消费端），字段类型变更、字段删除、字段号冲突等兼容性破坏可能在上线后才暴露。

#### 4.6.1 架构

```
适配器 ──→ 序列化时查询 Schema Registry ──→ Kafka (带 Schema ID)
计算模块 ──→ 反序列化时查询 Schema Registry ──→ 校验通过后处理
Query Service ──→ 反序列化时查询 Schema Registry ──→ 校验通过后构建视图
```

#### 4.6.2 技术要求

| 要求 | 说明 |
|-------------|-------------|
| Schema Registry 选型 | Confluent Schema Registry（原生支持 Protobuf） |
| 兼容性模式 | `FULL`（同时检查向前和向后兼容），禁止破坏性变更进入生产 |
| 注册范围 | 所有 Kafka Topic 使用的 Protobuf 消息类型：核心消息（`UnifiedPosition`）+ 每种扩展消息（`ChinaPositionExt`、`IndiaPositionExt`、`OptionExt` 等） |
| Subject 命名 | `topic-name-value`（如 `harbour.unified.position.intraday-value`） |
| CI 集成 | 流水线在构建阶段执行 `protoc` 编译 + Schema Registry 兼容性预检（dry-run validate），不兼容则阻断合并 |

#### 4.6.3 与 YAML 扩展注册表的打通

YAML 注册表与 Schema Registry 互补：

| 注册表 | 管理内容 | 检查时机 |
|----------|---------|-------------|
| **YAML 扩展注册表** | 扩展 Key → 枚举值 → 消息类型的映射关系 | CI 静态扫描 + 适配器运行时校验 |
| **Schema Registry** | Protobuf 消息二进制的字段兼容性（类型、字段号、删除/新增规则） | 构建时 CI 预检 + 生产者注册/消费者反序列化时实时校验 |

两者的 subject 和版本信息统一存储，形成平台的**统一元数据中心**。YAML 中的 `proto_message_type` 字段引用 Schema Registry 中的对应 subject。

### 4.7 深度解析：核心归核心，扩展归扩展

> 本节对上述扩展机制做进一步设计哲学拆解，回答"为什么这样设计""如何保证不把 message 塞太满""如果不这样做还有什么选择"。

#### 4.7.1 问题本质：字段的交集 vs 并集

多市场字段天然存在巨大差异，以下为四个市场 + 期权通用的字段矩阵：

```
中国: execution_id, account_id, symbol, ..., today_qty, sod_qty, is_covered
印度: execution_id, account_id, symbol, ..., span_param_group, combined_spreads
韩国: execution_id, account_id, symbol, ..., (无特殊字段)
香港: execution_id, account_id, symbol, ..., (无特殊字段)
期权: execution_id, account_id, symbol, ..., frozen_qty, today_frozen_qty, sod_frozen_qty
```

- **所有市场的交集** → 只有 `execution_id`, `account_id`, `symbol` 等约 13 个字段
- **所有市场的并集** → 30+ 字段，其中 60% 对大多数市场是 `null`
- **平铺并集方案** → 核心 message 臃肿、语义污染、新增市场必须修改核心 `.proto` 导致所有下游重新编译部署

Harbour 的方案：**核心 message 只存交集，差集塞进 `map<string, google.protobuf.Any>` 扩展槽**。

#### 4.7.2 两层结构拆解

**第一层：核心字段 — 永不变更的公共属性**

核心字段的入选标准（三条必须同时满足）：

| 标准 | 含义 |
|-----------|---------|
| **全市场共有** | 所有市场都有这个语义（即使源字段名不同，适配器做了映射） |
| **通用模块依赖** | 跨市场汇总等通用计算模块需要读取它（如 `account_id`、`symbol`、`quantity`） |
| **永久不废弃** | 是持仓/保证金业务的基本原子，不会因业务演变而被移除 |

```protobuf
message UnifiedPosition {
    string execution_id = 1;    // 全市场：唯一成交 ID（去重、审计追踪）
    string account_id = 2;      // 全市场：账户
    string sub_account_id = 3;  // 全市场：子账户
    string symbol = 4;          // 全市场：合约代码
    string mic = 5;             // 全市场：ISO 市场编码
    string venue = 6;           // 全市场：交易场所（同一 MIC 可能有多个）
    PositionDirection direction = 7;  // 全市场：买卖方向
    string quantity = 8;        // 全市场：数量（字符串避免浮点精度丢失）
    string avg_price = 9;       // 全市场：均价
    int64 timestamp = 10;       // 全市场：成交时间戳
    string business_date = 11;  // 全市场：交易日 YYYYMMDD
    PositionType type = 12;     // 全市场：期货/期权
    string source = 13;         // 全市场：来源适配器标识

    map<string, google.protobuf.Any> extensions = 20;  // ← 唯一扩展入口
}
```

> 该 message 一旦稳定，便不再修改。新增市场接入时，核心字段一个不动。

**第二层：扩展槽 — 每个市场独立维护的私有字段**

每个扩展是**独立的 `.proto` 文件**，有自己的 `package`，编译后是完全解耦的类：

```protobuf
// china_extensions.proto — 只属于目标市场
package harbour.ext;
message ChinaPositionExt {
    string today_qty = 1;       // 今仓
    string sod_qty = 2;         // 昨仓
    bool is_covered = 3;        // 备兑标记
}

// india_extensions.proto — 只属于印度市场
package harbour.ext;
message IndiaPositionExt {
    string span_param_group = 1;
    repeated string combined_spreads = 2;
}

// option_extensions.proto — 跨市场复用的期权扩展
package harbour.ext;
message OptionExt {
    string frozen_qty = 1;
    string today_frozen_qty = 2;
    string sod_frozen_qty = 3;
}
```

关键设计点：

- 每个扩展文件独立编译，新增韩国特有字段只需新建 `KoreaPositionExt`，不动 `ChinaPositionExt`，更不动 `UnifiedPosition`
- `OptionExt` 是"跨市场复用"的扩展（中国期权、印度期权都需要），但不放入核心字段 — 因为期货市场不需要

#### 4.7.3 运行时消费：隔离原则（核心设计精髓）

不同模块**读不同的层**，这是保持扩展性同时不污染核心的关键：

```java
// ─── 通用跨市场汇总模块 ───
// 只读核心字段，完全不感知扩展，不 import 任何扩展类
UnifiedPosition pos = consumer.next();
BigDecimal qty = new BigDecimal(pos.getQuantity());
aggregator.add(pos.getAccountId(), pos.getSymbol(), qty);
// ↑ 这段代码对 4 个市场完全相同，一行不用改

// ─── 目标市场专属模块 ───
// 读到核心字段 + 按需解析中国扩展
if (pos.containsExtensions(ExtensionKey.EXT_CHINA_POSITION.name())) {
    ChinaPositionExt ext = pos.getExtensionsMap()
        .get(ExtensionKey.EXT_CHINA_POSITION.name())
        .unpack(ChinaPositionExt.class);
    BigDecimal todayQty = new BigDecimal(ext.getTodayQty());
    BigDecimal sodQty = new BigDecimal(ext.getSodQty());
    // 基于今仓/昨仓差异计算阶梯保证金...
}
```

```
消费隔离示意：

  通用模块                      目标市场模块
  ┌──────────────────┐       ┌──────────────────────────┐
  │ 只读核心字段       │       │ 读核心字段                 │
  │ 不 import 任何扩展 │       │ + 解析 ChinaPositionExt   │
  │ 永远不因新市场改动  │       │ + 执行阶梯保证金逻辑       │
  └──────────────────┘       └──────────────────────────┘
```

#### 4.7.4 四层治理防线：防止扩展槽变成垃圾场

`map<string, Any>` 本身只是自由键值对，如果没有治理，最终会变成充斥拼写错误、未注册类型、废弃字段的垃圾场。Harbour 设计了四层递进防线：

| 防线 | 层级 | 机制 | 拦截内容 |
|-------|------|------|---------|
| **① 编译期** | 代码 | `ExtensionKey` 枚举，禁止魔法字符串 | 拼写错误（`"china"` vs `"chian"`）、未定义 Key |
| **② CI 静态扫描** | 流水线 | 扫描 `putExtensions` 调用，对照 YAML 注册表 | 未注册 Key、类型不匹配、裸字符串调用 |
| **③ 构建时** | CI | Schema Registry `FULL` 兼容性预检 | 字段类型变更、字段号冲突、破坏性删除 |
| **④ 运行时** | 适配器 | 启动时加载注册表到内存，写入前检查 | 绕过 CI 的未注册 Key（最后一道防线） |

**防线 ①：ExtensionKey 枚举（编译期安全）**

```java
// ❌ CI 会拦截 — 裸字符串，拼写错误编译期不报错
pos.putExtensions("china", any);

// ✅ 唯一合法写法 — 枚举常量，拼写错误编译期暴露
pos.putExtensions(ExtensionKey.EXT_CHINA_POSITION.name(), any);
```

**防线 ②：YAML 注册表 + CI 静态扫描**

```yaml
# extensions_registry.yaml — 扩展的唯一真实来源
extensions:
  china:   [EXT_CHINA_POSITION, harbour.ext.ChinaPositionExt]
  india:   [EXT_INDIA_POSITION, harbour.ext.IndiaPositionExt]
  option:  [EXT_OPTION,        harbour.ext.OptionExt]
```

CI 扫描规则：
- 所有 `putExtensions(key, any)` 调用的 key 必须在注册表中
- `any.pack()` / `any.unpack()` 的泛型类型必须与注册表声明一致
- 禁止使用 `putExtensions` 的字符串重载（强制走枚举路径）

**防线 ③：Schema Registry FULL 兼容性**

不同于 YAML 注册表管理"Key → 类型"映射，Schema Registry 管理"Protobuf 二进制兼容性"：

- `FULL` 模式：同时检查向前（新生产者 → 旧消费者）和向后（旧生产者 → 新消费者）兼容
- 只允许：新增 optional 字段、删除 optional 字段（谨慎）、新增枚举值
- 禁止：修改字段类型、修改字段号、删除 required 字段
- CI 预检：`protoc` 编译后 dry-run 验证，不兼容则阻断合并

**防线 ④：适配器运行时启动检查**

```java
// 适配器启动时加载注册表
ExtensionRegistry registry = ExtensionRegistry.loadFromYaml("extensions_registry.yaml");

// 每次写入扩展前检查
public void putExtension(UnifiedPosition.Builder builder, ExtensionKey key, Any value) {
    if (!registry.isRegistered(key)) {
        throw new IllegalExtensionException("未注册的扩展 Key: " + key);
    }
    if (!registry.matchesType(key, value)) {
        throw new IllegalExtensionException("扩展类型不匹配: expected " 
            + registry.getType(key) + ", got " + value.getTypeUrl());
    }
    builder.putExtensions(key.name(), value);
}
```

#### 4.7.5 方案选型完整对比

| 方案 | 结论 | 核心缺陷 |
|----------|:-------:|-------------|
| **平铺 `optional` 字段** | ❌ | 核心 message 膨胀到 30+ 字段；新增市场必须修改核心 `.proto`，所有下游重新编译部署；字段语义归属模糊 |
| **`oneof` 分支** | ❌ | 每条消息只能有一个市场的数据，跨市场汇总模块无法同时处理多市场；新增市场仍需修改核心 message |
| **每个市场独立 message** | ❌ | 失去统一性，Kafka Topic 里存多种类型，通用模块无法消费，需要类型判断分支 |
| **JSON 自由字段** | ❌ | 丢掉类型安全，字段拼写错误运行时才暴露；序列化体积大、性能差；无 Schema Registry 兼容性校验 |
| **`map<string, Any>` + 治理** | ✅ | 核心永久 13 个字段、扩展独立文件、类型安全、新增市场零侵入核心、四层治理防退化 |

#### 4.7.6 设计决策总结

```
                     ┌──────────────┐
                     │   新市场接入   │
                     └──────┬───────┘
                            │
              ┌─────────────┼─────────────┐
              │             │             │
              ▼             ▼             ▼
       核心字段是否    扩展字段是否    保证金规则
       需要新增？      需要新增？      是否不同？
              │             │             │
              ▼             ▼             ▼
         ❌ 不需要      ✅ 新建扩展     ✅ 新建插件
        (交集已覆盖)   .proto 文件    注册到 ProcessorReg
                       + 注册表一行
```

**核心优势**：

1. **核心 message 永远 13 个字段** — 不管接多少个市场，`UnifiedPosition` 都不变
2. **新增市场零侵入核心** — 只加一个扩展 `.proto` 文件 + 注册表一行 YAML
3. **消费端按需解析** — 通用模块不碰扩展，专属模块只解析自己关心的
4. **拼写错误编译期暴露** — 枚举 Key 替代魔法字符串
5. **CI 堵死非法注入** — 注册表 + 静态扫描 + Schema Registry，不注册就合不进去
6. **扩展独立演进** — 目标市场扩展升级字段不影响印度、韩国、香港的消费者

> 这一设计模式源自 Protobuf 生态的标准实践 — gRPC 的 `grpc-gateway` 转码、Google Cloud API 的 `google.api.HttpBody`、Istio 的 `EnvoyFilter` 均使用相同的 `map<string, Any>` 扩展机制处理多版本/多场景差异化数据。

---

## 5. 查询服务层（CQRS 读写分离）

### 5.1 建设背景

纯流式架构擅长异步事件处理，无法满足 UI、查询系统、实时风控低延迟同步点查需求；若由各下游自行消费流并构建视图，会造成数据不一致、重复开发。

### 5.2 架构链路

```
计算模块 → Kafka 结果 Topic → Query Service (物化视图) → REST/gRPC 接口
```

### 5.3 核心能力

- 消费结果流，在**本地 RocksDB** 中构建以 `account_id` 为索引的账户快照：

| 存储 | 技术选型 | 用途 | 数据量级 |
|---------|-----------|---------|:-----------:|
| 本地持久化存储 | RocksDB | 全量账户快照；所有读写直接操作 RocksDB | 无上限 |

- **启动 hydration**：每次启动时，每个实例重放结果 Topic 并从头重建 RocksDB 状态，hydration 完成后才能对外提供查询流量。为确保全量重建可行，`harbour.result.position` / `harbour.result.margin` 必须使用 `cleanup.policy=compact`，以 `account_id` 为消息 key（压缩保留每个账户的最新快照，因此重放得到的就是当前状态）。就绪探针 (`/health/ready`) 仅在 hydration 完成后返回 200；hydration 时长受 compacted Topic 大小限制，作为启动指标监控。
- 所有写入同步到 RocksDB；查询直接读取 RocksDB（按 `account_id` 点查）。由于状态始终可从 Kafka 重建，实例是可抛弃且可互换的 — 扩缩或故障转移只需启动新实例重新 hydration；
- 提供标准查询接口：`GetPosition`、`GetMargin`；
- 响应头携带 `X-Data-Last-Update-Timestamp` 以及消息对应的 Kafka `offset` 和 `transaction_id`，标识数据版本，便于问题回溯；
- **Kafka Consumer 隔离级别**：必须配置 `isolation.level=read_committed`，确保只消费计算模块事务已提交的消息，避免读到未提交事务的中间态数据造成快照脏读；
- 数据保证**最终一致性**，强一致性场景对接结算库专用接口。

### 5.4 价值

- 统一查询入口、消除下游重复开发；
- 读写链路解耦、不影响核心流处理逻辑。

---

## 6. SOD 与日内增量数据合并时序

日初快照 (SOD) 来自 SOD Adaptor，日内增量 (Intraday) 来自实时适配器，两者到达时序无保证，计算模块必须处理以下场景。

### 6.1 关键时序场景

| 场景 | 说明 | 处理策略 |
|----------|-------------|-------------------|
| SOD 先到，Intraday 后到 | 正常路径 | SOD 建立基线 → Intraday 增量叠加 |
| Intraday 先到，SOD 后到 | 开盘后快照延迟就绪 | 缓存 Intraday → SOD 到达后回放合并，合并完毕前标记该账户为 `initializing` |
| SOD 与 Intraday 同时到达 | 并发写入 | 基于 `business_date` 分区锁，同一交易日同一账户串行合并 |
| SOD 数据修正重发 | 发布修正版日初快照 | SOD 消息携带 `sod_version`，版本号递增；收到更高版本时回滚该交易日的增量合并，以新版 SOD 重建基线 |

### 6.2 去重策略

同一笔成交可能同时出现在 SOD 和 Intraday 中（例如 快照时间窗口与实时流重叠）。计算模块使用 **`execution_id`**（见 [§5.3.1](#431-核心消息定义永久不变)）进行去重：

- SOD 加载时记录所有 `execution_id` 到当日去重集合；
- Intraday 消息到达时先查去重集合，命中则跳过；
- 去重集合在下一个交易日 SOD 到达时清空重建。

### 6.3 状态标记

每个账户在每个交易日有三种合并状态，通过查询接口暴露：

| 状态 | 含义 | 查询行为 |
|-------|---------|---------------|
| `initializing` | SOD 未就绪，仅累积了 Intraday 增量 | 返回临时结果 + 状态标记，调用方可选择等待 |
| `ready` | SOD 已就绪，增量合并正常 | 返回最终结果 |
| `stale` | 超过阈值时间未收到任何数据 | 返回最后已知结果 + 告警标记 |

---

## 7. 模块间数据管线（Position → Margin）

计算模块不是孤立的 silo — 一个模块的输出可以通过 Kafka 作为另一个模块的输入，形成处理 DAG。核心场景是 **Position → Margin**：保证金计算依赖持仓数据。

```
  适配器 ──→ position.sod / position.intraday
                    │
                    ▼
  ┌──────────────────────────────────────┐
  │        持仓计算模块                    │
  │  SOD + Intraday 合并                 │
  │  → 每个账户的实时持仓                 │
  └──────────────────┬───────────────────┘
                     │
                     │ harbour.result.position (OctaneMessage)
                     │ RoutingKey = POSITION
                     ├──────────────────────────────┐
                     ▼                              ▼
  ┌──────────────────────────────────────┐   Query Service (CQRS)
  │        保证金计算模块                  │        │
  │  消费：                               │        │ REST / gRPC
  │    • 持仓结果（来自上游）              │        ▼
  │    • 参考数据（保证金费率）            │   OSM / Aviator / FOX
  │    • 行情数据（价格）                  │
  │  → 每个账户的保证金                    │
  └──────────────────┬───────────────────┘
                     │
                     │ harbour.result.margin (OctaneMessage)
                     ▼
               Query Service (CQRS) ──→ OSM / Aviator / FOX
```

### 7.1 为什么不用单一模块？

| 方案 | 问题 |
|----------|-------|
| **单体**同时做持仓和保证金 | 持仓变动触发全量保证金重算；耦合导致无法独立扩缩、测试、部署 |
| **独立模块，Kafka 串联** | 持仓模块独立扩缩；持仓结果在 Kafka 上持久化 — 保证金可从任意 offset 重放；保证金 bug 不会破坏持仓数据 |

### 7.2 数据流细节

```
持仓计算发布：
  Topic:   harbour.result.position
  信封:    OctaneMessage {
      key  = RoutingKey.POSITION,
      body = PositionResult Proto {
          account_id, symbol, direction,
          sod_quantity, intraday_delta, current_position,
          business_date, status, extensions
      }
  }

保证金计算消费：
  订阅:    harbour.result.position
  每条 key = POSITION 的 OctaneMessage：
      1. 反序列化 body → PositionResult
      2. 从参考数据缓存查询保证金费率
      3. 应用保证金规则 → 计算保证金要求
      4. 发布至 harbour.result.margin
```

### 7.3 顺序保证

同一 `account_id` 的持仓结果按序到达（单分区 per account，通过 `kafkaPartitionKey`）。保证金计算按序处理，因此保证金始终基于最新持仓 — 不会读到过期数据。

### 7.4 故障隔离

如果保证金计算故障：
- 持仓计算不受影响 — 持仓持续在 `harbour.result.position` 上更新
- 恢复后，保证金计算从最后 committed offset 重放
- 数据不丢；保证金追上当前持仓状态

如果持仓计算故障：
- 保证金计算看不到新持仓消息 → 最后已知保证金保持
- 超过 stale 阈值的账户被标记
- 持仓计算恢复后，保证金自动恢复

### 7.5 通用模式

Position → Margin 是通用模式的一个实例：**任何计算模块都可以消费任何结果 Topic**。其他潜在管线：

```
Position → Margin       (已实现)
Margin → Risk           (保证金驱动风险限额检查)
Position → P&L          (持仓 + 行情 → 盈亏)
```

约束始终相同：从上游结果 Topic 消费 `OctaneMessage`，按 `RoutingKey` 过滤，用预期 schema 反序列化 `body`，计算，发布至新的结果 Topic。

## 8. 新市场接入与代码复用规则

平台最大化复用通用能力，区分可复用部分与必须新增部分，避免"零修改"误导。

### 8.1 复用与新增明细

| 能力分类 | 是否可复用 | 说明 |
|--------------------|:---------:|-------------|
| 公共计算框架 | ✅ 完全复用 | 子账户聚合、SOD 初始化、日内增量合并、通用风控框架 |
| 跨市场全局汇总模块 | ✅ 完全复用 | 全市场持仓/保证金聚合逻辑，无需改动 |
| 适配器标准化流程 | ✅ 复用模式 | 统一流程：解析 → 清洗 → 扩展封装 → 包装为 OctaneMessage |
| Kafka 总线 & 下游接口 | ✅ 完全复用 | Topic 结构、数据格式全局统一，下游无需适配 |
| 市场专属保证金规则 | ❌ 必须新增 | 各市场规则不同（SPAN/阶梯/KRX），需独立开发插件/模块 |
| 市场特有持仓语义处理 | ❌ 部分新增 | 解析对应扩展消息，实现市场特有逻辑 |

### 8.2 新市场接入工作量总结

新增一个市场，仅需完成 **3 项工作**：

1. 开发对应市场适配器；
2. 实现该市场专属保证金计算插件/模块；
3. 增加扩展消息解析逻辑（如有市场特有语义）。

> 对比传统架构（每个市场独立开发全套适配+计算+接口），新市场接入仅需完成适配器 + 保证金插件 + 扩展解析，重复开发模块量（公共计算框架、Kafka 总线、下游接口、全局汇总）全部复用，开发工作量大幅降低（具体比例取决于新市场的保证金规则复杂度，此处为定性估计）。

