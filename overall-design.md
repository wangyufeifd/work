# Harbour Unified Position & Margin Middle Platform — Complete Architecture Design

| Attribute | Value |
|-----------|-------|
| **Doc Version** | V1.6 |
| **Audience** | Development, Testing, Operations, Architecture Review, Cross-team Integration |
| **Contents** | Overall Architecture, Layered Design, Data Flow, Data Model (Core + Extension Mechanism + Schema Registry), Module Reuse Rules, Transitional Dual-Model Governance, Stream Processing Semantics (incl. Changelog + DLQ), Adaptor Boundaries, Query Service (incl. read_committed), Security Design, Deployment Model, Observability (incl. OpenTelemetry), Testing Strategy, Trading Day Handling, Glossary |

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Top-Level Mandatory Design Principles](#2-top-level-mandatory-design-principles)
3. [Overall Layered Architecture & Responsibilities](#3-overall-layered-architecture--responsibilities)
   - 3.3.3 [Deep Dive: SOD & Intraday Separate Topic Design](#333-deep-dive-sod--intraday-separate-topic-design)
4. [End-to-End Complete Data Flow](#4-end-to-end-complete-data-flow)
5. [Unified Data Model Design (Core + Extension Mechanism)](#5-unified-data-model-design-core--extension-mechanism)
   - 5.7 [Deep Dive: Core Belongs to Core, Extensions Belong to Extensions](#57-deep-dive-core-belongs-to-core-extensions-belong-to-extensions)
6. [New Market Onboarding & Code Reuse Rules](#6-new-market-onboarding--code-reuse-rules)
7. [Transitional Dual-Model Governance](#7-transitional-dual-model-governance)
8. [Stream Processing Semantics & Idempotency Specification](#8-stream-processing-semantics--idempotency-specification) (incl. Changelog State Recovery · DLQ Dead Letter Queue)
9. [Adaptor Layer Boundaries & Operation Specification](#9-adaptor-layer-boundaries--operation-specification)
10. [Query Service Layer (CQRS Read/Write Separation)](#10-query-service-layer-cqrs-readwrite-separation)
11. [Architecture Constraints & Operations Specification](#11-architecture-constraints--operations-specification)
12. [Platform Capability Summary](#12-platform-capability-summary)
13. [Security Design](#13-security-design)
14. [Deployment & Scaling Model](#14-deployment--scaling-model)
15. [Observability & Monitoring](#15-observability--monitoring)
16. [Testing Strategy](#16-testing-strategy)
17. [Market-Specific Special Trading Day Handling](#17-market-specific-special-trading-day-handling)
Appendix A. [Glossary](#appendix-a-glossary)

---

## 1. Project Overview

### 1.1 Positioning

Harbour is a **unified position & margin calculation middle platform** serving multiple markets and exchanges. The platform uniformly connects to heterogeneous data sources across regions, standardizes data formats, calculation logic, and interface specifications, providing consistent and reliable position and margin data to downstream trading, settlement, risk control, and monitoring systems.

### 1.2 Core Problems Solved

- Exchange/market data source protocols, fields, and business semantics vary greatly, leading to extremely high repeated adaptation costs for cross-market integration;
- Traditional architectures develop full code for each market independently, causing massive duplication and high maintenance costs;
- Downstream systems need to integrate with multiple heterogeneous interfaces, imposing heavy migration, integration, and operational burdens;
- The existing architecture cannot support unified GMI + multi-level sub-account management and cross-market unified risk calculation.

### 1.3 Overall Architecture Philosophy

> **Unified Data Model + Isolated Adaptor Layer + Reusable Calculation Modules + Kafka Stream-Driven Architecture + CQRS Query Service + Standardized Extension Mechanism**

---

## 2. Top-Level Mandatory Design Principles

All platform components, code, and processes must comply with the following rules:

1. All upstream heterogeneous data must be uniformly converted by adaptors into a single standard Protobuf (Octane) model; all calculation modules consume only this unified model;
2. Strictly divide responsibilities by layer — data ingestion, data standardization, message transport, business calculation, and data query are fully decoupled;
3. Platform-internal streaming data uses only `OctaneMessage` as the sole Kafka message format — a unified envelope wrapping Protobuf-encoded business payloads (see `knowledge/octane-message.md`);
4. All market-specific differentiation logic and private semantics are uniformly shielded at the adaptor layer; generic calculation modules must not contain market hard-coding;
5. During the transition period, two data models may coexist, but synchronization, validation, and decommissioning rules must be established to eliminate technical debt and semantic drift;
6. The core data model remains long-term stable and unchanging; market-specific business semantics are implemented through the standardized extension mechanism without polluting core fields.

---

## 3. Overall Layered Architecture & Responsibilities

The platform is divided top-to-bottom into **6 logically independent layers**, each with a single responsibility and clear boundaries, supporting independent development, testing, deployment, and scaling.

```
                         ┌──────────────────────────────────────┐
                         │       Upstream Heterogeneous Data     │
                         │   China · India · Korea · HK · GMI    │
                         └────────────────┬─────────────────────┘
                                          ▼
                         ┌──────────────────────────────────────┐
                         │     Adaptor Standardization Layer     │
                         │  Parse → Clean → Derive → Wrap →     │
                         │  OctaneMessage { key, body, ... }    │
                         └────────────────┬─────────────────────┘
                                          ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │                  Kafka Unified Message Bus Layer                     │
  │                                                                     │
  │  All Topics carry OctaneMessage (RoutingKey + Protobuf body)        │
  │  position.sod · position.intraday · margin.sod · margin.intraday    │
  │  Capabilities: Replay · Backfill · Checkpoint Resume · Partitioned  │
  └────────────────────────────────┬────────────────────────────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    ▼                             ▼
  ┌─────────────────────────────────┐  ┌─────────────────────────────────┐
  │  Position Calculation Service    │  │  Margin Calculation Service      │
  │  (Independent Service)           │  │  (Independent Service)           │
  │                                  │  │                                  │
  │  • SOD + Intraday merge          │  │  • Margin rules engine           │
  │  • Market-specific extensions    │  │  • Reference data cache          │
  │  • Sub-account aggregation       │  │    (rates, haircuts, parameters) │
  │  • Global cross-market summary   │  │  • Manual override support       │
  │  • execution_id dedup            │  │  • Multi-source aggregation      │
  │  • State: INITIALIZING/READY/    │  │                                  │
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
  │  Query Service    │                                 │  OSM / Aviator   │
  │  (CQRS)           │                                 │  FOX / Risk      │
  │                   │                                 │                  │
  │  Caffeine · Redis │                                 │  Direct Kafka     │
  │  RocksDB · Views  │                                 │  consumption      │
  │  REST / gRPC      │                                 │  (streaming)      │
  └──────────────────┘                                 └──────────────────┘
```

### 3.1 Upstream Heterogeneous Data Source Layer

Aggregates all business market raw data; different markets use different protocols, structures, and semantics:

| Market | Data Source | Protocol |
|--------|------------|----------|
| China | OSM real-time execution Kafka topic + GMI DB SOD snapshots (FTP) | Kafka / FTP |
| India | Precision HTTP interface data | HTTP |
| Korea | Koscom exchange HTTP data | HTTP |
| Hong Kong | HKEX exchange private HTTP data stream | HTTP |
| GMI DB | Daily opening position & margin static snapshot data | DB |

### 3.2 Adaptor Standardization Layer

Each market/data source corresponds to an independent adaptor service responsible for raw data parsing, cleaning, standardization, and format conversion.

#### 3.2.1 Existing Adaptor Inventory

- **Harbour China Adaptor** — China market adaptor
- **Harbour India Adaptor** — India market adaptor
- **Harbour Korea Adaptor** — Korea market adaptor
- **Harbour HKEX Adaptor** — HKEX adaptor
- **GMI DB Adaptor** — Dedicated to SOD snapshot initialization

#### 3.2.2 Core Responsibilities

Parse upstream raw data, perform cleaning, field mapping, and semantic packing, wrap as `OctaneMessage` envelopes, and publish to the Kafka message bus.

#### 3.2.3 Allowed Operations (Whitelist)

> Only data preprocessing, format conversion, and semantic packing are allowed; any business calculation is prohibited.

- Field mapping, data type conversion, timestamp uniform formatting;
- Filter dirty data based on data integrity rules (null values, illegal enums, out-of-range data);
- Derive basic fields required by the unified model from raw trade flows (e.g., calculate position delta from trade direction + quantity);
- Pack market-private business semantics into standard extension messages (`extensions`).

#### 3.2.4 Strictly Prohibited Operations

- Business rule calculations such as margin, P&L, and risk metrics;
- Position aggregation, hedge netting, or position consolidation across multiple records or accounts;
- Sub-account hierarchy relationship calculation and derivation.

> **Enforcement**: Dual protection via Code Review + automated checks to prevent responsibility boundary violations.

### 3.3 Kafka Unified Message Bus Layer

The platform's core data hub, bearing data transport, buffering, replay, backfill, and fault recovery capabilities. All links transmit only `OctaneMessage` — the unified envelope wrapping Protobuf-encoded business payloads. Every producer and consumer reads and writes this single message type; the `RoutingKey` field in the envelope determines which schema to use when deserializing `body`.

#### 3.3.1 Standard Topic Partitioning (by data type + time dimension)

| Topic | Description |
|-------|-------------|
| `harbour.unified.position.sod` | SOD snapshot position data |
| `harbour.unified.position.intraday` | Intraday incremental real-time position data |
| `harbour.unified.margin.sod` | SOD snapshot margin data |
| `harbour.unified.margin.intraday` | Intraday incremental real-time margin data |

#### 3.3.2 Basic Capabilities

Supports data replay, historical backfill, and fault checkpoint resume; all Topic data structures are globally unified and **market-agnostic**.

#### 3.3.3 Deep Dive: SOD & Intraday Separate Topic Design

> Why must SOD and Intraday be two separate Topics? This section demonstrates the necessity from 6 dimensions.

**Fundamental Differences Between the Two Data Types**:

| Attribute | SOD (Start of Day) | Intraday |
|-----------|-------------------|----------|
| **Source** | GMI DB snapshot | Real-time adaptors (OSM execution stream, etc.) |
| **Frequency** | Once per day (pre-market open) | Continuously during trading hours |
| **Volume** | Full snapshot (all accounts at once) | Single trade granularity |
| **Semantics** | Complete position baseline | Incremental changes |
| **Latency Req.** | Minutes tolerable | Milliseconds |
| **Reliability** | Can wait for corrected re-send if wrong | Lost = permanently gone |
| **Recovery** | Replay today's SOD | Requires SOD + replay all Intraday for the day |

---

**Dimension 1: Completely Different Consumption Semantics**

SOD "establishes baseline"; Intraday "accumulates increments". If mixed in one Topic, consumers must type-check every message:

```
Mixed Topic (❌ anti-pattern):
  [SOD  account=A01 qty=100]  ← baseline
  [INT  account=A01 delta=+2] ← increment
  [INT  account=A02 delta=-5]
  [SOD  account=A02 qty=50]   ← A02 baseline arrives after increments? Out of order!
  [INT  account=A01 delta=-1]
  [SOD  account=A01 qty=120]  ← Corrected SOD or duplicate?
```

Consumers are forced to maintain a complex state machine: is this SOD or Intraday? Has this account's SOD arrived yet? Is this a correction? All logic tangled together.

**After separation**: Consumers subscribe to two Topics — SOD Consumer only cares about baseline establishment, Intraday Consumer only cares about incremental accumulation. Clear responsibilities.

---

**Dimension 2: Throughput Characteristics Conflict**

```
                        Pre-open 5 min          Trading Hours (4h)
                        ──────────────          ──────────────────
SOD Topic               Full burst              Nearly zero traffic
                        (thousands of accounts × tens of positions)
Intraday Topic          No traffic              Continuous steady flow
                        (market not open yet)   (hundreds per second)
```

If mixed, Kafka partition design faces a dilemma: partition count designed for SOD peak wastes resources; designed for Intraday steady state causes backlog at open. Separation enables independent tuning.

---

**Dimension 3: Different Retention Policies**

| Policy | SOD | Intraday |
|--------|-----|----------|
| **Retention** | 30 days (compliance audit requires SOD snapshots) | 7 days (reconstructible from SOD + subsequent Intraday) |
| **Compaction** | `cleanup.policy=compact` (same key, keep latest) | `cleanup.policy=delete` (every increment is meaningful, no compaction) |
| **Deletion** | Batch delete by day | Rolling time window |

Kafka Topic `retention.ms` and `cleanup.policy` are topic-level; mixing forces the intersection → either SOD retention is insufficient or Intraday wastes storage.

---

**Dimension 4: Completely Different Fault Recovery Paths**

```
Scenario: Calculation module down for 30 minutes, needs recovery

Separated:
  ① SOD Consumer: Restore baseline state from Changelog (seconds)
  ② Intraday Consumer: Resume from last committed offset, consume 30 min of data (minutes)
  ③ Merge: baseline + increments → normal state
  Two independent pipelines, no interference

Mixed:
  Must replay from a single Topic offset
  → Cannot skip SOD messages
  → Cannot distinguish increments from baselines
  → Recovery logic extremely complex
```

This integrates with §8.3 Changelog state recovery: separated Topics enable independent Changelogs.

---

**Dimension 5: Different Partition Key & Count Strategies**

| Attribute | SOD | Intraday |
|-----------|-----|----------|
| **Partition Key** | `account_id` | `account_id` |
| **Partition Count** | 8 (small data volume, one-shot) | 32 (designed for trading-hour throughput) |
| **Message Key** | `account_id:business_date` | `execution_id` |

Both use `account_id` partitioning for co-partition (same account's SOD and Intraday land on the same consumer instance), but different partition counts are justified — SOD's small volume uses fewer partitions to reduce overhead, Intraday needs more partitions for concurrent consumption. Separation enables independent partition count adjustment.

---

**Dimension 6: Different Message Versioning & Dedup Semantics**

| Semantic | SOD | Intraday |
|----------|-----|----------|
| **Versioning** | `sod_version` increments (GMI correction re-send) | No version concept |
| **Duplicate Handling** | New SOD for same `account_id + business_date` **overwrites** old | `execution_id` globally unique, duplicates **skipped** |
| **Consumer Behavior** | Receiving higher-version SOD → rollback day's increments → rebuild baseline with new SOD | Query dedup set → discard if hit |

Two completely different dedup/versioning semantics; mixed Topic forces consumers to maintain both logic sets simultaneously.

---

**Topic Planning Summary**:

```
harbour.unified.position.sod
  ├── Partition key: account_id
  ├── Partitions: 8 (tunable)
  ├── Retention: 30 days
  ├── Compaction: cleanup.policy=compact (keep latest per key)
  ├── Semantics: SOD baseline snapshot
  └── Versioning: sod_version increments

harbour.unified.position.intraday
  ├── Partition key: account_id
  ├── Partitions: 32 (tunable, per throughput)
  ├── Retention: 7 days
  ├── Compaction: cleanup.policy=delete (no compaction, every message meaningful)
  ├── Semantics: Intraday incremental changes
  └── Dedup: execution_id

harbour.unified.margin.sod        ← same as above (position → margin)
harbour.unified.margin.intraday   ← same as above (position → margin)
```

---

**The only scenario that does NOT need separation**: if a market has only Intraday and no SOD (no SOD snapshots provided, relying entirely on real-time stream accumulation). Its Intraday Topic design is unchanged; there is simply no corresponding SOD Topic. There is no scenario requiring "merging into one Topic".

**Conclusion**: SOD and Intraday **must** be two separate Topics. This is a design decision based on data semantics as the boundary, not arbitrary.

| Dimension | Separated | Combined |
|-----------|:---------:|:--------:|
| Consumption semantic clarity | ✅ Two Consumers, each with its own role | ❌ Single Consumer forced to type-check |
| Throughput tuning | ✅ SOD/Intraday independently scalable partitions | ❌ Cannot satisfy both peak and steady state |
| Retention policy | ✅ SOD 30d compact / Intraday 7d no-compact | ❌ Forced to take intersection |
| Fault recovery | ✅ Two independent recovery pipelines | ❌ Recovery logic extremely complex |
| Partition strategy | ✅ Independent partition count & key semantics | ❌ Mutually constrained |
| Versioning/dedup semantics | ✅ Independently managed | ❌ Two logic sets intertwined |
| Operational complexity | ✅ Separated concerns, fast problem localization | ❌ One Topic carrying two semantics |

### 3.4 Calculation Module Layer (Core Business Layer)

> For detailed design of calculation module class architecture, state machine implementation, Kafka Streams topology, and fault recovery, see standalone documents:
> - Position calculation module: **`calculation-module-architecture.md`**
> - Margin calculation module (multi-source aggregation + manual override + reference data cache): **`margin-module-architecture.md`**

All calculation modules subscribe only to Kafka unified streams, fully decoupled from upstream data sources. After completing business calculations, results are wrapped as `OctaneMessage` and output to result-type Kafka Topics.

**Position Calculation and Margin Calculation are two separate, independent microservices** — separate processes, separate deployments, independently scaled. They never communicate in-process or via direct RPC: Position Calculation publishes results to `harbour.result.position`, and Margin Calculation consumes them from that Kafka Topic (see [§3.8](#38-inter-module-data-pipeline)).

#### 3.4.1 Module Classification

| Type | Module | Deployment Mode |
|------|--------|----------------|
| Market-specific calculation | China position/margin, India, Korea, HKEX modules | Independently deployed per market, independently scaled by market throughput |
| Global cross-market aggregation | All-market position & margin unified aggregation module | Consumer Group multi-instance, horizontally scaled by `account_id` partition |

#### 3.4.2 Global Aggregation Sharding Strategy

Result Topics output by market-specific calculation modules uniformly use `account_id` as Kafka partition key, ensuring cross-market result messages for the same account all land in the same partition. The global aggregation module consumes in Consumer Group multi-instance mode, with each instance responsible for several partitions, performing account-granularity cross-market aggregation within the instance.

```
Market Module A ──→ Result Topic (partition=hash(account_id))
Market Module B ──→ Result Topic (partition=hash(account_id))
                              │
            ┌─────────────────┼─────────────────┐
            ▼                 ▼                 ▼
    Agg Instance 1      Agg Instance 2      Agg Instance 3
    (partitions 0-3)    (partitions 4-7)    (partitions 8-11)
```

- **Per-account ordering**: Same `account_id` always handled by same partition, same instance, ensuring strict aggregation ordering;
- **Horizontal scaling**: Increase partition count on new instances + Rebalance, linear throughput growth;
- **Fault isolation**: Single instance failure only affects its assigned partition accounts; other partitions unaffected;
- **Cross-account global metrics** (e.g., all-market total position): performed by Query Service materialized views via cross-account `SUM` aggregation, or add a lightweight global aggregation layer (consuming per-partition aggregation results for secondary rollup) — not implemented in the stream processing layer.

Global result Topics are also partitioned by `account_id`; downstream Query Service consumes with the same partition key, writing to local RocksDB materialized views.

#### 3.4.3 Core Calculation Capabilities

- GMI + multi-level sub-account position & margin aggregation;
- SOD snapshot + Intraday incremental data merge calculation;
- Futures, options full-category position calculation, option frozen position, margin benefit logic;
- Market-specific margin rule calculation (China tiered margin, India SPAN, Korea KRX rules, etc.);
- Cross-market unified risk metric statistics.

#### 3.4.4 Example: China Realtime Position Calculation Flow

Below is the concrete implementation of China market realtime position calculation, illustrating how the abstract layered architecture maps to an actual pipeline:

```
Step ①: Load T-1 SOD baseline from GMI
  ┌─────────────────────────────────────────────────────┐
  │  GMI T-1 SOD position FTP file                      │
  │  ↓ Filter: keep only China-market records           │
  │  ↓ Aggregate by key: GMI account + instrument       │
  │    symbol + long/short direction                     │
  │  ↓ Result: T-1 SOD position baseline per key         │
  └─────────────────────────────────────────────────────┘

Step ②: Hydrate T-1 executions onto the baseline
  ┌─────────────────────────────────────────────────────┐
  │  Kafka Topic: T-1 all execution messages             │
  │  ↓ Aggregate by the same key                         │
  │    (GMI account + instrument symbol + long/short)    │
  │  ↓ Merge onto GMI T-1 SOD baseline                   │
  │  ↓ Result: T day SOD position data                   │
  └─────────────────────────────────────────────────────┘

Step ③: Realtime intraday accumulation
  ┌─────────────────────────────────────────────────────┐
  │  Kafka Topic: intraday realtime execution messages   │
  │  ↓ Aggregate by the same key                         │
  │  ↓ Accumulate onto T day SOD position                │
  │  ↓ Result: realtime position (continuously updated)  │
  └─────────────────────────────────────────────────────┘
```

Key design points:

- **Three-stage pipeline**: GMI baseline → T-1 execution hydration → intraday realtime accumulation, each stage producing a progressively fresher position snapshot;
- **Uniform aggregation key**: `GMI account + instrument symbol + long/short` is used consistently across all three stages, ensuring correct merge semantics;
- **SOD drift correction**: T-1 SOD from GMI FTP may be a stale snapshot; hydrating T-1 executions onto it corrects the baseline to true T day SOD before intraday messages begin arriving;
- **Realtime final state**: after intraday accumulation begins, the position is updated continuously as each execution message arrives, providing a realtime view.

### 3.5 Downstream Result Output Layer

Calculation-completed results, wrapped as `OctaneMessage`, are published to result Kafka Topics (`harbour.result.position`, `harbour.result.margin`). The result Topics have exactly two consumers:

1. **Margin Calculation Service** — consumes `harbour.result.position` as its position input (see [§3.8](#38-inter-module-data-pipeline));
2. **Query Service (CQRS)** — consumes **both** result Topics and builds materialized views.

All downstream business systems access position & margin data **exclusively through the Query Service REST/gRPC interfaces** — no downstream system consumes result Kafka Topics directly:

- **OSM** — Order Settlement Management System
- **Aviator** — Margin / Position Monitoring UI
- **FOX** — Position Query System

This single-entry constraint guarantees every downstream reads the same version of the data (same offsets, same snapshots), eliminating cross-system data divergence.

### 3.6 Query Service Layer (CQRS Read/Write Separation)

Designed for synchronous query scenarios, solving the problem that a pure streaming architecture cannot satisfy low-latency point queries. Read and write pipelines are physically isolated. Detailed rules in [Chapter 10](#10-query-service-layer-cqrs-readwrite-separation).

### 3.7 SOD & Intraday Data Merge Timing Specification

SOD snapshots come from GMI DB Adaptor; Intraday increments come from real-time adaptors. Their arrival timing is unguaranteed — the calculation module must handle the following scenarios.

#### 3.7.1 Key Timing Scenarios

| Scenario | Description | Handling Strategy |
|----------|-------------|-------------------|
| SOD arrives first, Intraday later | Normal path | SOD establishes baseline → Intraday increments accumulate |
| Intraday arrives first, SOD later | Post-open GMI snapshot delayed | Cache Intraday → replay merge after SOD arrives; mark account as `initializing` until merge complete |
| SOD & Intraday arrive concurrently | Concurrent writes | `business_date`-based partition lock; same trading day, same account serial merge |
| SOD data correction re-send | GMI publishes corrected SOD snapshot | SOD message carries `sod_version`, version increments; on receiving higher version, rollback that day's increment merge, rebuild baseline with new SOD |

#### 3.7.2 Dedup Strategy

The same trade may appear in both SOD and Intraday (e.g., GMI snapshot time window overlaps with real-time stream). The calculation module uses **`execution_id`** (see [§5.3.1](#531-core-message-definition-permanently-immutable)) for deduplication:

- On SOD load, record all `execution_id`s into the day's dedup set;
- On Intraday message arrival, check dedup set first; skip if hit;
- Dedup set is cleared and rebuilt on the next trading day's SOD arrival.

#### 3.7.3 State Markers

Each account has three merge states per trading day, exposed via query interface:

| State | Meaning | Query Behavior |
|-------|---------|---------------|
| `initializing` | SOD not yet ready, only Intraday increments accumulated | Return interim result + state marker; caller may choose to wait |
| `ready` | SOD ready, increment merge normal | Return final result |
| `stale` | No data received beyond threshold time | Return last known result + alert marker |

---

### 3.8 Inter-Module Data Pipeline

Calculation modules are not isolated silos — one module's output can feed another's input via Kafka, forming a processing DAG. The primary case is **Position → Margin**: margin calculation depends on position data.

```
  Adaptors ──→ position.sod / position.intraday
                    │
                    ▼
  ┌──────────────────────────────────────┐
  │        Position Calculation Module    │
  │  SOD + Intraday merge               │
  │  → realtime position per account     │
  └──────────────────┬───────────────────┘
                     │
                     │ harbour.result.position (OctaneMessage)
                     │ RoutingKey = POSITION
                     ├──────────────────────────────┐
                     ▼                              ▼
  ┌──────────────────────────────────────┐   Query Service (CQRS)
  │        Margin Calculation Module      │        │
  │  Consumes:                            │        │ REST / gRPC
  │    • position results (from above)    │        ▼
  │    • reference data (margin rates)    │   OSM / Aviator / FOX
  │    • market data (prices)             │
  │  → margin per account                 │
  └──────────────────┬───────────────────┘
                     │
                     │ harbour.result.margin (OctaneMessage)
                     ▼
               Query Service (CQRS) ──→ OSM / Aviator / FOX
```

#### 3.8.1 Why Not a Single Module?

| Approach | Issue |
|----------|-------|
| **Single monolith** doing both position and margin | Position changes trigger full margin recalculation; coupling makes independent scaling, testing, and deployment impossible |
| **Separate modules, chained via Kafka** | Position module scales independently of margin; position results are durable on Kafka — margin can replay from any offset; a margin bug doesn't corrupt position data |

#### 3.8.2 Data Flow Details

```
Position Calc publishes:
  Topic:   harbour.result.position
  Envelope: OctaneMessage {
      key  = RoutingKey.POSITION,
      body = PositionResult Proto {
          account_id, symbol, direction,
          sod_quantity, intraday_delta, current_position,
          business_date, status, extensions
      }
  }

Margin Calc consumes:
  Subscribes to: harbour.result.position
  For each OctaneMessage with key = POSITION:
      1. Deserialize body → PositionResult
      2. Look up margin rates from reference data cache
      3. Apply margin rules → compute margin requirement
      4. Publish to harbour.result.margin
```

#### 3.8.3 Ordering Guarantee

Position results for the same `account_id` arrive in order (single partition per account via `kafkaPartitionKey`). Margin Calc processes them in order, so margin is always derived from the latest position — no stale reads.

#### 3.8.4 Failure Isolation

If Margin Calc fails:
- Position Calc continues unaffected — positions keep updating on `harbour.result.position`
- On recovery, Margin Calc replays from its last committed offset
- No data loss; margin catches up to current position state

If Position Calc fails:
- Margin Calc sees no new position messages → last known margin remains
- Accounts with no updates beyond the stale threshold are marked accordingly
- On Position Calc recovery, margin resumes automatically

#### 3.8.5 General Pattern

The Position → Margin pipeline is one instance of a general pattern: **any calculation module can consume any result topic**. Other potential pipelines:

```
Position → Margin       (implemented)
Margin → Risk           (margin feeds risk limit checks)
Position → P&L          (position + market prices → P&L)
```

The constraints are always the same: consume `OctaneMessage` from the upstream result topic, filter by `RoutingKey`, deserialize `body` with the expected schema, compute, and publish to a new result topic.

---

## 4. End-to-End Complete Data Flow

1. Each market generates raw business data (mutually different protocols & structures);
2. The corresponding adaptor parses raw data, performs cleaning, field derivation, and market semantic packing;
3. The adaptor wraps data into `OctaneMessage` envelopes (with `RoutingKey` set by message type, `body` containing Protobuf-encoded core fields + extension fields);
4. `OctaneMessage` envelopes are published to the corresponding Kafka unified bus Topics (`position.*`, `margin.*`);
5. **Position Calculation Modules** subscribe to Kafka streams, execute SOD + Intraday merge, sub-account aggregation, and market-specific position logic;
6. Position results are wrapped as `OctaneMessage` and published to `harbour.result.position`;
7. **Margin Calculation Modules** consume `harbour.result.position` alongside reference data and market data, then execute margin rules and publish results to `harbour.result.margin`;
8. Both result Topics (`harbour.result.position`, `harbour.result.margin`) are consumed by the Query Service, which builds materialized views; downstream systems (OSM, Aviator, FOX) fetch position & margin data via Query Service REST/gRPC interfaces — no downstream consumes result Topics directly.

---

## 5. Unified Data Model Design (Core + Extension Mechanism)

> **Transport layer note**: All Kafka messages use the `OctaneMessage` envelope (see `knowledge/octane-message.md`). The data model described below defines the Protobuf schemas carried inside `OctaneMessage.body`. The envelope provides routing, versioning, and partitioning — orthogonal to the business schema design.

### 5.1 Design Background

Multiple markets have substantial market-specific business semantics:

| Market | Specific Semantics |
|--------|-------------------|
| China | Today position / yesterday position distinction, covered call marker (for fees and margin benefits) |
| India | SPAN parameter groups, combined spread position info |
| Generic Options | Frozen quantity, covered frozen quantity |

- Flattening all market fields into the core model → bloated model, semantic pollution, new markets require core structure modification;
- Directly discarding specific fields → calculation layer cannot complete business logic.

### 5.2 Extension Mechanism Design Principles

| Principle | Description |
|-----------|-------------|
| **Core is Core** | All-market common fields go into core message; structure permanently stable, unchanged by market iterations |
| **Extension is Extension** | Market-specific semantics stored independently, not intruding on core fields |
| **Isolated Consumption** | Generic modules read only core fields; market-specific modules parse extensions as needed |
| **Governance Loop** | Extension names & types uniformly registered; adaptors and CI enforce validation; eliminate illegal injection |

### 5.3 Technical Approach: `map<string, google.protobuf.Any>` Extension Slot

Uses Protobuf3 + `google.protobuf.Any` for type-safe, zero-intrusion extensions.

#### 5.3.1 Core Message Definition (Permanently Immutable)

```protobuf
// unified_position.proto
syntax = "proto3";
import "google/protobuf/any.proto";
import "harbour/common/enums.proto";

message UnifiedPosition {
    // Global core common fields (all markets)
    string execution_id = 1;        // Globally unique trade identifier, for dedup & audit trail
    string account_id = 2;
    string sub_account_id = 3;
    string symbol = 4;
    string mic = 5;                 // ISO market identifier code
    string venue = 6;               // Multiple trading venues possible under same MIC
    PositionDirection direction = 7;
    string quantity = 8;            // Amount/quantity as string to avoid floating-point precision loss
    string avg_price = 9;
    int64 timestamp = 10;           // Physical timestamp (trade occurrence moment)
    string business_date = 11;      // Trading day (YYYYMMDD), decoupled from timestamp, critical for SOD merge
    PositionType type = 12;         // Instrument type: futures/options
    string source = 13;             // Source adaptor identifier, for issue tracing

    // Sole unified extension entry point
    map<string, google.protobuf.Any> extensions = 20;
}
```

#### 5.3.2 Market / Business Independent Extension Messages

Extension files split by market and business dimension, fully decoupled:

```protobuf
// china_extensions.proto — China market extensions
syntax = "proto3";
package harbour.ext;

message ChinaPositionExt {
    string today_qty = 1;       // Today position
    string sod_qty = 2;         // Yesterday position (SOD)
    bool is_covered = 3;        // Covered call marker
}
```

```protobuf
// india_extensions.proto — India market extensions
syntax = "proto3";
package harbour.ext;

message IndiaPositionExt {
    string span_param_group = 1;
    repeated string combined_spreads = 2;
}
```

```protobuf
// option_extensions.proto — Options generic extensions (cross-market reuse)
syntax = "proto3";
package harbour.ext;

message OptionExt {
    string frozen_qty = 1;
    string today_frozen_qty = 2;
    string sod_frozen_qty = 3;
}
```

#### 5.3.3 Module Consumption Rules (Code Examples)

**Generic aggregation module**: Uses only core fields, completely unaware of extensions

```java
UnifiedPosition pos = ...;
BigDecimal totalQty = new BigDecimal(pos.getQuantity());
// Execute generic sub-account merge, global aggregation logic
```

**Market-specific module**: Strongly-typed extension parsing on demand

```java
// China market calculation module example
if (pos.getExtensionsMap().containsKey("china")) {
    Any any = pos.getExtensionsMap().get("china");
    ChinaPositionExt ext = any.unpack(ChinaPositionExt.class);
    // Use today position, yesterday position, covered marker for margin calculation
}
```

#### 5.3.4 Extension Key Enum Safety Layer (Compile-Time Protection)

`map<string, Any>` keys are free strings; typos (`"china"` vs `"chian"`) are runtime bugs. An enum registry is added on top, mapping keys to integer enums. Adaptors and calculation modules reference extensions via enum constants — typos caught at compile time:

```protobuf
// extension_key.proto
syntax = "proto3";
package harbour.ext;

enum ExtensionKey {
    EXT_UNKNOWN = 0;            // Reserved, prohibited
    EXT_CHINA_POSITION = 1;
    EXT_INDIA_POSITION = 2;
    EXT_OPTION = 3;
}
```

Adaptors use enum names as map keys when writing:

```java
// Compile-time safe: ExtensionKey.EXT_CHINA_POSITION.name() → "EXT_CHINA_POSITION"
pos.putExtensions(
    ExtensionKey.EXT_CHINA_POSITION.name(),
    Any.pack(chinaExt)
);
```

Calculation modules also use enum constants, avoiding magic strings:

```java
if (pos.containsExtensions(ExtensionKey.EXT_CHINA_POSITION.name())) {
    ChinaPositionExt ext = pos.getExtensionsMap()
        .get(ExtensionKey.EXT_CHINA_POSITION.name())
        .unpack(ChinaPositionExt.class);
}
```

> **Note**: Enum Keys are a **naming convention layer** atop `map<string, Any>`; they do not change the wire format and are backward-compatible. The enum definition table itself is the registry's core data structure.

### 5.4 Approach Comparison

| Approach | Verdict | Reason |
|----------|:-------:|--------|
| Flattened `optional` fields | ❌ Rejected | Model bloat, semantic pollution, new markets require core structure changes |
| `oneof` single-choice branch | ❌ Rejected | New markets still require core message changes; insufficient stability |
| `map<string, Any>` | ✅ Adopted | Core permanently stable, extensions independent, type-safe, zero-intrusion for new markets |

### 5.5 Extension Mechanism Governance Specification

#### Extension Registry

Maintain a unified registry (YAML) recording extension Key ↔ Protobuf type mapping. All extensions must be registered before use:

```yaml
# extensions_registry.yaml
# string_key: [enum_key, proto_message_type]
extensions:
  china:   [EXT_CHINA_POSITION, harbour.ext.ChinaPositionExt]
  india:   [EXT_INDIA_POSITION, harbour.ext.IndiaPositionExt]
  option:  [EXT_OPTION,        harbour.ext.OptionExt]
```

#### Governance Rules

- **Adaptor mandatory validation**: Prohibit unregistered string Keys or enum Keys; prohibit non-Protobuf raw data; must use enum constant references when writing (`ExtensionKey.EXT_XXX.name()`); CI scans prohibit bare-string `putExtensions("china", ...)` calls;
- **Dependency declaration**: Each market module must document its dependent extension Keys (string + enum) and message types;
- **CI automated checks**: Pipeline validates extension Keys are registered, enum references are valid, types are legal; illegal code blocked from merge;
- **Version compatibility**: Extension messages follow Protobuf forward-compatibility rules; iterations do not affect core or other markets; new extension Keys sync-add `ExtensionKey` enum values and registry entries.

### 5.6 Schema Registry: Full-Lifecycle Message Schema Governance

The YAML extension registry (see §5.5) only manages extension Key ↔ message type mappings — it does **not** manage Schema binary compatibility. In multi-team collaboration (adaptor teams independently iterate extension messages; calculation module teams independently upgrade consumers), compatibility breaks such as field type changes, field deletions, and field number conflicts may only surface post-deployment.

#### 5.6.1 Architecture

```
Adaptor ──→ Query Schema Registry on serialize ──→ Kafka (with Schema ID)
Calculation Module ──→ Query Schema Registry on deserialize ──→ Validate then process
Query Service ──→ Query Schema Registry on deserialize ──→ Validate then build views
```

#### 5.6.2 Technical Requirements

| Requirement | Description |
|-------------|-------------|
| Schema Registry selection | Confluent Schema Registry (native Protobuf support) |
| Compatibility mode | `FULL` (checks both forward and backward compatibility); breaking changes blocked from production |
| Registration scope | All Protobuf message types used by Kafka Topics: core messages (`UnifiedPosition`) + each extension message (`ChinaPositionExt`, `IndiaPositionExt`, `OptionExt`, etc.) |
| Subject naming | `topic-name-value` (e.g., `harbour.unified.position.intraday-value`) |
| CI integration | Pipeline executes `protoc` compilation + Schema Registry compatibility pre-check (dry-run validate) at build stage; incompatible → block merge |

#### 5.6.3 Integration with YAML Extension Registry

YAML registry and Schema Registry are complementary:

| Registry | Manages | Check Timing |
|----------|---------|-------------|
| **YAML Extension Registry** | Extension Key → enum value → message type mapping | CI static scan + adaptor runtime validation |
| **Schema Registry** | Protobuf message binary field compatibility (types, field numbers, delete/add rules) | Build-time CI pre-check + producer register / consumer deserialize real-time validation |

Both registries' subjects and version info are stored uniformly, forming the platform's **unified metadata center**. The YAML `proto_message_type` field references the corresponding subject in Schema Registry.

### 5.7 Deep Dive: Core Belongs to Core, Extensions Belong to Extensions

> This section further dissects the design philosophy of Chapter 5's extension mechanism, answering "why this design", "how to prevent overstuffing the message", and "what alternatives exist".

#### 5.7.1 The Essence: Intersection vs Union of Fields

Multi-market fields inherently have massive differences. Below is the field matrix for four markets + options generic:

```
China:   execution_id, account_id, symbol, ..., today_qty, sod_qty, is_covered
India:   execution_id, account_id, symbol, ..., span_param_group, combined_spreads
Korea:   execution_id, account_id, symbol, ..., (no special fields)
HK:      execution_id, account_id, symbol, ..., (no special fields)
Options: execution_id, account_id, symbol, ..., frozen_qty, today_frozen_qty, sod_frozen_qty
```

- **Intersection of all markets** → only ~13 fields like `execution_id`, `account_id`, `symbol`
- **Union of all markets** → 30+ fields, of which 60% are `null` for most markets
- **Flattened union approach** → bloated core message, semantic pollution, new markets force core `.proto` changes causing all downstream recompilation & redeployment

Harbour's approach: **core message stores only the intersection; the difference goes into `map<string, google.protobuf.Any>` extension slots**.

#### 5.7.2 Two-Layer Structure Breakdown

**Layer 1: Core Fields — Permanently Unchanging Common Attributes**

Core field inclusion criteria (all three must be satisfied):

| Criterion | Meaning |
|-----------|---------|
| **Shared by all markets** | All markets have this semantic (even if source field names differ, adaptors have mapped them) |
| **Generic module dependency** | Cross-market aggregation and other generic modules need to read it (e.g., `account_id`, `symbol`, `quantity`) |
| **Permanently non-deprecated** | A basic atom of position/margin business; will not be removed by business evolution |

```protobuf
message UnifiedPosition {
    string execution_id = 1;    // All markets: unique trade ID (dedup, audit trail)
    string account_id = 2;      // All markets: account
    string sub_account_id = 3;  // All markets: sub-account
    string symbol = 4;          // All markets: contract code
    string mic = 5;             // All markets: ISO market code
    string venue = 6;           // All markets: trading venue (multiple per MIC)
    PositionDirection direction = 7;  // All markets: buy/sell direction
    string quantity = 8;        // All markets: quantity (string avoids float precision loss)
    string avg_price = 9;       // All markets: average price
    int64 timestamp = 10;       // All markets: trade timestamp
    string business_date = 11;  // All markets: trading day YYYYMMDD
    PositionType type = 12;     // All markets: futures/options
    string source = 13;         // All markets: source adaptor identifier

    map<string, google.protobuf.Any> extensions = 20;  // ← Sole extension entry
}
```

> Once stable, this message is never modified again. When new markets onboard, core fields are untouched.

**Layer 2: Extension Slots — Independently Maintained Per-Market Private Fields**

Each extension is an **independent `.proto` file** with its own `package`, compiling into fully decoupled classes:

```protobuf
// china_extensions.proto — belongs only to China market
package harbour.ext;
message ChinaPositionExt {
    string today_qty = 1;       // Today position
    string sod_qty = 2;         // Yesterday position
    bool is_covered = 3;        // Covered call marker
}

// india_extensions.proto — belongs only to India market
package harbour.ext;
message IndiaPositionExt {
    string span_param_group = 1;
    repeated string combined_spreads = 2;
}

// option_extensions.proto — cross-market reusable options extension
package harbour.ext;
message OptionExt {
    string frozen_qty = 1;
    string today_frozen_qty = 2;
    string sod_frozen_qty = 3;
}
```

Key design points:

- Each extension file compiles independently; adding Korea-specific fields only requires a new `KoreaPositionExt`, without touching `ChinaPositionExt`, let alone `UnifiedPosition`
- `OptionExt` is a "cross-market reusable" extension (needed by both China and India options), but not placed in core fields — because futures markets don't need it

#### 5.7.3 Runtime Consumption: Isolation Principle (Core Design Essence)

Different modules **read different layers** — this is the key to maintaining extensibility without polluting the core:

```java
// ─── Generic Cross-Market Aggregation Module ───
// Reads only core fields, completely unaware of extensions, imports no extension classes
UnifiedPosition pos = consumer.next();
BigDecimal qty = new BigDecimal(pos.getQuantity());
aggregator.add(pos.getAccountId(), pos.getSymbol(), qty);
// ↑ This code is identical for all 4 markets, not a single line changes

// ─── China Market-Specific Module ───
// Reads core fields + parses China extension on demand
if (pos.containsExtensions(ExtensionKey.EXT_CHINA_POSITION.name())) {
    ChinaPositionExt ext = pos.getExtensionsMap()
        .get(ExtensionKey.EXT_CHINA_POSITION.name())
        .unpack(ChinaPositionExt.class);
    BigDecimal todayQty = new BigDecimal(ext.getTodayQty());
    BigDecimal sodQty = new BigDecimal(ext.getSodQty());
    // Calculate tiered margin based on today/yesterday position difference...
}
```

```
Consumption Isolation Illustration:

  Generic Module                  China Market Module
  ┌──────────────────┐       ┌──────────────────────────┐
  │ Reads only core   │       │ Reads core fields         │
  │ No extension import│      │ + Parses ChinaPositionExt │
  │ Never changed by   │       │ + Executes tiered margin  │
  │ new market onboarding│     │   logic                   │
  └──────────────────┘       └──────────────────────────┘
```

#### 5.7.4 Four-Layer Governance Defense: Preventing Extension Slots from Becoming a Dumping Ground

`map<string, Any>` itself is just free key-value pairs. Without governance, it eventually becomes a dumping ground full of typos, unregistered types, and deprecated fields. Harbour designed four progressive defense layers:

| Layer | Stage | Mechanism | Blocks |
|-------|-------|-----------|--------|
| **① Compile-time** | Code | `ExtensionKey` enum, no magic strings | Typos (`"china"` vs `"chian"`), undefined Keys |
| **② CI Static Scan** | Pipeline | Scan `putExtensions` calls against YAML registry | Unregistered Keys, type mismatches, bare-string calls |
| **③ Build-time** | CI | Schema Registry `FULL` compatibility pre-check | Field type changes, field number conflicts, breaking deletions |
| **④ Runtime** | Adaptor | Load registry to memory at startup, check before write | Unregistered Keys that bypassed CI (last line of defense) |

**Defense ①: ExtensionKey Enum (Compile-Time Safety)**

```java
// ❌ CI will block — bare string, typo not caught at compile time
pos.putExtensions("china", any);

// ✅ Only legal form — enum constant, typo exposed at compile time
pos.putExtensions(ExtensionKey.EXT_CHINA_POSITION.name(), any);
```

**Defense ②: YAML Registry + CI Static Scan**

```yaml
# extensions_registry.yaml — single source of truth for extensions
extensions:
  china:   [EXT_CHINA_POSITION, harbour.ext.ChinaPositionExt]
  india:   [EXT_INDIA_POSITION, harbour.ext.IndiaPositionExt]
  option:  [EXT_OPTION,        harbour.ext.OptionExt]
```

CI scan rules:
- All `putExtensions(key, any)` call keys must exist in registry
- `any.pack()` / `any.unpack()` generic types must match registry declarations
- Prohibit string-overload `putExtensions` (force enum path)

**Defense ③: Schema Registry FULL Compatibility**

Unlike the YAML registry managing "Key → type" mappings, Schema Registry manages "Protobuf binary compatibility":

- `FULL` mode: checks both forward (new producer → old consumer) and backward (old producer → new consumer) compatibility
- Allowed: add optional fields, delete optional fields (cautiously), add enum values
- Prohibited: change field types, change field numbers, delete required fields
- CI pre-check: `protoc` compilation then dry-run validation; incompatible → block merge

**Defense ④: Adaptor Runtime Startup Check**

```java
// Adaptor loads registry at startup
ExtensionRegistry registry = ExtensionRegistry.loadFromYaml("extensions_registry.yaml");

// Check before each extension write
public void putExtension(UnifiedPosition.Builder builder, ExtensionKey key, Any value) {
    if (!registry.isRegistered(key)) {
        throw new IllegalExtensionException("Unregistered extension Key: " + key);
    }
    if (!registry.matchesType(key, value)) {
        throw new IllegalExtensionException("Extension type mismatch: expected " 
            + registry.getType(key) + ", got " + value.getTypeUrl());
    }
    builder.putExtensions(key.name(), value);
}
```

#### 5.7.5 Complete Approach Comparison

| Approach | Verdict | Core Defect |
|----------|:-------:|-------------|
| **Flattened `optional` fields** | ❌ | Core message bloats to 30+ fields; new markets require core `.proto` changes, all downstream recompile & redeploy; field semantic ownership unclear |
| **`oneof` branch** | ❌ | Each message can only hold one market's data; cross-market aggregation module cannot process multiple markets simultaneously; new markets still require core message changes |
| **Per-market independent messages** | ❌ | Loses unification; Kafka Topics store multiple types; generic modules cannot consume; need type-check branches |
| **JSON free fields** | ❌ | Loses type safety; field typos only caught at runtime; large serialization size, poor performance; no Schema Registry compatibility validation |
| **`map<string, Any>` + governance** | ✅ | Core permanently 13 fields, extensions in independent files, type-safe, zero-intrusion core for new markets, four-layer governance prevents regression |

#### 5.7.6 Design Decision Summary

```
                     ┌──────────────┐
                     │ New Market    │
                     │ Onboarding    │
                     └──────┬───────┘
                            │
              ┌─────────────┼─────────────┐
              │             │             │
              ▼             ▼             ▼
       Core fields need  Extension fields  Margin rules
       additions?        need additions?   differ?
              │             │             │
              ▼             ▼             ▼
         ❌ No need      ✅ Create new    ✅ Create new plugin
        (intersection    extension .proto  Register to
         already covers)  + one registry   ProcessorReg
                          line
```

**Core Advantages**:

1. **Core message forever 13 fields** — no matter how many markets onboard, `UnifiedPosition` never changes
2. **New market zero-intrusion on core** — only add one extension `.proto` file + one YAML registry line
3. **Consumer on-demand parsing** — generic modules don't touch extensions; specific modules only parse what they care about
4. **Typos caught at compile time** — enum Keys replace magic strings
5. **CI blocks illegal injection** — registry + static scan + Schema Registry; can't merge without registration
6. **Extensions evolve independently** — upgrading China market extension fields does not affect India, Korea, or HK consumers

> This design pattern originates from standard Protobuf ecosystem practices — gRPC's `grpc-gateway` transcoding, Google Cloud API's `google.api.HttpBody`, and Istio's `EnvoyFilter` all use the same `map<string, Any>` extension mechanism to handle multi-version / multi-scenario differentiated data.

---

## 6. New Market Onboarding & Code Reuse Rules

The platform maximizes reuse of generic capabilities, distinguishing reusable parts from must-add-new parts, avoiding "zero modification" misconceptions.

### 6.1 Reuse vs. New Development Breakdown

| Capability Category | Reusable? | Description |
|--------------------|:---------:|-------------|
| Common calculation framework | ✅ Fully reusable | Sub-account aggregation, SOD initialization, Intraday increment merge, generic risk framework |
| Cross-market global aggregation module | ✅ Fully reusable | All-market position/margin aggregation logic, no changes needed |
| Adaptor standardization process | ✅ Reusable pattern | Unified process: parse → clean → extension pack → wrap as OctaneMessage |
| Kafka bus & downstream interfaces | ✅ Fully reusable | Topic structure & data format globally unified; downstream needs no adaptation |
| Market-specific margin rules | ❌ Must add new | Market rules differ (SPAN/tiered/KRX); requires independent plugin/module development |
| Market-specific position semantics | ❌ Partially new | Parse corresponding extension messages; implement market-specific logic |

### 6.2 New Market Onboarding Effort Summary

Onboarding a new market requires only **3 tasks**:

1. Develop the corresponding market adaptor;
2. Implement that market's specific margin calculation plugin/module;
3. Add extension message parsing logic (if market-specific semantics exist).

> Compared to traditional architecture (each market independently develops full adaptor + calculation + interface), new market onboarding only requires adaptor + margin plugin + extension parsing. Duplicated development modules (common calculation framework, Kafka bus, downstream interfaces, global aggregation) are all reused, significantly reducing development effort (specific percentage depends on the new market's margin rule complexity; this is a qualitative estimate).

---

## 7. Transitional Dual-Model Governance

During the project transition period, two data models coexist for rapid delivery and long-term architecture implementation, strictly controlled to avoid technical debt.

### 7.1 Dual-Model Positioning

| Model | Type | Purpose | Lifecycle |
|-------|------|---------|-----------|
| **Temporary Model** | RESTful Java POJO | External REST interface, supports OSM migration from old FTP pipeline | Transitional facility, planned decommission |
| **Final Model** | Protobuf (Octane) | Platform-internal Kafka streams, adaptors, calculation modules standard model | Long-term sole standard |

Temporary model classes include: `PositionRecord`, `FuturesPositionRecord`, `OptionPositionRecord`, `MarginRecord`, `ExtendedAccountMap`, `ExtendedAccount`.

### 7.2 Mandatory Governance Rules

1. POJO model marked `@Deprecated`, with explicit decommission timeline, transitional use only;
2. **POJOs auto-generated from Proto definitions; manual maintenance prohibited**: Use `protoc` plugins (or custom code generators) to auto-generate DTO classes for REST interfaces from `.proto` files. All field changes must first be made in Protobuf definitions, then rerun the code generator to produce new POJOs. CI pipeline verifies POJO files match generated output; hand-modified POJOs blocked from merge;
3. If exposing Proto-generated raw classes is inappropriate (e.g., concern about downstream coupling to Proto internal structure), a thin mapping layer (MapStruct / manual Mapper) may be added, but the mapping layer's source types still come from Proto-generated classes — no independent POJO maintenance;
4. Convergence plan: gradually connect REST interface bottom layer directly to Proto-generated classes, eventually decommission all intermediate mapping layers, with Protobuf as the sole data source.

### 7.3 Risk Mitigation

- ❌ Dual-model desync, field drift;
- ❌ Business logic long-term dependent on temporary POJOs, architecture cannot unify;
- ❌ Later-stage migration cost escalation.

> Through the above rules' full-chain control, risks are completely eliminated.

---

## 8. Stream Processing Semantics & Idempotency Specification

### 8.1 Exactly-Once Semantic Boundaries (Explicit Scope, Prevent Expectation Deviation)

| Layer | Semantic | Description |
|-------|----------|-------------|
| **Stream Processing Layer** | Exactly-Once | Based on Kafka Streams, within the consume → compute → produce closed loop: atomic Offset commit, transactional output, guaranteeing no duplicate processing and no state loss within the stream |
| **Business Idempotency** | Must carry idempotency key | All output messages must carry `idempotency_key`, formally defined as:

```
idempotency_key = source + ":" + account_id + ":" + business_date + ":" + sequence
```

Field meanings:
- `source`: Source adaptor identifier (see core message §5.3.1), prevents different adaptors generating the same `sequence` for the same account causing false dedup;
- `account_id`: Account identifier;
- `business_date`: Trading day (YYYYMMDD);
- `sequence`: Monotonically increasing sequence number generated by the adaptor layer, starting from 1 within each `(source, account_id, business_date)` combination, maintained by the adaptor.

Downstream uniformly uses this full key for dedup. Calculation module outgoing messages inherit the adaptor's idempotency key; newly produced calculation result messages construct new idempotency keys using `calculation_module_id + account_id + business_date + auto-increment sequence` |
| **External Interaction** | At-Least-Once | Connecting to databases or third-party services only guarantees at-least-once; relies on database unique constraints / Upsert semantics for dedup; does not promise end-to-end exactly-once |
| **Data Backfill & Replay** | Eventual Consistency | Fault recovery and historical backfill may replay messages; short-term data may be overwritten; final state consistent with real-time calculation |

### 8.2 Implementation Requirements

- Calculation modules add message idempotency validation logic;
- Operations documentation supplements replay and checkpoint recovery procedures.

### 8.3 State Recovery: Local State + Changelog Topic

Calculation modules (especially SOD/Intraday merge logic) maintain substantial stateful information — SOD baseline data, `execution_id` dedup sets, account merge states. Rebuilding state purely by replaying full history from Kafka beginning would require replaying up to 7 days of data within the retention period, causing fault recovery times far exceeding acceptable limits (minute-level backlog → tens of minutes of rebuilding).

#### 8.3.1 Recovery Strategies

| Approach | Applicable Scenario | Recovery Time |
|----------|-------------------|:-------------:|
| **Kafka Streams built-in Changelog** | Calculation modules using Kafka Streams | Seconds (restore state store from changelog topic) |
| **Manual Changelog Topic** | Calculation modules managing their own state (e.g., RocksDB) | Seconds (restore from changelog + replay tail messages) |
| **Pure Kafka Replay** | Degradation path without Changelog | Minutes~hours (depends on historical data volume) |

#### 8.3.2 Manual Changelog Specification (for non-Kafka-Streams approaches)

Calculation modules write each state change to a dedicated compacted changelog topic:

```
harbour.state.changelog.<module_name>
```

- **Compacted topic**: Key = `account_id + business_date`, Value = Protobuf-serialized state snapshot;
- **Write timing**: After completing each batch of message processing and state changes, async batch write to Changelog;
- **Recovery flow**: On restart, first consume full snapshot from Changelog to rebuild state → then consume from the original Topic's last committed offset, filling in tail messages that arrived after Changelog write;
- **Kafka Streams approach**: Configure `num.standby.replicas ≥ 1`; Standby instances auto-maintain hot-standby state; sub-second failover on primary instance failure.

#### 8.3.3 Operational Benefits

- Reduces hard dependency on Kafka historical retention time (no longer need to retain 7 days of full data solely for state recovery);
- Fault recovery time reduced from "tens of minutes of replay" to "Changelog load + tail catch-up ≈ seconds";
- Standby Replica enables hot standby, eliminating state rebuild wait during single-point failures.

### 8.4 Dead Letter Queue (DLQ) Specification

All components, after failing to process a single message and exhausting retries, must route the failed message to the Dead Letter Queue — silent discard is prohibited.

#### 8.4.1 Topic Naming

```
harbour.dlq.<source_component>
```

| Component | DLQ Topic |
|-----------|----------|
| China Adaptor | `harbour.dlq.china-adaptor` |
| India Adaptor | `harbour.dlq.india-adaptor` |
| China Calculation Module | `harbour.dlq.china-calc` |
| Global Aggregation Module | `harbour.dlq.global-agg` |
| Query Service | `harbour.dlq.query-service` |

#### 8.4.2 Message Format

Each DLQ message wraps the original message + failure metadata:

```protobuf
message DLQMessage {
    bytes original_message = 1;       // The failed OctaneMessage (envelope + body)
    string error_type = 2;            // Exception class name, e.g., "RefDataException"
    string error_message = 3;         // Exception details
    string stack_trace = 4;           // Stack trace (truncated to first 2KB)
    int32 retry_count = 5;            // Number of retries attempted
    int64 failure_timestamp = 6;      // Failure timestamp
    string source_component = 7;      // Source component
    string trace_id = 8;              // Original trace_id, for correlated investigation
}
```

#### 8.4.3 Retry Strategy

| Failure Type | Retry Strategy | Description |
|-------------|---------------|-------------|
| Transient (network timeout, RefData unavailable) | Exponential backoff: 100ms → 200ms → 400ms → 800ms → 1.6s (max 5 retries) | Exhausted → DLQ |
| Permanent (illegal message format, Schema mismatch) | No retry, direct to DLQ | Requires manual intervention |
| Business exception (margin calculation overflow, etc.) | No retry, direct to DLQ | Requires business logic or data source investigation |

#### 8.4.4 DLQ Operations

- **Alerting**: Any DLQ Topic message backlog > 0 triggers alert (every DLQ entry means data loss risk);
- **Replay Console**: Provides operations API `POST /admin/dlq/{topic}/replay`, supporting filtering by time range or specified offset; after fix, manually reinject to normal Topic;
- **Retention**: DLQ messages retained 30 days, auto-cleaned after expiration;
- **Monitoring Dashboard**: Grafana Dashboard displays each DLQ Topic backlog, growth rate, top error types.

---

## 9. Adaptor Layer Boundaries & Operation Specification

Re-clarify the boundary between "data preprocessing" and "business calculation", resolving issues where the original rules were too absolute and hard to implement in practice.

Core distinction principle: **Scope boundary** — derivations within a single message are preprocessing; cross-message / cross-account operations are business calculation.

### Allowed Scope (Single-Message Scope)

- Field conversion, data type mapping, timestamp uniform formatting;
- Filter dirty data based on data integrity rules (null values, illegal enums, out-of-range data);
- Derive unified model basic fields for this message from a **single** raw trade flow (e.g., calculate this record's position delta direction and quantity from trade direction × quantity, assigning to `quantity` and `direction` fields);
- Pack market-private business semantics into standard extension messages.

### Prohibited Scope (Cross-Message / Cross-Account Scope)

- Business rule calculations such as margin, P&L, and risk metrics;
- Position aggregation, hedge netting, or position consolidation across multiple records or accounts (summing `quantity` across multiple messages IS aggregation);
- Sub-account hierarchy relationship calculation and derivation;
- Any calculation based on historical state or external reference data.

### Enforcement

- Code review checklist + automated rule validation

---

## 10. Query Service Layer (CQRS Read/Write Separation)

### 10.1 Background

Pure streaming architecture excels at async event processing but cannot satisfy UI, query systems, and real-time risk control's low-latency synchronous point-query needs. If each downstream consumer independently builds views from streams, it causes data inconsistency and duplicated development.

### 10.2 Architecture Pipeline

```
Calculation Module → Kafka Result Topic → Query Service (Materialized Views) → REST/gRPC Interface
```

### 10.3 Core Capabilities

- Consume result streams, build `account_id`-indexed account snapshots in a **single-tier local RocksDB** — no in-process or remote cache tiers (no Caffeine, no Redis):

| Storage | Technology | Purpose | Data Volume |
|---------|-----------|---------|:-----------:|
| Local persistent store | RocksDB | Full account snapshots; every read/write hits RocksDB directly | Unlimited |

- **Startup hydration**: on every startup, each instance replays the result Topics and rebuilds its RocksDB state from scratch before serving query traffic. To make full rebuild possible, `harbour.result.position` / `harbour.result.margin` must use `cleanup.policy=compact` with `account_id` as the message key (compaction keeps the latest snapshot per account, so replay yields exactly the current state). The readiness probe (`/health/ready`) returns 200 only after hydration completes; hydration duration is bounded by the compacted Topic size and is monitored as a startup metric.
- All writes go synchronously to RocksDB; queries read RocksDB directly (point lookup by `account_id`). Since state is always rebuildable from Kafka, instances are disposable and interchangeable — scaling or failover simply spins up a new instance and re-hydrates;
- Provide standard query interfaces: `GetPosition`, `GetMargin`;
- Response headers carry `X-Data-Last-Update-Timestamp` along with the message's Kafka `offset` and `transaction_id`, identifying data version for issue tracing;
- **Kafka Consumer isolation level**: Must configure `isolation.level=read_committed`, ensuring only calculation module transaction-committed messages are consumed, preventing dirty reads of uncommitted transaction intermediate states in snapshots;
- Data guarantees **eventual consistency**; strong-consistency scenarios connect to dedicated settlement database interfaces.

### 10.4 Value

- Unified query entry, eliminating downstream duplicated development;
- Read/write pipeline decoupling, not affecting core stream processing logic.

---

## 11. Architecture Constraints & Operations Specification

- All market differences must be shielded at the adaptor layer; generic calculation modules prohibit market hard-coding;
- Strictly comply with adaptor operation whitelist; strictly prohibit boundary-violating business calculation implementation;
- Dual-model transition period enforces synchronization validation; new requirements prioritize Protobuf model;
- Stream semantic boundaries explicitly communicated externally; downstream must implement business idempotency;
- Extension messages strictly go through registry; prohibit private addition of extension Keys;
- Query Service only maintains read-only snapshots; must not modify any business data.

---

## 12. Platform Capability Summary

| Capability | Description |
|------------|-------------|
| Multi-level sub-accounts | Support GMI + multi-level sub-account unified position & margin calculation |
| Full instrument coverage | Futures, options full categories; support option frozen position & margin benefit logic |
| SOD + Intraday merge | Support SOD snapshot + Intraday incremental merge calculation |
| Global multi-market | Compatible with global multi-market, multi-exchange differentiated margin rules; strong horizontal scalability |
| Unified query access | All downstream systems (OSM / Aviator / FOX) access position & margin data via Query Service REST/gRPC — single entry, consistent data version |
| High availability | Support data replay, backfill, fault recovery; meet financial system high-availability and auditability requirements |
| Long-term maintainability | Layered decoupling + extension mechanism; long-term architecture stable, maintainable, evolvable |

---

## 13. Security Design

### 13.1 Transport Security

| Link | Requirement | Description |
|------|------------|-------------|
| Adaptor → Kafka | mTLS + SASL/SCRAM | Prevent unauthorized data injection; mutual certificate authentication |
| Kafka → Calculation Module | mTLS | Consumer certificate validation; prevent unauthorized consumption |
| Calculation Module → Result Topic | mTLS | Producer certificate validation |
| Query Service ↔ Downstream Clients | TLS 1.3 + API Token | Token issued by gateway, periodic rotation |
| All internal component links | TLS 1.3 | Eliminate plaintext transmission of sensitive position data within network |

### 13.2 Authentication & Authorization

| Interface Type | Auth Method | Authorization Granularity |
|---------------|------------|--------------------------|
| REST query interface | JWT (OAuth2 Client Credentials) | Account-level: only return `account_id`s the caller is authorized to view |
| gRPC query interface | mTLS + JWT | Same as above; also validated at interceptor layer |
| Admin interface (replay, operations) | mTLS + independent Admin Token | Only allowed for operations role |

### 13.3 Audit Logging

All query operations must record immutable audit logs:

```
{
  "timestamp": "2026-01-15T10:30:00.123Z",
  "caller": "osm-service",
  "operation": "GetPosition",
  "target_account": "CN_ACC_001",
  "result": "success",
  "trace_id": "a1b2c3d4"
}
```

- Logs written to independent Kafka Topic (`harbour.audit.log`), physically isolated from business Topics;
- Retention ≥ 90 days, meeting minimum financial compliance requirements.

### 13.4 Extension Message Injection Prevention

- Adaptors must validate Key & message type match via registry before writing to `extensions`;
- Calculation modules, before parsing extension messages, first validate type with `Any.is(ExpectedType.class)`; on type mismatch, log alert and discard that extension (do not discard the entire core message);
- CI static scan: prohibit `Any.pack()` calls passing non-registered message types.

---

## 14. Deployment & Scaling Model

### 14.1 Process / Container Boundaries

| Component | Deployment Unit | Instances | Scaling Strategy |
|-----------|----------------|:---------:|-----------------|
| Market adaptors | Independent container/process | 1~N | Independently scaled by upstream data volume; China market can deploy 3 instances |
| Kafka bus | Managed cluster | — | Maintained by infrastructure team |
| Generic calculation modules | Independent container/process | 2+ | HPA auto-scale based on CPU usage |
| Market-specific calculation modules | Independent container/process | 1~N | Independently scaled by their market throughput |
| Global aggregation module | Independent container/process | 2+ | Consumer Group partitioned by `account_id`, horizontal scaling; on failure, corresponding partitions taken over by sibling instances |
| Query Service | Independent container/process | 2+ | Stateless (RocksDB rebuilt from compacted result Topics on every startup); HPA auto-scale by QPS |

### 14.2 Kafka Partition Mapping

| Topic | Partitions | Partition Key | Max Consumer Instances |
|-------|:---------:|--------------|:----------------------:|
| `harbour.unified.position.sod` | 16 | `account_id` hash | 16 |
| `harbour.unified.position.intraday` | 32 | `account_id` hash | 32 |
| `harbour.unified.margin.sod` | 16 | `account_id` hash | 16 |
| `harbour.unified.margin.intraday` | 32 | `account_id` hash | 32 |

> Partition counts designed based on estimated peak throughput (10,000 msg/s), estimated at 1 partition : 300 msg/s. May be increased in production based on measured data.

### 14.3 Cross-Region Deployment

| Region | Data Residency Requirement | Deployment Strategy |
|--------|:-------------------------:|---------------------|
| China Mainland | Data must not leave country | Independent Kafka cluster + full Harbour instance |
| Hong Kong | Can egress to Asia-Pacific | Adaptor deployed in HK; data sent via dedicated line to Asia-Pacific center Kafka |
| India | Data must not leave country | Independent Harbour instance |
| Korea | Can egress to Asia-Pacific | Adaptor deployed in Korea; data sent to Asia-Pacific center Kafka |

> Cross-region aggregation is performed by specific instances of the global aggregation module consuming multi-region result Topics; not real-time (T+1 minute); requires additional network authorization and security audit.

---

## 15. Observability & Monitoring

### 15.1 Distributed Tracing

#### Standards & Implementation

Adopts **W3C Trace Context** standard (`traceparent` + `tracestate` headers); all components uniformly use **OpenTelemetry SDK** for automatic injection and extraction of trace context.

```
Adaptor (OTel SDK) → Kafka Header (traceparent) → Calculation Module (OTel SDK) → Result Kafka Header (traceparent) → Downstream / Query Service (OTel SDK)
```

| Component | OpenTelemetry Integration | Description |
|-----------|--------------------------|-------------|
| Adaptor | OTel Java Agent auto-instrumentation + manual Span creation | Auto-capture Kafka Producer calls; manually create processing Spans per message |
| Calculation Module | OTel Java Agent auto-instrumentation + manual Span creation | Auto-capture Kafka Consumer/Producer; manually create Spans for calculation logic |
| Query Service | OTel Java Agent + gRPC/REST interceptors | Auto-capture inbound requests and RocksDB queries |
| Global Aggregation Module | Same as Calculation Module | — |

#### Trace Propagation Path

Each message generates or inherits a `trace_id` at the adaptor entry, transparently propagated across all components:

```
Adaptor Entry (root span)
  └── Field Mapping (child span)
  └── Kafka Produce (child span, inject traceparent to header)
        └── Calculation Module Consume (span, extract traceparent from header)
              └── Margin Calculation (child span)
              └── Kafka Produce (child span)
                    └── Query Service Consume (span)
                          └── Cache Query (child span)
```

#### Backend Integration

- Export trace data via OpenTelemetry Collector to **Jaeger / Grafana Tempo / Zipkin**, enabling end-to-end call chain visualization without manual log correlation to locate latency bottlenecks;
- Logs simultaneously print `trace_id` and `span_id`, enabling cross-search with ELK / Grafana Loki.

### 15.2 Core Metrics

| Metric Category | Metric | Collection Method | Alert Threshold (Suggested) |
|----------------|--------|-------------------|:--------------------------:|
| Throughput | Adaptor output msg/s | Prometheus Counter | < 50% of expected |
| Throughput | Calculation module processing msg/s | Prometheus Counter | < 50% of expected |
| Latency | Adaptor → Kafka write latency (p99) | Prometheus Histogram | > 50ms |
| Latency | End-to-end latency: adaptor entry → calculation result output (p99) | Calculation module compares `timestamp` on consume | > 500ms |
| Reliability | Adaptor input/output message count reconciliation | Compare Counter every 5 min | Difference > 0.01% |
| Reliability | Error rate (parse failures + calculation exceptions) / total messages | Prometheus Counter | > 0.1% |
| Resource | Query Service RocksDB read latency (p99) | OTel / Prometheus Histogram | > 10ms |
| Resource | Query Service startup hydration duration | Startup metric | > 5 minutes |
| Resource | CPU / Memory / Disk | Node Exporter | CPU > 70%, Memory > 80% |
| Business | Percentage of accounts with `stale` state | Calculation module reports | > 5% |
| Business | SOD data delay (current time - SOD `business_date` expected arrival time) | Periodic check | > 10 minutes |

### 15.3 Health Check Endpoints

| Endpoint | Purpose | Returns |
|----------|---------|---------|
| `GET /health/live` | Kubernetes Liveness Probe | 200 if process alive |
| `GET /health/ready` | Kubernetes Readiness Probe | 200 if upstream connections normal + internal state ready (Query Service additionally requires RocksDB hydration complete) |
| `GET /health/deep` | Operations deep check | Connection status details for each dependency (Kafka, DB, RocksDB) |

### 15.4 Data Reconciliation

- Adaptor, after each successful Kafka batch write, reports `{source, topic, partition, offset_range, message_count}` to reconciliation Topic;
- Calculation module reports consumption statistics to the same Topic after processing each batch;
- Independent reconciliation service compares upstream/downstream counts every 5 minutes; difference > 0.01% triggers alert.

---

## 16. Testing Strategy

### 16.1 Testing Pyramid

| Level | Scope | Coverage Requirement | Description |
|-------|-------|:-------------------:|-------------|
| Unit Tests | Adaptor field mapping, calculation module single rules | ≥ 80% | Mock upstream raw data; verify field mapping correctness |
| Integration Tests | Adaptor → Kafka → Calculation Module → Result validation | Core scenarios 100% | Use Testcontainers to start Kafka; verify end-to-end calculation correctness |
| Replay Tests | Historical trading day data complete replay | Every trading day | Take historical real SOD + Intraday data; compare replay results with historical snapshots |
| Chaos Tests | Network latency, Kafka disconnection, SOD out-of-order arrival | Quarterly | Inject faults; verify degradation behavior and recovery capability |

### 16.2 New Market Onboarding Test Checklist

When onboarding a new market, all of the following tests must pass:

1. Adaptor unit tests: cover all upstream raw message types → standard Proto mapping;
2. Extension message serialization/deserialization round-trip tests;
3. Adaptor → Kafka integration test: simulate upstream data push; verify Topic write correctness;
4. Market-specific calculation module unit tests: cover all margin/position rule scenarios for that market;
5. End-to-end replay test: take at least 3 historical trading days of that market's data for complete replay; result deviation from historical snapshot < 0.01%.

---

## 17. Market-Specific Special Trading Day Handling

### 17.1 Trading Calendar Service

The platform requires an independent market trading calendar service (or integration with existing enterprise trading calendar) to provide trading day determination capabilities to all layers:

| Service Capability | Description |
|-------------------|-------------|
| `isTradingDay(mic, date)` | Determine whether the specified market is a trading day on the given date |
| `getPreviousTradingDay(mic, date)` | Get the most recent previous trading day |
| `getSODExpectedTime(mic, date)` | Return the expected time window for that market's SOD data arrival |

### 17.2 Layer-Specific Adaptation Rules

| Scenario | Handling Rule |
|----------|---------------|
| **Holiday** | Adaptor produces no Intraday data; SOD Adaptor produces no new SOD snapshot; calculation module maintains previous trading day's final state; account marked as `ready` (not `stale`) |
| **Half-day trading** | Adaptor pushes data normally; SOD snapshot timestamp set to that half-day's market open time; calculation module merges normally |
| **Emergency market closure** | Adaptor detects exchange status change and immediately stops pushing; already-pushed increment data processed normally; account state marked as `stale` if unchanged within 5 minutes |
| **Timezone alignment** | Each adaptor converts upstream timestamps to UTC before filling `timestamp` field; `business_date` calculated per each market's local trading calendar (China UTC+8, India UTC+5:30); global aggregation aligns by UTC time window |

### 17.3 Cross-Market Aggregation Trading Day Alignment

When the global aggregation module consumes multi-market data, each market's `business_date` may differ (timezone differences + local holidays). Aggregation logic uses **UTC calendar day** for same-day aggregation; `business_date` is only passed through to downstream as source market reference information — no cross-market alignment is performed.

---

## Appendix A: Glossary

| Term | Full Name / Description |
|------|------------------------|
| **SOD** | Start of Day — snapshot recording the static position and margin state at the opening of each trading day |
| **Intraday** | Intraday incremental data — trade/position changes generated in real-time during trading hours |
| **GMI** | Global Multi-level Investor — system managing account hierarchy relationships |
| **Octane** | Code name for the platform's internal standard Protobuf data model |
| **Adaptor** | Adaptor — service responsible for converting upstream heterogeneous data into the platform standard format |
| **MIC** | Market Identifier Code — ISO 10383 defined market identifier |
| **CQRS** | Command Query Responsibility Segregation |
| **DLQ** | Dead Letter Queue — stores messages that failed processing |
| **SPAN** | Standard Portfolio Analysis of Risk — India/international portfolio margin calculation methodology |
| **KRX** | Korea Exchange — and its margin rules |
| **Proto / Protobuf** | Protocol Buffers — Google's cross-language serialization protocol |
| **EOD** | End of Day — day-end processing, counterpart to SOD |

---

> **Document Version**: V1.7  
> **Last Updated**: 2026-07-19  
> **Change Summary**:
> - **V1.7**: Query Service storage simplified to single-tier local RocksDB (§10.3) — removed Caffeine/Redis tiers; state rebuilt by replaying compacted result Topics on every startup (hydration gated by readiness probe); `harbour.result.*` Topics mandated `cleanup.policy=compact` keyed by `account_id`. Fixed top-level architecture diagram: Position Calculation and Margin Calculation drawn as two independent microservices chained via `harbour.result.position` Kafka Topic, not co-located in one process. Corrected downstream consumption model (§3.5, §4): result Topics have exactly two consumers — Margin Calculation (Topic A) and Query Service (Topics A & B); OSM / Aviator / FOX access data exclusively via Query Service REST/gRPC, no direct stream consumption.
> - **V1.6**: Added `margin-module-architecture.md` (Margin calculation module multi-source event aggregation architecture, manual override REST endpoints, reference data cache layer, Position vs Margin module comparison).
> - **V1.5**: Added `calculation-module-architecture.md` (Position calculation module complete Class architecture, state machine implementation, Kafka Streams topology, fault recovery plan).
> - **V1.4**: Added §3.3.3 SOD & Intraday Separate Topic Design Deep Dive (6-dimension demonstration: consumption semantics, throughput, retention policy, fault recovery, partition strategy, versioning/dedup semantics).
> - **V1.3**: Added §5.7 Data Model Design Deep Dive (intersection vs union, two-layer structure breakdown, consumption isolation principle, four-layer governance defense, complete approach comparison, design decision summary).
> - **V1.2**: Global aggregation module changed to `account_id`-sharded Consumer Group horizontal scaling (§3.4.2); Added §5.6 Schema Registry full-lifecycle governance; Query Service mandatory `isolation.level=read_committed` (§10.3); Idempotency key formally defined as `source:account_id:business_date:sequence` (§8.1); POJOs changed to Proto auto-generation, manual maintenance prohibited (§7.2); Distributed tracing standardized to W3C Trace Context + OpenTelemetry (§15.1); Added §8.3 Changelog state recovery specification; Added §8.4 DLQ dead letter queue specification.
> - **V1.1**: Added Chapters 13–17 (Security, Deployment, Observability, Testing, Trading Day Handling) + Appendix Glossary; Corrected architecture diagram to fork structure; Added §3.7 SOD/Intraday merge timing specification; Core message supplemented with 4 fields; Extension Keys added enum safety layer; Idempotency key `sequence` definition clarified.
