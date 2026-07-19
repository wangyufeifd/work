# Harbour Unified Position & Margin Middle Platform — Complete Architecture Design

## Table of Contents

1. [Project Overview](#1-project-overview)
   - 1.1 [Positioning](#11-positioning)
   - 1.2 [Core Problems Solved](#12-core-problems-solved)
   - 1.3 [Overall Architecture Philosophy](#13-overall-architecture-philosophy)
   - 1.4 [Design Principles](#14-design-principles)
2. [Overall Layered Architecture & Responsibilities](#2-overall-layered-architecture--responsibilities)
   - 2.1 [Upstream Heterogeneous Data Source Layer](#21-upstream-heterogeneous-data-source-layer)
   - 2.2 [Adaptor Standardization Layer](#22-adaptor-standardization-layer)
   - 2.3 [Kafka Unified Message Bus Layer](#23-kafka-unified-message-bus-layer) *(2.3.3: SOD & Intraday Separate Topic Design)*
   - 2.4 [Calculation Module Layer](#24-calculation-module-layer-core-business-layer) *(2.4.4: Realtime Position Calculation Example)*
   - 2.5 [Downstream Result Output Layer](#25-downstream-result-output-layer)
   - 2.6 [Query Service Layer (CQRS)](#26-query-service-layer-cqrs-readwrite-separation)
3. [End-to-End Complete Data Flow](#3-end-to-end-complete-data-flow)
4. [Unified Data Model Design](#4-unified-data-model-design-core--extension-mechanism)
   - 4.1 [Design Background](#41-design-background)
   - 4.2 [Extension Mechanism Design Principles](#42-extension-mechanism-design-principles)
   - 4.3 [Technical Approach: map<string, Any> Extension Slot](#43-technical-approach-mapstring-googleprotobufany-extension-slot)
   - 4.4 [Approach Comparison](#44-approach-comparison)
   - 4.5 [Extension Mechanism Governance](#45-extension-mechanism-governance-specification)
   - 4.6 [Schema Registry](#46-schema-registry-full-lifecycle-message-schema-governance)
   - 4.7 [Deep Dive: Core vs Extensions](#47-deep-dive-core-belongs-to-core-extensions-belong-to-extensions)
5. [Query Service (CQRS)](#5-query-service-layer-cqrs-readwrite-separation)
   - 5.1 [Background](#51-background)
   - 5.2 [Architecture Pipeline](#52-architecture-pipeline)
   - 5.3 [Core Capabilities](#53-core-capabilities)
   - 5.4 [Value](#54-value)
6. [SOD & Intraday Data Merge Timing](#6-sod--intraday-data-merge-timing)
   - 6.1 [Key Timing Scenarios](#61-key-timing-scenarios)
   - 6.2 [Dedup Strategy](#62-dedup-strategy)
   - 6.3 [State Markers](#63-state-markers)
7. [Inter-Module Data Pipeline](#7-inter-module-data-pipeline)
   - 7.1 [Why Not a Single Module?](#71-why-not-a-single-module)
   - 7.2 [Data Flow Details](#72-data-flow-details)
   - 7.3 [Ordering Guarantee](#73-ordering-guarantee)
   - 7.4 [Failure Isolation](#74-failure-isolation)
   - 7.5 [General Pattern](#75-general-pattern)
8. [New Market Onboarding](#8-new-market-onboarding--code-reuse-rules)
   - 8.1 [Reuse vs. New Development](#81-reuse-vs-new-development-breakdown)
   - 8.2 [Onboarding Effort Summary](#82-new-market-onboarding-effort-summary)

---

## 1. Project Overview

### 1.1 Positioning

Harbour is a **unified position & margin calculation middle platform** serving multiple markets and exchanges. The platform uniformly connects to heterogeneous data sources across regions, standardizes data formats, calculation logic, and interface specifications, providing consistent and reliable position and margin data to downstream trading, settlement, risk control, and monitoring systems.

### 1.2 Core Problems Solved

- Exchange/market data source protocols, fields, and business semantics vary greatly, leading to high repeated adaptation costs;
- Traditional architectures develop full code for each market independently, causing duplication and maintenance overhead;
- Downstream systems need to integrate with multiple heterogeneous interfaces, imposing integration and operational burdens;
- The existing architecture cannot support unified multi-level sub-account management and cross-market unified risk calculation.

### 1.3 Overall Architecture Philosophy

> **Unified Data Model + Isolated Adaptor Layer + Reusable Calculation Modules + Kafka Stream-Driven Architecture + CQRS Query Service + Standardized Extension Mechanism**

---

## 2. Overall Layered Architecture & Responsibilities

The platform is divided top-to-bottom into **6 logically independent layers**, each with a single responsibility and clear boundaries, supporting independent development, testing, deployment, and scaling.

```
                         ┌──────────────────────────────────────┐
                         │       Upstream Heterogeneous Data     │
                         │   FTP · Kafka · REST · Database    │
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
  │  Query Service    │                                 │  Downstream       │
  │  (CQRS)           │                                 │  Systems          │
  │                   │                                 │                  │
  │  RocksDB · Views  │                                 │  REST / gRPC      │
  │  REST / gRPC      │                                 │                   │
  └──────────────────┘                                 └──────────────────┘
```

### 2.1 Upstream Heterogeneous Data Source Layer

Aggregates all business market raw data; different markets use different protocols, structures, and semantics:

| Data Source Type | Example | Protocol |
|-----------------|---------|----------|
| Real-time stream | Kafka topic | Kafka |
| Batch file | FTP/SFTP download | FTP |
| REST API | HTTP endpoint | HTTP |
| Database | SQL query / snapshot | DB |

### 2.2 Adaptor Standardization Layer

Each market/data source corresponds to an independent adaptor service responsible for raw data parsing, cleaning, standardization, and format conversion.

#### 2.2.1 Existing Adaptor Inventory

- **SOD Adaptor** — Reads end-of-day position snapshots from database or FTP
- **Stream Adaptor** — Consumes real-time execution streams from Kafka or REST APIs

#### 2.2.2 Core Responsibilities

Parse upstream raw data, perform cleaning, field mapping, and semantic packing, wrap as `OctaneMessage` envelopes, and publish to the Kafka message bus.

#### 2.2.3 Allowed Operations (Whitelist)

> Adaptors operate within a per-message scope. Single-message derivations — including arithmetic (e.g. direction × quantity → position delta) — are allowed. Cross-message or cross-account logic belongs in the calculation layer, not the adaptor.

- Field mapping, data type conversion, timestamp uniform formatting;
- Filter dirty data based on data integrity rules (null values, illegal enums, out-of-range data);
- Derive basic fields required by the unified model from a single raw trade flow (e.g., calculate position delta from trade direction + quantity);
- Pack market-private business semantics into standard extension messages (`extensions`).

#### 2.2.4 Operations That Belong in the Calculation Layer (Not the Adaptor)

- Margin, P&L, and risk metric calculations;
- Position aggregation, hedge netting, or position consolidation across multiple records or accounts;
- Sub-account hierarchy resolution.

> **Enforcement**: Code Review + automated checks keep these boundaries clear.

### 2.3 Kafka Unified Message Bus Layer

The platform's core data hub, bearing data transport, buffering, replay, backfill, and fault recovery capabilities. All links transmit only `OctaneMessage` — the unified envelope wrapping Protobuf-encoded business payloads. Every producer and consumer reads and writes this single message type; the `RoutingKey` field in the envelope determines which schema to use when deserializing `body`.

#### 2.3.1 Standard Topic Partitioning (by data type + time dimension)

| Topic | Description |
|-------|-------------|
| `harbour.unified.position.sod` | SOD snapshot position data |
| `harbour.unified.position.intraday` | Intraday incremental real-time position data |
| `harbour.unified.margin.sod` | SOD snapshot margin data |
| `harbour.unified.margin.intraday` | Intraday incremental real-time margin data |

#### 2.3.2 Basic Capabilities

Supports data replay, historical backfill, and fault checkpoint resume; all Topic data structures are globally unified and **market-agnostic**.

#### 2.3.3 Deep Dive: SOD & Intraday Separate Topic Design

> Why must SOD and Intraday be two separate Topics? This section demonstrates the necessity from 6 dimensions.

**Fundamental Differences Between the Two Data Types**:

| Attribute | SOD (Start of Day) | Intraday |
|-----------|-------------------|----------|
| **Source** | Database snapshot or FTP file | Real-time adaptors (Kafka streams, REST APIs) |
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

This integrates with the state recovery approach described in Changelog state recovery: separated Topics enable independent Changelogs.

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
| **Versioning** | `sod_version` increments (corrected re-send) | No version concept |
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

### 2.4 Calculation Module Layer (Core Business Layer)

> For detailed design of calculation module class architecture, state machine implementation, Kafka Streams topology, and fault recovery, see standalone documents:
> - Position calculation module: **`calculation-module-architecture.md`**
> - Margin calculation module (multi-source aggregation + manual override + reference data cache): **`margin-module-architecture.md`**

All calculation modules subscribe only to Kafka unified streams, fully decoupled from upstream data sources. After completing business calculations, results are wrapped as `OctaneMessage` and output to result-type Kafka Topics.

**Position Calculation and Margin Calculation are two separate, independent microservices** — separate processes, separate deployments, independently scaled. They never communicate in-process or via direct RPC: Position Calculation publishes results to `harbour.result.position`, and Margin Calculation consumes them from that Kafka Topic (see [§3.8](#7-inter-module-data-pipeline)).

#### 2.4.1 Module Classification

| Type | Module | Deployment Mode |
|------|--------|----------------|
| Market-specific calculation | Per-market position/margin modules | Independently deployed per market, independently scaled by market throughput |
| Global cross-market aggregation | All-market position & margin unified aggregation module | Consumer Group multi-instance, horizontally scaled by `account_id` partition |

#### 2.4.2 Global Aggregation Sharding Strategy

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

#### 2.4.3 Core Calculation Capabilities

- multi-level sub-account position & margin aggregation;
- SOD snapshot + Intraday incremental data merge calculation;
- Futures, options full-category position calculation, option frozen position, margin benefit logic;
- Market-specific margin rule calculation (tiered margin, SPAN, exchange-specific rules, etc.);
- Cross-market unified risk metric statistics.

#### 2.4.4 Example: Realtime Position Calculation Flow

Below is a concrete position calculation pipeline, illustrating how the abstract layered architecture maps to an implementation:

```
Step ①: Load T-1 SOD baseline
  ┌─────────────────────────────────────────────────────┐
  │  T-1 SOD position file (FTP or database snapshot)    │
  │  ↓ Filter: keep relevant records                     │
  │  ↓ Aggregate by key: account + instrument            │
  │    symbol + long/short direction                      │
  │  ↓ Result: T-1 SOD position baseline per key          │
  └─────────────────────────────────────────────────────┘

Step ②: Hydrate T-1 executions onto the baseline
  ┌─────────────────────────────────────────────────────┐
  │  Kafka Topic: T-1 all execution messages             │
  │  ↓ Aggregate by the same key                          │
  │    (account + instrument symbol + long/short)         │
  │  ↓ Merge onto T-1 SOD baseline                        │
  │  ↓ Result: T day SOD position data                    │
  └─────────────────────────────────────────────────────┘

Step ③: Realtime intraday accumulation
  ┌─────────────────────────────────────────────────────┐
  │  Kafka Topic: intraday realtime execution messages   │
  │  ↓ Aggregate by the same key                          │
  │  ↓ Accumulate onto T day SOD position                 │
  │  ↓ Result: realtime position (continuously updated)   │
  └─────────────────────────────────────────────────────┘
```

Key design points:

- **Three-stage pipeline**: SOD baseline → T-1 execution hydration → intraday realtime accumulation;
- **Uniform aggregation key**: `account + instrument symbol + long/short` is used consistently across all three stages;
- **SOD drift correction**: T-1 SOD from a file or database may be a stale snapshot; hydrating T-1 executions onto it corrects the baseline before intraday messages begin arriving;
- **Realtime final state**: after intraday accumulation begins, the position is updated continuously as each execution message arrives.

### 2.5 Downstream Result Output Layer

Calculation-completed results, wrapped as `OctaneMessage`, are published to result Kafka Topics (`harbour.result.position`, `harbour.result.margin`). The result Topics have exactly two consumers:

1. **Margin Calculation Service** — consumes `harbour.result.position` as its position input (see [§3.8](#7-inter-module-data-pipeline));
2. **Query Service (CQRS)** — consumes **both** result Topics and builds materialized views.

All downstream business systems access position & margin data **exclusively through the Query Service REST/gRPC interfaces** — no downstream system consumes result Kafka Topics directly:

- **OSM** — Order Settlement Management System
- **Aviator** — Margin / Position Monitoring UI
- **FOX** — Position Query System

This single-entry constraint guarantees every downstream reads the same version of the data (same offsets, same snapshots), eliminating cross-system data divergence.

### 2.6 Query Service Layer (CQRS Read/Write Separation)

Designed for synchronous query scenarios, solving the problem that a pure streaming architecture cannot satisfy low-latency point queries. Read and write pipelines are physically isolated. Detailed rules in [Chapter 5](#5-query-service-layer-cqrs-readwrite-separation).

## 3. End-to-End Complete Data Flow

1. Each market generates raw business data (mutually different protocols & structures);
2. The corresponding adaptor parses raw data, performs cleaning, field derivation, and market semantic packing;
3. The adaptor wraps data into `OctaneMessage` envelopes (with `RoutingKey` set by message type, `body` containing Protobuf-encoded core fields + extension fields);
4. `OctaneMessage` envelopes are published to the corresponding Kafka unified bus Topics (`position.*`, `margin.*`);
5. **Position Calculation Modules** subscribe to Kafka streams, execute SOD + Intraday merge, sub-account aggregation, and market-specific position logic;
6. Position results are wrapped as `OctaneMessage` and published to `harbour.result.position`;
7. **Margin Calculation Modules** consume `harbour.result.position` alongside reference data and market data, then execute margin rules and publish results to `harbour.result.margin`;
8. Both result Topics (`harbour.result.position`, `harbour.result.margin`) are consumed by the Query Service, which builds materialized views; downstream systems (OSM, Aviator, FOX) fetch position & margin data via Query Service REST/gRPC interfaces — no downstream consumes result Topics directly.

---

## 4. Unified Data Model Design (Core + Extension Mechanism)

> **Transport layer note**: All Kafka messages use the `OctaneMessage` envelope (see `knowledge/octane-message.md`). The data model described below defines the Protobuf schemas carried inside `OctaneMessage.body`. The envelope provides routing, versioning, and partitioning — orthogonal to the business schema design.

### 4.1 Design Background

Different markets and instruments have specific business semantics not shared by all. Examples:

| Scenario | Specific Semantics |
|----------|-------------------|
| Some equity markets | Today/yesterday position distinction, covered call marker |
| Some derivatives markets | SPAN parameter groups, combined spread position info |
| Options (cross-market) | Frozen quantity, covered frozen quantity |

- Flattening all market fields into the core model → bloated model, semantic pollution, new markets require core structure modification;
- Directly discarding specific fields → calculation layer cannot complete business logic.

### 4.2 Extension Mechanism Design Principles

| Principle | Description |
|-----------|-------------|
| **Core is Core** | All-market common fields go into core message; structure permanently stable, unchanged by market iterations |
| **Extension is Extension** | Market-specific semantics stored independently, not intruding on core fields |
| **Isolated Consumption** | Generic modules read only core fields; market-specific modules parse extensions as needed |
| **Governance Loop** | Extension names & types uniformly registered; adaptors and CI enforce validation; eliminate illegal injection |

### 4.3 Technical Approach: `map<string, google.protobuf.Any>` Extension Slot

Uses Protobuf3 + `google.protobuf.Any` for type-safe, zero-intrusion extensions.

#### 4.3.1 Core Message Definition (Permanently Immutable)

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

#### 4.3.2 Market / Business Independent Extension Messages

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

#### 4.3.3 Module Consumption Rules (Code Examples)

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

#### 4.3.4 Extension Key Enum Safety Layer (Compile-Time Protection)

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

### 4.4 Approach Comparison

| Approach | Verdict | Reason |
|----------|:-------:|--------|
| Flattened `optional` fields | ❌ Rejected | Model bloat, semantic pollution, new markets require core structure changes |
| `oneof` single-choice branch | ❌ Rejected | New markets still require core message changes; insufficient stability |
| `map<string, Any>` | ✅ Adopted | Core permanently stable, extensions independent, type-safe, zero-intrusion for new markets |

### 4.5 Extension Mechanism Governance Specification

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

### 4.6 Schema Registry: Full-Lifecycle Message Schema Governance

The YAML extension registry (see §4.5) only manages extension Key ↔ message type mappings — it does **not** manage Schema binary compatibility. In multi-team collaboration (adaptor teams independently iterate extension messages; calculation module teams independently upgrade consumers), compatibility breaks such as field type changes, field deletions, and field number conflicts may only surface post-deployment.

#### 4.6.1 Architecture

```
Adaptor ──→ Query Schema Registry on serialize ──→ Kafka (with Schema ID)
Calculation Module ──→ Query Schema Registry on deserialize ──→ Validate then process
Query Service ──→ Query Schema Registry on deserialize ──→ Validate then build views
```

#### 4.6.2 Technical Requirements

| Requirement | Description |
|-------------|-------------|
| Schema Registry selection | Confluent Schema Registry (native Protobuf support) |
| Compatibility mode | `FULL` (checks both forward and backward compatibility); breaking changes blocked from production |
| Registration scope | All Protobuf message types used by Kafka Topics: core messages (`UnifiedPosition`) + each extension message (`ChinaPositionExt`, `IndiaPositionExt`, `OptionExt`, etc.) |
| Subject naming | `topic-name-value` (e.g., `harbour.unified.position.intraday-value`) |
| CI integration | Pipeline executes `protoc` compilation + Schema Registry compatibility pre-check (dry-run validate) at build stage; incompatible → block merge |

#### 4.6.3 Integration with YAML Extension Registry

YAML registry and Schema Registry are complementary:

| Registry | Manages | Check Timing |
|----------|---------|-------------|
| **YAML Extension Registry** | Extension Key → enum value → message type mapping | CI static scan + adaptor runtime validation |
| **Schema Registry** | Protobuf message binary field compatibility (types, field numbers, delete/add rules) | Build-time CI pre-check + producer register / consumer deserialize real-time validation |

Both registries' subjects and version info are stored uniformly, forming the platform's **unified metadata center**. The YAML `proto_message_type` field references the corresponding subject in Schema Registry.

### 4.7 Deep Dive: Core Belongs to Core, Extensions Belong to Extensions

> This section further dissects the design philosophy of the extension mechanism described above, answering "why this design", "how to prevent overstuffing the message", and "what alternatives exist".

#### 4.7.1 The Essence: Intersection vs Union of Fields

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

#### 4.7.2 Two-Layer Structure Breakdown

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

#### 4.7.3 Runtime Consumption: Isolation Principle (Core Design Essence)

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

#### 4.7.4 Four-Layer Governance Defense: Preventing Extension Slots from Becoming a Dumping Ground

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

#### 4.7.5 Complete Approach Comparison

| Approach | Verdict | Core Defect |
|----------|:-------:|-------------|
| **Flattened `optional` fields** | ❌ | Core message bloats to 30+ fields; new markets require core `.proto` changes, all downstream recompile & redeploy; field semantic ownership unclear |
| **`oneof` branch** | ❌ | Each message can only hold one market's data; cross-market aggregation module cannot process multiple markets simultaneously; new markets still require core message changes |
| **Per-market independent messages** | ❌ | Loses unification; Kafka Topics store multiple types; generic modules cannot consume; need type-check branches |
| **JSON free fields** | ❌ | Loses type safety; field typos only caught at runtime; large serialization size, poor performance; no Schema Registry compatibility validation |
| **`map<string, Any>` + governance** | ✅ | Core permanently 13 fields, extensions in independent files, type-safe, zero-intrusion core for new markets, four-layer governance prevents regression |

#### 4.7.6 Design Decision Summary

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

## 5. Query Service Layer (CQRS Read/Write Separation)

### 5.1 Background

Pure streaming architecture excels at async event processing but cannot satisfy UI, query systems, and real-time risk control's low-latency synchronous point-query needs. If each downstream consumer independently builds views from streams, it causes data inconsistency and duplicated development.

### 5.2 Architecture Pipeline

```
Calculation Module → Kafka Result Topic → Query Service (Materialized Views) → REST/gRPC Interface
```

### 5.3 Core Capabilities

- Consume result streams, build `account_id`-indexed account snapshots in a **local RocksDB** store:

| Storage | Technology | Purpose | Data Volume |
|---------|-----------|---------|:-----------:|
| Local persistent store | RocksDB | Full account snapshots; every read/write hits RocksDB directly | Unlimited |

- **Startup hydration**: on every startup, each instance replays the result Topics and rebuilds its RocksDB state from scratch before serving query traffic. To make full rebuild possible, `harbour.result.position` / `harbour.result.margin` must use `cleanup.policy=compact` with `account_id` as the message key (compaction keeps the latest snapshot per account, so replay yields exactly the current state). The readiness probe (`/health/ready`) returns 200 only after hydration completes; hydration duration is bounded by the compacted Topic size and is monitored as a startup metric.
- All writes go synchronously to RocksDB; queries read RocksDB directly (point lookup by `account_id`). Since state is always rebuildable from Kafka, instances are disposable and interchangeable — scaling or failover simply spins up a new instance and re-hydrates;
- Provide standard query interfaces: `GetPosition`, `GetMargin`;
- Response headers carry `X-Data-Last-Update-Timestamp` along with the message's Kafka `offset` and `transaction_id`, identifying data version for issue tracing;
- **Kafka Consumer isolation level**: Must configure `isolation.level=read_committed`, ensuring only calculation module transaction-committed messages are consumed, preventing dirty reads of uncommitted transaction intermediate states in snapshots;
- Data guarantees **eventual consistency**; strong-consistency scenarios connect to dedicated settlement database interfaces.

### 5.4 Value

- Unified query entry, eliminating downstream duplicated development;
- Read/write pipeline decoupling, not affecting core stream processing logic.

---

## 6. SOD & Intraday Data Merge Timing

SOD snapshots come from SOD Adaptor; Intraday increments come from real-time adaptors. Their arrival timing is unguaranteed — the calculation module must handle the following scenarios.

### 6.1 Key Timing Scenarios

| Scenario | Description | Handling Strategy |
|----------|-------------|-------------------|
| SOD arrives first, Intraday later | Normal path | SOD establishes baseline → Intraday increments accumulate |
| Intraday arrives first, SOD later | Post-open snapshot delayed | Cache Intraday → replay merge after SOD arrives; mark account as `initializing` until merge complete |
| SOD & Intraday arrive concurrently | Concurrent writes | `business_date`-based partition lock; same trading day, same account serial merge |
| SOD data correction re-send | Corrected snapshot published | SOD message carries `sod_version`, version increments; on receiving higher version, rollback that day's increment merge, rebuild baseline with new SOD |

### 6.2 Dedup Strategy

The same trade may appear in both SOD and Intraday (e.g., snapshot time window overlaps with real-time stream). The calculation module uses **`execution_id`** (see [§5.3.1](#431-core-message-definition-permanently-immutable)) for deduplication:

- On SOD load, record all `execution_id`s into the day's dedup set;
- On Intraday message arrival, check dedup set first; skip if hit;
- Dedup set is cleared and rebuilt on the next trading day's SOD arrival.

### 6.3 State Markers

Each account has three merge states per trading day, exposed via query interface:

| State | Meaning | Query Behavior |
|-------|---------|---------------|
| `initializing` | SOD not yet ready, only Intraday increments accumulated | Return interim result + state marker; caller may choose to wait |
| `ready` | SOD ready, increment merge normal | Return final result |
| `stale` | No data received beyond threshold time | Return last known result + alert marker |

---

## 7. Inter-Module Data Pipeline (Position → Margin)

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

### 7.1 Why Not a Single Module?

| Approach | Issue |
|----------|-------|
| **Single monolith** doing both position and margin | Position changes trigger full margin recalculation; coupling makes independent scaling, testing, and deployment impossible |
| **Separate modules, chained via Kafka** | Position module scales independently of margin; position results are durable on Kafka — margin can replay from any offset; a margin bug doesn't corrupt position data |

### 7.2 Data Flow Details

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

### 7.3 Ordering Guarantee

Position results for the same `account_id` arrive in order (single partition per account via `kafkaPartitionKey`). Margin Calc processes them in order, so margin is always derived from the latest position — no stale reads.

### 7.4 Failure Isolation

If Margin Calc fails:
- Position Calc continues unaffected — positions keep updating on `harbour.result.position`
- On recovery, Margin Calc replays from its last committed offset
- No data loss; margin catches up to current position state

If Position Calc fails:
- Margin Calc sees no new position messages → last known margin remains
- Accounts with no updates beyond the stale threshold are marked accordingly
- On Position Calc recovery, margin resumes automatically

### 7.5 General Pattern

The Position → Margin pipeline is one instance of a general pattern: **any calculation module can consume any result topic**. Other potential pipelines:

```
Position → Margin       (implemented)
Margin → Risk           (margin feeds risk limit checks)
Position → P&L          (position + market prices → P&L)
```

The constraints are always the same: consume `OctaneMessage` from the upstream result topic, filter by `RoutingKey`, deserialize `body` with the expected schema, compute, and publish to a new result topic.

## 8. New Market Onboarding & Code Reuse Rules

The platform maximizes reuse of generic capabilities, distinguishing reusable parts from must-add-new parts, avoiding "zero modification" misconceptions.

### 8.1 Reuse vs. New Development Breakdown

| Capability Category | Reusable? | Description |
|--------------------|:---------:|-------------|
| Common calculation framework | ✅ Fully reusable | Sub-account aggregation, SOD initialization, Intraday increment merge, generic risk framework |
| Cross-market global aggregation module | ✅ Fully reusable | All-market position/margin aggregation logic, no changes needed |
| Adaptor standardization process | ✅ Reusable pattern | Unified process: parse → clean → extension pack → wrap as OctaneMessage |
| Kafka bus & downstream interfaces | ✅ Fully reusable | Topic structure & data format globally unified; downstream needs no adaptation |
| Market-specific margin rules | ❌ Must add new | Market rules differ (SPAN, tiered, etc.); requires independent plugin/module development |
| Market-specific position semantics | ❌ Partially new | Parse corresponding extension messages; implement market-specific logic |

### 8.2 New Market Onboarding Effort Summary

Onboarding a new market requires only **3 tasks**:

1. Develop the corresponding market adaptor;
2. Implement that market's specific margin calculation plugin/module;
3. Add extension message parsing logic (if market-specific semantics exist).

> Compared to traditional architecture (each market independently develops full adaptor + calculation + interface), new market onboarding only requires adaptor + margin plugin + extension parsing. Duplicated development modules (common calculation framework, Kafka bus, downstream interfaces, global aggregation) are all reused, significantly reducing development effort (specific percentage depends on the new market's margin rule complexity; this is a qualitative estimate).

