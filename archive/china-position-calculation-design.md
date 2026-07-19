# China Realtime Position Calculation — Harbour Implementation Design

> Maps the three-stage China position pipeline onto the Harbour platform architecture.

---

## 1. Overview

The China realtime position calculation has three stages:

| Stage | Input | Output |
|-------|-------|--------|
| ① Load baseline | GMI T-1 SOD position FTP file | T-1 SOD baseline (aggregated by `account + symbol + direction`) |
| ② Hydrate T-1 executions | Kafka historical execution messages (T-1) | T day SOD position (corrected baseline) |
| ③ Realtime accumulation | Kafka intraday execution messages | Realtime position |

Below is how each stage maps to Harbour components.

---

## 2. Component Mapping

```
  GMI FTP                          OSM Kafka Topic
     │                                    │
     ▼                                    ▼
┌─────────────┐                   ┌──────────────┐
│ GMI DB      │                   │ China        │
│ Adaptor     │                   │ Adaptor      │
│             │                   │              │
│ • Download  │                   │ • Parse      │
│   FTP file  │                   │   OSM msgs   │
│ • Filter    │                   │ • Map fields │
│   China     │                   │ • Wrap ext   │
│ • Aggregate │                   │ • → Proto    │
│ • → Proto   │                   │              │
└──────┬──────┘                   └──────┬───────┘
       │                                 │
       │ UnifiedPosition                 │ UnifiedPosition
       │ source=SOD                      │ source=INT
       │ business_date=T-1               │ business_date=T
       ▼                                 ▼
┌──────────────────────────────────────────────────┐
│              Kafka Unified Message Bus            │
│                                                  │
│  harbour.unified.position.sod                    │
│  harbour.unified.position.intraday               │
└──────────────────────┬───────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────┐
│       China Position Calculation Module           │
│                                                  │
│  Stage ②: Replay T-1 intraday → merge onto SOD  │
│  Stage ③: Consume T intraday → accumulate        │
│                                                  │
│  State: SOD baseline + execution_id dedup set    │
└──────────────────────┬───────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────┐
│         harbour.result.position (Kafka)           │
│         Query Service (REST/gRPC)                 │
└──────────────────────────────────────────────────┘
```

---

## 3. Stage ① — GMI DB Adaptor: Produce SOD Baseline

### 3.1 Responsibility

The GMI DB Adaptor reads the GMI T-1 SOD position FTP file and produces `UnifiedPosition` messages to the SOD topic.

### 3.2 Processing Steps

```
FTP file download
  │
  ▼
Filter: keep only China-market records
  (by MIC or venue field in GMI data)
  │
  ▼
Aggregate by key:
  GMI account + instrument symbol + long/short
  │
  ▼
Map to UnifiedPosition Proto:
  • account_id     ← GMI account
  • sub_account_id ← from GMI hierarchy (if available)
  • symbol         ← instrument symbol
  • direction      ← LONG / SHORT
  • quantity       ← aggregated position qty
  • mic            ← "XSHG" / "XSHE" etc.
  • type           ← FUTURE / OPTION
  • source         ← "GMI_DB"
  • business_date  ← T-1 (YYYYMMDD)
  • timestamp      ← file generation time
  • extensions     ← ChinaPositionExt { today_qty, sod_qty, is_covered }
  │
  ▼
Publish to: harbour.unified.position.sod
  Partition key: hash(account_id)
```

### 3.3 Key Decisions

| Decision | Rationale |
|----------|-----------|
| **Aggregation happens in the Adaptor** | The FTP file is a static snapshot with no incremental semantics — aggregating by key is data normalization, not business calculation. It's within the Adaptor's allowed scope (single-source, no cross-account logic). |
| **`business_date = T-1`** | The FTP file represents T-1 end-of-day state. The "T day SOD" is produced later by Stage ② hydrating T-1 executions onto this baseline. |
| **`source = GMI_DB`** | Distinguishes SOD messages from intraday messages, critical for the calculation module's merge logic. |
| **Use `sod_version`** | If GMI issues a corrected FTP file, increment the version. The calculation module rolls back and rebuilds on receiving a higher version. |

---

## 4. Stage ①.5 — China Adaptor: Produce Intraday Messages

### 4.1 Responsibility

The China Adaptor consumes the OSM realtime execution Kafka topic, parses each execution message, and produces `UnifiedPosition` messages to the intraday topic.

### 4.2 Processing Steps

```
OSM Kafka topic (raw execution messages)
  │
  ▼
Parse raw format → extract fields
  │
  ▼
Map to UnifiedPosition Proto:
  • execution_id   ← OSM trade ID (globally unique)
  • account_id     ← OSM account
  • symbol         ← instrument symbol
  • direction      ← BUY / SELL
  • quantity       ← trade qty (direction × qty → position delta)
  • avg_price      ← trade price
  • mic            ← "XSHG" / "XSHE"
  • type           ← FUTURE / OPTION
  • source         ← "OSM"
  • business_date  ← T (current trading day)
  • timestamp      ← trade execution time
  • extensions     ← ChinaPositionExt { today_qty, sod_qty, is_covered }
  │
  ▼
Publish to: harbour.unified.position.intraday
  Partition key: hash(account_id)
```

### 4.3 Key Decisions

| Decision | Rationale |
|----------|-----------|
| **`execution_id` is mandatory** | Enables dedup across SOD/Intraday merge — if the same trade appears in both GMI snapshot and OSM stream, the calculation module skips the duplicate. |
| **`quantity` is a position delta** | The Adaptor derives this from `direction × trade_qty` — this is single-message derivation, within allowed scope per §9. |
| **Extensions for China-specific fields** | `today_qty` (今仓), `sod_qty` (昨仓), `is_covered` (备兑) are packed as `ChinaPositionExt` — the calculation module reads them for margin computation. |

---

## 5. Stage ② + ③ — China Position Calculation Module

This is the core. One module handles both Stage ② (catch-up hydration) and Stage ③ (realtime accumulation) using the same merge logic.

### 5.1 Module Topology (Kafka Streams)

```
                    ┌─────────────────────────┐
                    │  China Position Calc     │
                    │  (Kafka Streams app)     │
                    │                          │
  SOD Topic ───────▶│  SOD Consumer            │
                    │  ↓                        │
                    │  State Store:             │
                    │  ┌─────────────────────┐ │
                    │  │ SOD baseline (per    │ │
                    │  │ account + symbol +   │ │
                    │  │ direction)           │ │
                    │  ├─────────────────────┤ │
                    │  │ execution_id dedup   │ │
                    │  │ set (per trading day)│ │
                    │  ├─────────────────────┤ │
                    │  │ merge_status         │ │
                    │  │ (per account)        │ │
                    │  └─────────────────────┘ │
                    │           ↑               │
  Intraday Topic ──▶│  Intraday Consumer        │
                    │                          │
                    │  ↓                        │
                    │  Result Producer ────────▶│ harbour.result.position
                    └─────────────────────────┘
```

### 5.2 State Store Schema

```
Key:   account_id + ":" + symbol + ":" + direction
Value: PositionState {
    // Baseline from SOD
    string sod_quantity;           // T-1 SOD position qty
    int32  sod_version;            // latest SOD version seen
    string sod_business_date;      // T-1

    // Accumulated intraday deltas
    string intraday_delta;         // sum of all intraday quantity deltas

    // Computed
    string current_position;       // sod_quantity + intraday_delta

    // Dedup
    Set<string> seen_execution_ids;  // cleared on new trading day

    // Status
    AccountMergeStatus status;     // INITIALIZING | READY | STALE
    int64 last_update_timestamp;
}
```

### 5.3 Stage ②: Hydrate T-1 Executions (Catch-up)

When the calculation module starts (or restarts), it needs to hydrate T-1 executions that occurred after the GMI FTP snapshot was taken.

```
On startup:
  1. Consume SOD topic → load T-1 SOD baseline into state store
     Status → INITIALIZING

  2. Seek intraday consumer to offset corresponding to
     GMI FTP snapshot timestamp (T-1, e.g. 15:00 CST)

  3. Replay T-1 intraday messages from that offset:
     For each message:
       a. Check execution_id against dedup set → skip if seen
       b. Apply delta: intraday_delta += quantity
       c. Add execution_id to dedup set

  4. When business_date transitions from T-1 → T:
     Stage ② complete → Status → READY
     current_position = sod_quantity + intraday_delta
     This IS the T day SOD position.

  5. Continue to Stage ③ (realtime).
```

**Why replay instead of a separate "T-1 executions" topic?**

Harbour's intraday topic retains 7 days of data. The calculation module can seek to any offset within that window. No separate historical topic is needed — the same intraday topic serves both catch-up and realtime.

**Offset management**: The GMI FTP file carries a `snapshot_timestamp` (e.g. "2026-01-14T15:00:00+08:00"). The Adaptor embeds this in the SOD message. The calculation module maps this timestamp to a Kafka offset using `KafkaConsumer.offsetsForTimes()`.

### 5.4 Stage ③: Realtime Accumulation

```
Normal operation (Status = READY):

  For each incoming intraday message:
    1. Check execution_id against dedup set → skip if seen
    2. Apply delta: intraday_delta += quantity
    3. current_position = sod_quantity + intraday_delta
    4. Add execution_id to dedup set
    5. Publish result to harbour.result.position

  On new trading day (business_date changes):
    1. New SOD arrives with T day business_date
    2. Reset: sod_quantity = new SOD quantity
    3. Reset: intraday_delta = 0
    4. Clear execution_id dedup set
    5. Status → READY
```

### 5.5 SOD Version Correction

```
On receiving SOD with higher sod_version for same (account, symbol, direction, business_date):
  1. Rollback: sod_quantity = new SOD quantity
  2. Reset: intraday_delta = 0
  3. Replay all intraday messages from the new SOD's snapshot_timestamp offset
  4. Status → READY
```

### 5.6 Aggregation Key Reconciliation

The aggregation key `GMI account + instrument symbol + long/short` must be consistent end-to-end:

| Stage | Key Components | Notes |
|-------|---------------|-------|
| GMI DB Adaptor | `account_id` + `symbol` + `direction` | Aggregates FTP rows into UnifiedPosition |
| China Adaptor | `account_id` + `symbol` + `direction` | Each message is a single execution (delta), not aggregated |
| Calculation Module | `account_id` + `symbol` + `direction` | State store key; merges SOD + intraday by this key |

The `account_id` in all three stages is the GMI account identifier. If OSM uses a different account scheme, the China Adaptor must map it to GMI account during the parse step.

---

## 6. Result Output

### 6.1 Result Topic

```
harbour.result.position.china
  Partition key: hash(account_id)
  Message: PositionResult {
      string account_id;
      string symbol;
      PositionDirection direction;
      string sod_quantity;          // T-1 baseline (or corrected T day SOD)
      string intraday_delta;        // accumulated intraday change
      string current_position;      // sod + delta
      string business_date;
      int64  last_update_timestamp;
      AccountMergeStatus status;
      map<string, Any> extensions;  // ChinaPositionExt propagated
  }
```

### 6.2 Query Service

The Query Service consumes `harbour.result.position.china` and builds a materialized view:

- **Caffeine** (hot): accounts updated in the last 5 minutes
- **Redis** (warm): accounts updated in the last 7 days
- **RocksDB** (cold): full historical snapshots

REST endpoint: `GET /v1/positions/{account_id}?symbol=&direction=`

Response includes the merge status so callers can distinguish `INITIALIZING` (catch-up in progress) from `READY` (live).

---

## 7. Edge Cases

### 7.1 GMI FTP Delayed

If the GMI T-1 SOD FTP file arrives late (after intraday trading has started):

```
Timeline:
  09:00  Trading starts, intraday messages flowing
  09:30  GMI FTP file finally arrives

Calculation module behavior:
  09:00  Status = INITIALIZING (no SOD yet)
         Intraday messages are buffered in state store
  
  09:30  SOD arrives
         Replay buffered intraday → merge → Status → READY
```

### 7.2 GMI FTP Correction (Revised SOD)

If GMI issues a corrected FTP file with higher `sod_version`:

```
  1. SOD message arrives with sod_version = 2 (current = 1)
  2. Module detects version bump
  3. Rollback: discard current intraday_delta
  4. Install new sod_quantity from corrected SOD
  5. Replay intraday messages from corrected snapshot timestamp
  6. Status → READY
```

### 7.3 Weekend / Holiday Gap

If the last trading day was Friday and today is Monday:

- The GMI FTP file carries `business_date = Friday`
- The intraday topic for Friday has execution messages
- Stage ② replays Friday's intraday onto Friday's SOD → produces Friday EOD = Monday SOD
- Monday's intraday messages then accumulate normally

This works correctly because `business_date` is explicit in every message — the module doesn't assume "yesterday = T-1".

### 7.4 Duplicate Execution Across SOD and Intraday

The GMI FTP snapshot may include trades that also appear in the OSM stream (overlapping time window). The `execution_id` dedup set catches this:

```
  SOD loads → execution_ids {A, B, C, D} added to dedup set
  Intraday message with execution_id = C arrives → skipped (already in dedup set)
```

This assumes GMI and OSM share the same execution ID namespace. If they don't, the Adaptors must normalize to a common `execution_id` scheme.

---

## 8. Summary: What Each Harbour Component Does

| Component | Responsibility | New or Reused? |
|-----------|---------------|:-------------:|
| **GMI DB Adaptor** | Download FTP, filter China, aggregate, produce SOD Proto | New (market-specific Adaptor) |
| **China Adaptor** | Parse OSM execution stream, map fields, produce intraday Proto | New (market-specific Adaptor) |
| **Kafka Topics** | `position.sod` + `position.intraday` — standard Harbour topics | **Reused** |
| **China Position Calc Module** | Stage ② catch-up + Stage ③ realtime merge | New (market-specific calc plugin) |
| **Result Topic** | `harbour.result.position.china` | New (standard naming convention) |
| **Query Service** | Caffeine/Redis/RocksDB materialized view | **Reused** |
| **Schema Registry** | Proto compatibility validation | **Reused** |
| **DLQ** | Failed message routing | **Reused** |
| **Observability** | OpenTelemetry tracing + Prometheus metrics | **Reused** |

The only new code is two Adaptors + one calculation module. Everything else — Kafka topology, Query Service, Schema Registry, DLQ, observability — is reused from the Harbour platform.

---

> **Related documents**: `overall-design.md` (platform architecture), `calculation-module-architecture.md` (calc module class design), `margin-module-architecture.md` (margin module design).
