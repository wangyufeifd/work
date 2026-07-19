# OctaneMessage — Harbour Kafka Unified Message Envelope

## Overview

`OctaneMessage` is the **one and only message format** on all Harbour Kafka topics. Every producer wraps its payload in this envelope before publishing; every consumer unwraps it to access the business data. No raw Protobuf messages ever appear on Kafka directly.

```
                         ┌──────────────────────────┐
                         │      OctaneMessage        │
                         │                          │
                         │  key (RoutingKey)        │  ← tells you WHAT is inside
                         │  body (bytes)            │  ← the actual payload
                         │  id + version            │  ← primary key + revision
                         │  kafkaPartitionKey       │  ← where it routes
                         │  timestampUtc            │  ← when it happened
                         │  recordDate              │  ← business date (YYYYMMDD)
                         │  deliveryTag             │  ← delivery tracking
                         └──────────────────────────┘
                                      │
                       ┌──────────────┼──────────────┐
                       │              │              │
                       ▼              ▼              ▼
                  AccountMap      Position      Execution/Order
```

## Field Reference

| Field | Type | Purpose |
|-------|------|---------|
| `key` | `RoutingKey` | Message type identifier — determines which Protobuf schema to use when deserializing `body` |
| `body` | `bytes` | Serialized Protobuf payload — the actual business message (AccountMap, Position, Execution, Order, etc.) |
| `id` | `string` | Combined with `key` to form the message primary key; used by data lake / HBase ingestion for upsert |
| `version` | `int64` | Monotonic revision number — higher version replaces lower for the same `id` |
| `timestampUtc` | `int64` | UTC epoch millis — when the message was produced |
| `recordDate` | `int32` | Business date in YYYYMMDD format |
| `kafkaPartitionKey` | `string` | Kafka partition routing key — controls which partition the message lands in |
| `deliveryTag` | `int64` | Delivery tracking tag for message lifecycle management |
| `activeTillDate` | `int32` | Expiry date for time-limited messages |
| `timeInForce` | `string` | Time-in-force qualifier (e.g. GTC, IOC, FOK) |

## RoutingKey → Body Type Mapping

The `key` field acts as a type discriminator. Every consumer reads `OctaneMessage`, inspects `key`, then deserializes `body` with the corresponding schema:

| RoutingKey value | body deserializes to | Typical Kafka Topic |
|------------------|---------------------|---------------------|
| `ACCOUNT_MAP` | `AccountMap` | account-related topics |
| `POSITION` | `UnifiedPosition` | `harbour.unified.position.*` |
| `EXECUTION` | `Execution` | execution/intraday topics |
| `ORDER` | `Order` | order-related topics |

> The actual `RoutingKey` enum values may differ — this table illustrates the concept. The definitive mapping is maintained in the Protobuf schema registry.

## AccountMap

`AccountMap` is a batch container carrying multiple `Account` records:

```
AccountMap {
    repeated Account accounts = 1;
}

Account {
    string accountId;
    Type accountType;          // GMI | GSAccountNumber | ExchangeAccount | ...
    string beneficialOwner;
    string investmentAdvisor;
    string gsoe;
    string subAccount;
    string branchNumber;
    string mic;                // MIC of the exchange account
    double marginMultiplier;   // margin multiplier for this account
    bool isGiveOut;
    MicFamily micFamily;
    ChinaAccountType chinaGMIAccountType;
}
```

## Design Implications for Harbour

### 1. Kafka Topics are generic

All Harbour Kafka topics carry `OctaneMessage`, not domain-specific types. The topic name hints at the expected body type, but the envelope is always the same — consumers are never coupled to a single body schema.

### 2. Adaptors produce OctaneMessage

Adaptors wrap their output (`UnifiedPosition`, `AccountMap`, etc.) into `OctaneMessage.body`, set the appropriate `key`, and publish. They never produce raw Protobuf.

### 3. Calculation modules consume OctaneMessage

Calculation modules subscribe to topics, receive `OctaneMessage`, check `key`, and deserialize `body` accordingly. If a module only handles positions, it filters to `key = POSITION` and unpacks the body as `UnifiedPosition`.

### 4. id + version enable idempotent upsert

The `(id, version)` pair serves as a universal idempotency mechanism. Downstream systems (data lake, HBase, Query Service) use `id` as the primary key and `version` to resolve conflicts — higher version wins. This is separate from the `execution_id` + `sod_version` dedup logic at the calculation module level.

### 5. kafkaPartitionKey is explicit

Partition routing is declared in the envelope rather than inferred from the body. This lets producers control sharding without requiring consumers to peek inside `body` to extract a partition key.
