# Harbour Adaptor 内部 Class 架构设计

| 属性 | 值 |
|------|-----|
| **文档版本** | V1.0 |
| **适用范围** | 开发、架构评审、新市场适配器开发指南 |
| **前置阅读** | `最新的文档.md` 第 3.2 节（适配器标准化层）、第 5 章（统一数据模型）、第 9 章（适配器层边界与操作规范） |

---

## 目录

1. [设计驱动约束](#1-设计驱动约束)
2. [上游数据源类型总览](#2-上游数据源类型总览)
3. [整体 Class 架构图](#3-整体-class-架构图)
4. [核心接口与数据载体](#4-核心接口与数据载体)
5. [SourceConnector — 数据源接入层](#5-sourceconnector--数据源接入层)
6. [MessageParser — 格式解析层](#6-messageparser--格式解析层)
7. [FieldMapper + Validator — 字段映射与清洗层](#7-fieldmapper--validator--字段映射与清洗层)
8. [DerivationEngine — 基础字段推导层](#8-derivationengine--基础字段推导层)
9. [ExtensionPacker — 扩展封装层](#9-extensionpacker--扩展封装层)
10. [ProtoBuilder — 消息构建层](#10-protobuilder--消息构建层)
11. [ProtoPublisher — Kafka 输出层](#11-protopublisher--kafka-输出层)
12. [AbstractAdaptorPipeline — 模板方法骨架](#12-abstractadaptorpipeline--模板方法骨架)
13. [市场具体实现示例](#13-市场具体实现示例)
14. [支撑服务 Class](#14-支撑服务-class)
15. [Execution → Position Delta 职责分界](#15-execution--position-delta-职责分界)
16. [Class 清单与复用度汇总](#16-class-清单与复用度汇总)

---

## 1. 设计驱动约束

以下约束来自 `最新的文档.md` 第 3.2 节及第 9 章，适配器内部架构必须严格遵守：

| 约束 | 来源 |
|------|------|
| **允许的操作**：解析、清洗、字段映射、基础推导、扩展封装 → 输出 Proto | 文档 3.2.3 |
| **禁止的操作**：保证金/盈亏/风险计算、跨记录聚合、子账户层级推导 | 文档 3.2.4 |
| **强制使用 `ExtensionKey` 枚举**，禁止魔法字符串 | 文档 5.3.4 |
| **启动时加载 `extensions_registry.yaml`**，运行时校验扩展合法性 | 文档 5.5 |
| **接入 Schema Registry**，构建时 `FULL` 兼容性预检 | 文档 5.6 |
| **幂等键由适配器维护**：`source:account_id:business_date:sequence` | 文档 8.1 |
| **W3C traceparent + OpenTelemetry Span** 全链路追踪 | 文档 15.1 |
| **按 `account_id` 分区发布到 Kafka** | 文档 3.3 |

---

## 2. 上游数据源类型总览

适配器需要对接的上游数据源分为以下四大类：

| 类型 | 传输方式 | 数据格式 | 典型市场 | 读取模式 |
|------|---------|---------|---------|---------|
| **Kafka 实时流** | Kafka Topic | Protobuf / JSON | 中国 Genie | 持续消费 (Consumer Group) |
| **HTTP 接口** | REST/HTTP Poll | JSON / XML | 印度 Precision, 韩国 Koscom | 定时轮询 (5s~60s) |
| **FTP / SFTP 文件** | FTP/SFTP 服务器 | CSV / Plain Text | 部分 legacy 上游 | 定时扫描 + 增量下载 |
| **SQL 数据库** | JDBC 直连 | 关系表 | GMI DB (SOD 快照) | 定时查询 / CDC |

### 2.1 FTP/SFTP 文件数据源详解

```
┌─────────────────────────────────────────────────────┐
│              FTP/SFTP 文件数据源                      │
│                                                     │
│  远程 FTP Server                                     │
│  ┌──────────────────────────────────────┐           │
│  │  /data/positions/                     │           │
│  │  ├── 20260115_positions.csv           │           │
│  │  ├── 20260115_margins.csv             │           │
│  │  └── 20260116_positions.csv           │           │
│  └──────────────────────────────────────┘           │
│         │                                           │
│         │ SFTP (username/password 或 key-based)     │
│         ▼                                           │
│  ┌──────────────────────────────────────┐           │
│  │  FtpSourceConnector                   │           │
│  │  • 定时扫描 (cron: */5 * * * *)       │           │
│  │  • 文件名正则匹配 + 日期过滤          │           │
│  │  • 增量下载（记录 lastProcessedFile） │           │
│  │  • 下载后归档/删除（可配置）           │           │
│  └──────────────────────────────────────┘           │
│         │                                           │
│         ▼                                           │
│  ┌──────────────────────────────────────┐           │
│  │  CsvParser / PlainTextParser           │           │
│  │  • 分隔符可配置（, \t | 等）           │           │
│  │  • Header 行映射 / 位置索引映射         │           │
│  │  • 编码自动检测（UTF-8/GBK/ISO-8859-1）│           │
│  └──────────────────────────────────────┘           │
└─────────────────────────────────────────────────────┘
```

**CSV 文件接入示例**：

```csv
# 20260115_positions.csv (Header 行 + 数据行)
trade_id,account_id,symbol,side,qty,price,trade_time,trade_date
T001,A01,IF2403,BUY,2,3612.0,2026-01-15T09:31:05,20260115
T002,A02,SC2403,SELL,5,512.8,2026-01-15T09:32:10,20260115
```

### 2.2 SQL 数据库数据源详解

```
┌─────────────────────────────────────────────────────┐
│              SQL 数据库数据源                         │
│                                                     │
│  GMI Database (Oracle / PostgreSQL / MySQL)         │
│  ┌──────────────────────────────────────┐           │
│  │  Tables:                              │           │
│  │  • sod_positions (日初持仓快照)        │           │
│  │  • sod_margins   (日初保证金快照)      │           │
│  │  • account_hierarchy (账户层级关系)    │           │
│  └──────────────────────────────────────┘           │
│         │                                           │
│         │ JDBC Connection Pool (HikariCP)           │
│         ▼                                           │
│  ┌──────────────────────────────────────┐           │
│  │  SqlSourceConnector                   │           │
│  │  • 定时查询 (cron: 0 5 0 * * ?)       │           │
│  │  • 分页读取 (cursor-based)            │           │
│  │  • 增量读取 (WHERE update_time > ?)   │           │
│  │  • CDC 可选 (Debezium for real-time)  │           │
│  └──────────────────────────────────────┘           │
│         │                                           │
│         ▼                                           │
│  ┌──────────────────────────────────────┐           │
│  │  ResultSetMapper (将 JDBC Row →       │           │
│  │  ParsedRecord)                        │           │
│  │  • 列名映射                            │           │
│  │  • JDBC 类型 → Harbour 统一类型        │           │
│  └──────────────────────────────────────┘           │
└─────────────────────────────────────────────────────┘
```

**SQL 查询示例**：

```sql
-- GMI SOD 快照读取
SELECT
    sod_id          AS execution_id,
    investor_id     AS account_id,
    sub_inv_id      AS sub_account_id,
    instrument_code AS symbol,
    position_qty    AS quantity,
    avg_cost        AS avg_price,
    pos_direction   AS direction,
    trade_date      AS business_date
FROM sod_positions
WHERE trade_date = ?
ORDER BY sod_id
LIMIT ? OFFSET ?
```

### 2.3 数据源特性对比

| 特性 | Kafka | HTTP | FTP/SFTP | SQL |
|------|:-----:|:----:|:--------:|:---:|
| 延迟 | 实时 (<100ms) | 准实时 (5~60s) | 分钟级 | 分钟级 ~ 实时(CDC) |
| 顺序保证 | 分区内有序 | 无序 | 文件名有序 | 查询时指定 ORDER |
| 断点恢复 | offset | 无（需幂等）| lastProcessedFile | cursor |
| 数据量 | 持续大流量 | 中等 | 批量文件 | 批量查询 |
| 连接管理 | Consumer Group | HTTP Pool | SFTP Session Pool | JDBC Pool |
| 故障处理 | rebalance | 重试 + 退避 | 重试 + 跳过坏文件 | 重试 + 断点续查 |

---

## 3. 整体 Class 架构图

```
                    ┌──────────────────────────────────────────┐
                    │      AbstractAdaptorPipeline              │  ← 模板方法骨架（所有市场复用）
                    │  + start(): void                          │
                    │  # processOne(RawMessage): Mono<Void>      │
                    │  - parse()                                │
                    │  - clean() + map()                        │
                    │  - derive()                               │
                    │  - packExtensions()                       │
                    │  - buildProto()                           │
                    │  - publish()                              │
                    │  ~ onBeforeParse(RawMessage): void        │
                    │  ~ onAfterPublish(UnifiedPosition): void  │
                    └──────────┬───────────────────────────────┘
                               │ extends
          ┌────────────────────┼────────────────────┐
          │                    │                    │
          ▼                    ▼                    ▼
   ChinaAdaptor          IndiaAdaptor          KoreaAdaptor  ...
   (仅覆写市场差异部分)

         各 Adaptor 组合以下组件（依赖注入）：
          ┌─────────────────────────────────────────────┐
          │                                             │
          ▼                ▼               ▼            ▼
   ┌──────────┐   ┌──────────────┐  ┌──────────┐  ┌──────────┐
   │Source    │   │FieldMapper   │  │Extension │  │Publisher │
   │Connector │   │ + Validator  │  │Packer    │  │          │
   └──────────┘   └──────────────┘  └──────────┘  └──────────┘
          │                │               │            │
          ▼                ▼               ▼            ▼
   ┌──────────┐   ┌──────────────┐  ┌──────────┐  ┌──────────┐
   │Kafka     │   │MappingConfig │  │Extension │  │Kafka     │
   │Consumer  │   │(YAML/JSON)   │  │Registry  │  │Producer  │
   │HTTP Poll │   │              │  │          │  │          │
   │FTP Scan  │   └──────────────┘  └──────────┘  └──────────┘
   │SQL Query │
   └──────────┘

   管线数据流（6 阶段）：
   RawMessage ──①──▶ ParsedRecord ──②──▶ MappedRecord
                                             │
                                       ③ derive()
                                             ▼
                                       DerivedFields
                                             │
                          ┌──────────────────┘
                          │ ④ packExtensions()
                          ▼
                   UnifiedPosition (Proto)
                          │
                   ⑤ Schema Registry Validate
                          │
                   ⑥ publish() → Kafka
```

---

## 4. 核心接口与数据载体

### 4.1 数据载体（管线各阶段的 DTO）

```java
// 阶段入口：原始消息（从 SourceConnector 产出）
public class RawMessage {
    private String source;                  // 适配器标识，如 "CHINA_GENIE"
    private byte[] payload;                 // 原始字节数组
    private Map<String, String> headers;    // traceparent、文件名、分区信息等元数据
    private long ingestTimestamp;           // 接入时间戳 (epoch millis)

    // FTP/SFTP 特有字段
    private String filePath;                // 来源文件路径（如 /data/20260115_positions.csv）
    private long fileLineNumber;            // 文件行号（便于错误定位）
}

// 阶段①产出：解析后（字段仍是源系统名称）
public class ParsedRecord {
    private String source;
    private Map<String, Object> fields;     // 原始字段名 → 原始值
    private List<String> parseWarnings;     // 解析警告（非致命，如未知枚举值、精度截断）
}

// 阶段②产出：映射后（字段已是 Harbour 统一名称 + 清洗通过）
public class MappedRecord {
    private Map<String, Object> unifiedFields;  // 统一字段名 → 已清洗值
    private List<String> warnings;
    private String source;

    public String getString(String field) { ... }
    public Long getLong(String field) { ... }
    public Boolean getBoolean(String field) { ... }
    public <T> T get(String field, Class<T> type) { ... }
    public boolean has(String field) { ... }
    public String optString(String field, String default) { ... }
}

// 阶段③产出：推导字段
public class DerivedFields {
    private Map<String, Object> fields;

    public void put(String key, Object value) { ... }
    public void applyTo(UnifiedPosition.Builder builder) {
        // 将推导字段注入 Proto Builder（不做业务计算）
    }
}
```

### 4.2 核心接口契约

```java
// ─── 数据源接入 ───
public interface SourceConnector {
    void start();
    void stop();
    Flux<RawMessage> consume();     // Reactive 流，统一背压
    SourceType getType();           // KAFKA / HTTP_POLL / FTP_SFTP / SQL_QUERY
}

// ─── 格式解析 ───
public interface MessageParser<T extends RawMessage> {
    ParsedRecord parse(T rawMessage) throws ParseException;
    FormatType supportedFormat();   // PROTOBUF / JSON / XML / CSV / PLAIN_TEXT / JDBC_RESULTSET
}

// ─── 字段映射 ───
public interface FieldMapper {
    MappedRecord map(ParsedRecord parsed) throws MappingException;
}

// ─── 字段校验 ───
public interface FieldValidator {
    ValidationResult validate(MappedRecord record);
}

// ─── 基础推导规则 ───
public interface DerivationRule {
    boolean appliesTo(MappedRecord record);
    void apply(MappedRecord record, DerivedFields derived);
}

// ─── 扩展封装 ───
public interface ExtensionPacker {
    void pack(MappedRecord record, DerivedFields derived, UnifiedPosition.Builder builder);
}

// ─── 校验结果 ───
public class ValidationResult {
    public static ValidationResult ok() { ... }
    public static ValidationResult warn(String msg) { ... }
    public static ValidationResult fatal(String msg) { ... }
    public boolean isValid() { ... }
    public Severity getSeverity() { ... }   // WARN / FATAL
    public String getMessage() { ... }
}

public enum Severity { WARN, FATAL }
```

---

## 5. SourceConnector — 数据源接入层

### 5.1 Kafka 实时流连接器

```java
public class KafkaSourceConnector implements SourceConnector {

    private final KafkaConsumer<String, byte[]> consumer;
    private final String topic;
    private final Sinks.Many<RawMessage> sink;  // Reactor Sink

    public KafkaSourceConnector(String bootstrapServers, String topic, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", false);  // 手动 commit
        this.consumer = new KafkaConsumer<>(props);
        this.topic = topic;
        this.sink = Sinks.many().multicast().onBackpressureBuffer();
    }

    @Override
    public Flux<RawMessage> consume() {
        consumer.subscribe(List.of(topic));
        return sink.asFlux().doOnSubscribe(s -> startPollLoop());
    }

    private void startPollLoop() {
        Thread.ofVirtual().start(() -> {  // Java 21+ Virtual Thread
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, byte[]> r : records) {
                    RawMessage raw = new RawMessage();
                    raw.setSource(extractSource(r));
                    raw.setPayload(r.value());
                    raw.setHeaders(extractHeaders(r));
                    raw.setIngestTimestamp(System.currentTimeMillis());
                    sink.tryEmitNext(raw);
                }
                consumer.commitSync();
            }
        });
    }

    @Override
    public SourceType getType() { return SourceType.KAFKA; }
}
```

### 5.2 HTTP 轮询连接器

```java
public class HttpPollSourceConnector implements SourceConnector {

    private final WebClient webClient;
    private final String url;
    private final Duration interval;
    private final Sinks.Many<RawMessage> sink;

    public HttpPollSourceConnector(String url, Duration interval) {
        this.webClient = WebClient.builder()
            .codecs(config -> config.defaultCodecs().maxInMemorySize(16 * 1024 * 1024))  // 16MB
            .build();
        this.url = url;
        this.interval = interval;
        this.sink = Sinks.many().multicast().onBackpressureBuffer();
    }

    @Override
    public Flux<RawMessage> consume() {
        return Flux.interval(interval)
            .flatMap(tick -> webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(byte[].class)
                .map(body -> {
                    RawMessage raw = new RawMessage();
                    raw.setSource("HTTP_POLL:" + url);
                    raw.setPayload(body);
                    raw.setIngestTimestamp(System.currentTimeMillis());
                    return raw;
                })
                .doOnError(e -> log.error("HTTP poll failed: {}", url, e))
                .onErrorResume(e -> Mono.empty())  // 失败不中断轮询
            );
    }

    @Override
    public SourceType getType() { return SourceType.HTTP_POLL; }
}
```

### 5.3 FTP/SFTP 文件扫描连接器

```java
public class FtpSourceConnector implements SourceConnector {

    private final SftpClient sftpClient;                 // SSHJ 或 JSch
    private final FtpConfig config;
    private final Sinks.Many<RawMessage> sink;
    private final Set<String> processedFiles = ConcurrentHashMap.newKeySet();

    public FtpSourceConnector(FtpConfig config) {
        this.config = config;
        this.sftpClient = createSftpClient(config);
        this.sink = Sinks.many().multicast().onBackpressureBuffer();
    }

    @Override
    public Flux<RawMessage> consume() {
        return Flux.interval(config.getScanInterval())   // 如每 5 分钟
            .flatMap(tick -> scanAndDownload())
            .flatMap(this::readFile);
    }

    private Flux<String> scanAndDownload() {
        return Mono.fromCallable(() -> {
            List<String> newFiles = new ArrayList<>();
            for (SftpClient.DirEntry entry : sftpClient.ls(config.getRemoteDir())) {
                String filename = entry.getFilename();
                // 文件名正则匹配 + 排除已处理
                if (config.getFilePattern().matcher(filename).matches()
                    && processedFiles.add(filename)) {
                    String localPath = config.getLocalDir() + "/" + filename;
                    sftpClient.download(config.getRemoteDir() + "/" + filename, localPath);
                    newFiles.add(localPath);
                }
            }
            return newFiles;
        }).flatMapMany(Flux::fromIterable);
    }

    private Flux<RawMessage> readFile(String localPath) {
        return Flux.using(
            () -> new BufferedReader(new InputStreamReader(
                new FileInputStream(localPath), detectCharset(localPath))),
            reader -> Flux.fromStream(reader.lines())
                .skip(config.isHasHeader() ? 1 : 0)      // 跳过 header 行
                .filter(line -> !line.trim().isEmpty())   // 跳过空行
                .map(line -> {
                    RawMessage raw = new RawMessage();
                    raw.setSource("FTP:" + config.getName());
                    raw.setPayload(line.getBytes(StandardCharsets.UTF_8));
                    raw.setFilePath(localPath);
                    raw.setFileLineNumber(/* ... */);
                    raw.setIngestTimestamp(System.currentTimeMillis());
                    return raw;
                }),
            reader -> { /* close */ }
        );
    }

    private Charset detectCharset(String path) throws IOException {
        // 读取 BOM 或前几个字节检测编码（UTF-8 BOM / GBK / ISO-8859-1）
        byte[] bom = new byte[3];
        try (FileInputStream fis = new FileInputStream(path)) {
            fis.read(bom);
        }
        if (bom[0] == (byte) 0xEF && bom[1] == (byte) 0xBB && bom[2] == (byte) 0xBF) {
            return StandardCharsets.UTF_8;
        }
        // 默认回退到配置项
        return Charset.forName(config.getDefaultCharset());
    }

    @Override
    public SourceType getType() { return SourceType.FTP_SFTP; }

    // 配置对象
    @Data
    public static class FtpConfig {
        private String host;
        private int port = 22;
        private String username;
        private String password;              // 或 privateKeyPath
        private String remoteDir;
        private String localDir;
        private Pattern filePattern;          // 如 Pattern.compile("\\d{8}_positions\\.csv")
        private Duration scanInterval;        // 如 Duration.ofMinutes(5)
        private boolean hasHeader = true;     // CSV 是否有 header 行
        private String defaultCharset = "UTF-8";
    }
}
```

### 5.4 SQL 数据库查询连接器

```java
public class SqlSourceConnector implements SourceConnector {

    private final HikariDataSource dataSource;
    private final String query;
    private final int pageSize;
    private final CronSchedule schedule;
    private final Sinks.Many<RawMessage> sink;

    public SqlSourceConnector(SqlConfig config) {
        this.dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(config.getJdbcUrl());
        dataSource.setUsername(config.getUsername());
        dataSource.setPassword(config.getPassword());
        dataSource.setMaximumPoolSize(5);
        this.query = config.getQuery();
        this.pageSize = config.getPageSize();
        this.sink = Sinks.many().multicast().onBackpressureBuffer();
    }

    @Override
    public Flux<RawMessage> consume() {
        return Flux.interval(schedule.initialDelay(), schedule.period())
            .flatMap(tick -> executePagedQuery());
    }

    private Flux<RawMessage> executePagedQuery() {
        return Flux.create(sink -> {
            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

                int offset = 0;
                while (true) {
                    try (PreparedStatement stmt = conn.prepareStatement(
                        query + " LIMIT ? OFFSET ?")) {
                        stmt.setInt(1, pageSize);
                        stmt.setInt(2, offset);
                        try (ResultSet rs = stmt.executeQuery()) {
                            if (!rs.next()) break;  // 无更多数据
                            do {
                                RawMessage raw = new RawMessage();
                                raw.setSource("SQL:" + config.getName());
                                raw.setPayload(serializeRow(rs));  // → 列名:值的字节数组
                                raw.setIngestTimestamp(System.currentTimeMillis());
                                sink.next(raw);
                            } while (rs.next());
                        }
                    }
                    offset += pageSize;
                }
                sink.complete();
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    private byte[] serializeRow(ResultSet rs) throws SQLException {
        // 将 ResultSet 当前行转为 JSON 字节数组（便于 Parser 统一处理）
        ResultSetMetaData meta = rs.getMetaData();
        JSONObject row = new JSONObject();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            row.put(meta.getColumnName(i), rs.getObject(i));
        }
        return row.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public SourceType getType() { return SourceType.SQL_QUERY; }

    @Data
    public static class SqlConfig {
        private String name;                // 如 "GMI_DB"
        private String jdbcUrl;
        private String username;
        private String password;
        private String query;
        private int pageSize = 1000;
        private long initialDelaySec = 0;
        private long periodSec = 300;       // 每 5 分钟查询一次（SOD 场景）
    }
}
```

---

## 6. MessageParser — 格式解析层

```java
// ─── JSON Parser ───
public class JsonMessageParser implements MessageParser<RawMessage> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public ParsedRecord parse(RawMessage raw) throws ParseException {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = mapper.readValue(raw.getPayload(), Map.class);
            ParsedRecord record = new ParsedRecord();
            record.setSource(raw.getSource());
            record.setFields(map);
            return record;
        } catch (IOException e) {
            throw new ParseException("JSON parse error: " + e.getMessage(), e);
        }
    }

    @Override
    public FormatType supportedFormat() { return FormatType.JSON; }
}

// ─── XML Parser ───
public class XmlMessageParser implements MessageParser<RawMessage> {

    private final XmlMapper mapper = new XmlMapper();

    @Override
    public ParsedRecord parse(RawMessage raw) throws ParseException {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = mapper.readValue(raw.getPayload(), Map.class);
            ParsedRecord record = new ParsedRecord();
            record.setSource(raw.getSource());
            record.setFields(flattenXmlMap(map));  // 嵌套 XML → 扁平键值对
            return record;
        } catch (IOException e) {
            throw new ParseException("XML parse error: " + e.getMessage(), e);
        }
    }

    // 将嵌套 XML Map 拍平为 "parent.child" 键
    private Map<String, Object> flattenXmlMap(Map<String, Object> nested) { ... }

    @Override
    public FormatType supportedFormat() { return FormatType.XML; }
}

// ─── CSV Parser ───
public class CsvMessageParser implements MessageParser<RawMessage> {

    private final CsvConfig config;

    public CsvMessageParser(CsvConfig config) {
        this.config = config;
    }

    @Override
    public ParsedRecord parse(RawMessage raw) throws ParseException {
        String line = new String(raw.getPayload(), StandardCharsets.UTF_8);
        String[] values = line.split(config.getDelimiter(), -1);  // -1 保留尾部空字段

        if (values.length != config.getColumns().size()) {
            throw new ParseException(String.format(
                "Column count mismatch: expected %d, got %d at %s:%d",
                config.getColumns().size(), values.length,
                raw.getFilePath(), raw.getFileLineNumber()
            ));
        }

        Map<String, Object> fields = new LinkedHashMap<>();
        for (int i = 0; i < config.getColumns().size(); i++) {
            fields.put(config.getColumns().get(i), values[i].trim());
        }

        ParsedRecord record = new ParsedRecord();
        record.setSource(raw.getSource());
        record.setFields(fields);
        return record;
    }

    @Override
    public FormatType supportedFormat() { return FormatType.CSV; }

    @Data
    public static class CsvConfig {
        private String delimiter = ",";       // 分隔符：, \t | 等
        private List<String> columns;         // 列名列表（按顺序）
        // 如果 CSV 有 header 行，columns 可以配置为 header 映射后的统一名称
    }
}

// ─── Plain Text (固定宽度) Parser ───
public class FixedWidthParser implements MessageParser<RawMessage> {

    private final List<FieldDef> fieldDefs;  // 每个字段的起始位置和长度

    @Override
    public ParsedRecord parse(RawMessage raw) throws ParseException {
        String line = new String(raw.getPayload(), StandardCharsets.UTF_8);
        Map<String, Object> fields = new LinkedHashMap<>();

        for (FieldDef def : fieldDefs) {
            String value = line.substring(def.getStart(), def.getEnd()).trim();
            fields.put(def.getName(), value);
        }

        ParsedRecord record = new ParsedRecord();
        record.setSource(raw.getSource());
        record.setFields(fields);
        return record;
    }

    @Override
    public FormatType supportedFormat() { return FormatType.PLAIN_TEXT; }

    @Data
    public static class FieldDef {
        private String name;
        private int start;   // 起始位置 (0-indexed)
        private int length;  // 字段长度
        // getEnd() = start + length
    }
}

// ─── Protobuf Parser（上游已是 Proto） ───
public class ProtoMessageParser implements MessageParser<RawMessage> {

    @Override
    public ParsedRecord parse(RawMessage raw) throws ParseException {
        // 将 Proto 反序列化后提取字段为 Map
        // ...
    }

    @Override
    public FormatType supportedFormat() { return FormatType.PROTOBUF; }
}
```

---

## 7. FieldMapper + Validator — 字段映射与清洗层

### 7.1 抽象基类

```java
public abstract class AbstractFieldMapper implements FieldMapper {

    protected final MappingConfig config;              // 该市场的 YAML 映射配置
    protected final List<FieldValidator> validators;   // 校验器链

    @Override
    public MappedRecord map(ParsedRecord parsed) throws MappingException {
        MappedRecord record = new MappedRecord();
        record.setSource(parsed.getSource());

        // ① 字段名映射 + 类型转换
        for (FieldMapping mapping : config.getFieldMappings()) {
            Object rawValue = parsed.getFields().get(mapping.getSourceField());
            if (rawValue == null) {
                if (mapping.isRequired()) {
                    throw new MappingException("Required field missing: " + mapping.getSourceField());
                }
                continue;
            }
            Object mappedValue = mapping.getTransformer().transform(rawValue);
            record.getUnifiedFields().put(mapping.getTargetField(), mappedValue);
        }

        // ② 清洗校验
        for (FieldValidator validator : validators) {
            ValidationResult result = validator.validate(record);
            if (!result.isValid()) {
                if (result.getSeverity() == Severity.FATAL) {
                    throw new MappingException(result.getMessage());
                }
                record.getWarnings().add(result.getMessage());  // 非致命继续
            }
        }

        return record;
    }
}
```

### 7.2 YAML 映射配置文件

```yaml
# mappings/china_field_mapping.yaml
source: CHINA_GENIE
field_mappings:
  - source: trade_id
    target: execution_id
    transformer: passthrough
    required: true
  - source: investor_id
    target: account_id
    transformer: passthrough
    required: true
  - source: sub_inv_id
    target: sub_account_id
    transformer: passthrough
    required: false
  - source: instrument_code
    target: symbol
    transformer: passthrough
    required: true
  - source: market_code
    target: mic
    transformer: mic_lookup           # "CFFEX" → "XCFE" (ISO MIC)
    required: false
  - source: side                     # "B" → LONG, "S" → SHORT
    target: direction
    transformer: enum_map
    enum_values: { "B": "LONG", "S": "SHORT" }
    required: true
  - source: trade_qty
    target: quantity
    transformer: decimal_string       # 统一转字符串，避免浮点精度
    required: true
  - source: trade_price
    target: avg_price
    transformer: decimal_string
    required: true
  - source: trade_time
    target: timestamp
    transformer: epoch_millis         # ISO_8601 → epoch millis
    required: true
  - source: trade_date
    target: business_date
    transformer: date_format          # 确保 YYYYMMDD
    required: true
  - source: product_type             # "FUT" / "OPT"
    target: type
    transformer: enum_map
    enum_values: { "FUT": "FUTURE", "OPT": "OPTION" }
    required: true
  # 中国特有字段（不映射到核心，留给 ExtensionPacker 处理）
  - source: open_qty         # 今仓
    target: ext.today_qty
    transformer: decimal_string
    required: false
  - source: prev_qty         # 昨仓
    target: ext.sod_qty
    transformer: decimal_string
    required: false
  - source: covered_flag     # 备兑标记
    target: ext.is_covered
    transformer: boolean_flag
    required: false
```

### 7.3 Transformer 注册表

```java
public interface ValueTransformer {
    Object transform(Object rawValue);
}

// 内置 Transformer
public class PassthroughTransformer implements ValueTransformer { ... }
public class DecimalStringTransformer implements ValueTransformer { ... }
public class EnumMapTransformer implements ValueTransformer {
    // 从配置的 enum_values 中查找映射
}
public class EpochMillisTransformer implements ValueTransformer {
    // 支持多种时间格式 → epoch millis
}
public class DateFormatTransformer implements ValueTransformer {
    // 确保输出 YYYYMMDD
}
public class MicLookupTransformer implements ValueTransformer {
    // "CFFEX" → "XCFE" (ISO MIC 标准)
}
public class BooleanFlagTransformer implements ValueTransformer {
    // "Y"/"N", "1"/"0", "true"/"false" → Boolean
}
```

### 7.4 Validator 实现

```java
// 必填字段校验
public class RequiredFieldValidator implements FieldValidator {
    private final List<String> requiredFields;

    @Override
    public ValidationResult validate(MappedRecord record) {
        for (String field : requiredFields) {
            if (!record.has(field) || record.getString(field) == null) {
                return ValidationResult.fatal("Missing required field: " + field);
            }
        }
        return ValidationResult.ok();
    }
}

// 枚举值范围校验
public class EnumRangeValidator<T extends Enum<T>> implements FieldValidator {
    private final String fieldName;
    private final Set<String> allowedValues;

    public EnumRangeValidator(String fieldName, T[] enumValues) {
        this.fieldName = fieldName;
        this.allowedValues = Arrays.stream(enumValues)
            .map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    public ValidationResult validate(MappedRecord record) {
        String value = record.getString(fieldName);
        if (value != null && !allowedValues.contains(value)) {
            return ValidationResult.warn(
                String.format("Field '%s' has unknown enum value: '%s', allowed: %s",
                    fieldName, value, allowedValues));
        }
        return ValidationResult.ok();
    }
}

// 数值范围校验
public class NumericRangeValidator implements FieldValidator {
    private final String fieldName;
    private final BigDecimal min;
    private final BigDecimal max;

    @Override
    public ValidationResult validate(MappedRecord record) {
        String value = record.getString(fieldName);
        if (value != null) {
            BigDecimal num = new BigDecimal(value);
            if (min != null && num.compareTo(min) < 0) {
                return ValidationResult.fatal(String.format(
                    "Field '%s' value %s < min %s", fieldName, num, min));
            }
            if (max != null && num.compareTo(max) > 0) {
                return ValidationResult.fatal(String.format(
                    "Field '%s' value %s > max %s", fieldName, num, max));
            }
        }
        return ValidationResult.ok();
    }
}

// 时间戳合理性校验
public class TimestampReasonablenessValidator implements FieldValidator {
    private final String fieldName;
    private final long maxFutureMs = Duration.ofMinutes(5).toMillis();  // 最多容忍未来 5 分钟
    private final long maxPastMs = Duration.ofDays(7).toMillis();       // 最多容忍过去 7 天

    @Override
    public ValidationResult validate(MappedRecord record) {
        Long ts = record.getLong(fieldName);
        if (ts != null) {
            long now = System.currentTimeMillis();
            if (ts > now + maxFutureMs) {
                return ValidationResult.warn("Timestamp is too far in the future: " + ts);
            }
            if (ts < now - maxPastMs) {
                return ValidationResult.warn("Timestamp is too old: " + ts);
            }
        }
        return ValidationResult.ok();
    }
}
```

### 7.5 市场具体映射实现

```java
// 中国市场
public class ChinaFieldMapper extends AbstractFieldMapper {
    public ChinaFieldMapper() {
        super(
            MappingConfig.load("mappings/china_field_mapping.yaml")
        );
        this.validators = List.of(
            new RequiredFieldValidator(List.of("execution_id", "account_id", "symbol", "direction", "quantity")),
            new EnumRangeValidator<>("direction", PositionDirection.values()),
            new EnumRangeValidator<>("type", PositionType.values()),
            new NumericRangeValidator("quantity", BigDecimal.ZERO, null),  // >= 0
            new TimestampReasonablenessValidator("timestamp")
        );
    }
}

// 印度市场
public class IndiaFieldMapper extends AbstractFieldMapper {
    public IndiaFieldMapper() {
        super(MappingConfig.load("mappings/india_field_mapping.yaml"));
        this.validators = List.of(
            new RequiredFieldValidator(List.of("execution_id", "account_id", "symbol", "direction", "quantity")),
            new EnumRangeValidator<>("direction", PositionDirection.values()),
            new NumericRangeValidator("quantity", BigDecimal.ZERO, null)
        );
    }
}
```

---

## 8. DerivationEngine — 基础字段推导层

```java
public class DerivationEngine {

    private final List<DerivationRule> rules;  // 注入的市场专属规则列表

    public DerivationEngine(List<DerivationRule> rules) {
        this.rules = rules;
    }

    public DerivedFields derive(MappedRecord record) {
        DerivedFields derived = new DerivedFields();

        // ─── 通用推导（所有市场） ───

        // ① direction × quantity → position_delta
        if (record.has("direction") && record.has("quantity")) {
            PositionDirection dir = record.get("direction", PositionDirection.class);
            BigDecimal qty = new BigDecimal(record.getString("quantity"));
            derived.put("position_delta",
                dir == PositionDirection.LONG ? qty : qty.negate());
        }

        // ② 推导 mic（如果源数据缺失）
        if (!record.has("mic") && record.has("symbol")) {
            derived.put("mic", inferMicFromSymbol(record.getString("symbol")));
        }

        // ─── 市场专属推导（注入规则） ───
        for (DerivationRule rule : rules) {
            if (rule.appliesTo(record)) {
                rule.apply(record, derived);
            }
        }

        return derived;
    }

    // 从 symbol 推断 MIC
    private String inferMicFromSymbol(String symbol) {
        // 基于合约代码前缀查表
        if (symbol.startsWith("IF") || symbol.startsWith("IC") || symbol.startsWith("IM")) {
            return "XCFE";  // 中金所
        }
        if (symbol.startsWith("SC") || symbol.startsWith("FU")) {
            return "XSGE";  // 上期所
        }
        return "UNKNOWN";
    }
}
```

**市场专属推导规则示例**：

```java
// 中国市场：推导 venue（同一 MIC 下的子场所）
public class ChinaVenueDerivationRule implements DerivationRule {
    @Override
    public boolean appliesTo(MappedRecord record) {
        return record.getSource().contains("CHINA");
    }
    @Override
    public void apply(MappedRecord record, DerivedFields derived) {
        String symbol = record.getString("symbol");
        if (symbol.startsWith("IF") || symbol.startsWith("IC")) {
            derived.put("venue", "CFFEX");
        } else if (symbol.startsWith("SC")) {
            derived.put("venue", "INE");  // 上海国际能源交易中心
        } else {
            derived.put("venue", "UNKNOWN");
        }
    }
}
```

---

## 9. ExtensionPacker — 扩展封装层

```java
public class ChinaExtensionPacker implements ExtensionPacker {

    private final ExtensionRegistry registry;  // 启动时加载 YAML 注册表

    @Override
    public void pack(MappedRecord record, DerivedFields derived, UnifiedPosition.Builder builder) {
        ChinaPositionExt.Builder extBuilder = ChinaPositionExt.newBuilder();

        if (record.has("ext.today_qty")) {
            extBuilder.setTodayQty(record.getString("ext.today_qty"));
        }
        if (record.has("ext.sod_qty")) {
            extBuilder.setSodQty(record.getString("ext.sod_qty"));
        }
        if (record.has("ext.is_covered")) {
            extBuilder.setIsCovered(record.getBoolean("ext.is_covered"));
        }

        ChinaPositionExt ext = extBuilder.build();
        Any any = Any.pack(ext);

        // 防线④：运行时注册表校验
        registry.validate(ExtensionKey.EXT_CHINA_POSITION, any);

        // 防线①：枚举 Key 而非魔法字符串
        builder.putExtensions(ExtensionKey.EXT_CHINA_POSITION.name(), any);
    }
}
```

---

## 10. ProtoBuilder — 消息构建层

```java
public class ProtoBuilder {

    private final SchemaRegistryClient schemaClient;
    private final List<ExtensionPacker> extensionPackers;  // 一个市场可能有多个扩展
    private final String source;                            // 适配器标识

    public ProtoBuilder(List<ExtensionPacker> packers, String source,
                        SchemaRegistryClient schemaClient) {
        this.extensionPackers = packers;
        this.source = source;
        this.schemaClient = schemaClient;
    }

    public UnifiedPosition build(MappedRecord record, DerivedFields derived) {
        UnifiedPosition.Builder builder = UnifiedPosition.newBuilder()
            .setExecutionId(record.getString("execution_id"))
            .setAccountId(record.getString("account_id"))
            .setSubAccountId(record.optString("sub_account_id", ""))
            .setSymbol(record.getString("symbol"))
            .setMic(record.optString("mic", ""))
            .setVenue(record.optString("venue", ""))
            .setDirection(record.get("direction", PositionDirection.class))
            .setQuantity(record.getString("quantity"))
            .setAvgPrice(record.getString("avg_price"))
            .setTimestamp(record.getLong("timestamp"))
            .setBusinessDate(record.getString("business_date"))
            .setType(record.get("type", PositionType.class))
            .setSource(source);

        // 注入推导字段
        if (derived != null) {
            derived.applyTo(builder);
        }

        // 逐一执行扩展封装
        for (ExtensionPacker packer : extensionPackers) {
            packer.pack(record, derived, builder);
        }

        UnifiedPosition proto = builder.build();

        // 防线③：构建时 Schema Registry 兼容性校验
        schemaClient.validate("harbour.unified.position.intraday-value", proto);

        return proto;
    }
}
```

---

## 11. ProtoPublisher — Kafka 输出层

```java
public class ProtoPublisher {

    private final KafkaProducer<String, byte[]> producer;
    private final SequenceManager sequenceManager;
    private final SchemaRegistryClient schemaClient;
    private final MetricsCollector metrics;

    public void publish(UnifiedPosition proto, TracingContext tracing) {
        // ① 构造幂等键：source:account_id:business_date:sequence
        String idempotencyKey = IdempotencyKey.of(
            proto.getSource(),
            proto.getAccountId(),
            proto.getBusinessDate(),
            sequenceManager.next(proto.getSource(), proto.getAccountId(), proto.getBusinessDate())
        );

        // ② 选择正确的 Topic
        String topic = selectTopic(proto);

        // ③ 构造 ProducerRecord
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(
            topic,
            proto.getAccountId(),                    // 按 account_id 分区
            proto.toByteArray()
        );
        record.headers().add("traceparent", tracing.getTraceParentBytes());
        record.headers().add("idempotency_key", idempotencyKey.getBytes());
        record.headers().add("schema_id", String.valueOf(
            schemaClient.getLatestSchemaId(topic + "-value")).getBytes());

        // ④ 发送 + 指标采集
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                metrics.incrementError();
                metrics.getDlqRouter().route(proto, exception);  // 失败 → DLQ
            } else {
                metrics.incrementPublished();
                metrics.recordLatency(
                    System.currentTimeMillis() - proto.getTimestamp());
            }
        });
    }

    private String selectTopic(UnifiedPosition proto) {
        // 按数据类型 + 时间维度选择 Topic
        // 实际实现需接入消息类型判断（sod vs intraday, position vs margin）
        return "harbour.unified.position.intraday";  // 简化示例
    }
}
```

---

## 12. AbstractAdaptorPipeline — 模板方法骨架

```java
public abstract class AbstractAdaptorPipeline {

    // ─── 依赖注入的组件 ───
    protected final SourceConnector sourceConnector;
    protected final MessageParser<?> parser;
    protected final AbstractFieldMapper fieldMapper;
    protected final DerivationEngine derivationEngine;
    protected final ProtoBuilder protoBuilder;
    protected final ProtoPublisher publisher;

    // ─── 可观测性 ───
    protected final Tracer tracer;

    // ─── 模板方法：子类不覆写 ───
    public final void start() {
        sourceConnector.consume()
            .flatMap(this::processOne, 4)    // 4 路并行处理
            .doOnError(this::handleError)
            .doOnComplete(this::handleComplete)
            .subscribe();
    }

    public final void stop() {
        sourceConnector.stop();
        publisher.close();
    }

    // ─── 单条消息处理管线（6 阶段） ───
    private Mono<Void> processOne(RawMessage raw) {
        Span span = tracer.spanBuilder("adaptor.process")
            .setParent(Context.current().with(
                W3CTraceContextPropagator.getInstance()
                    .extract(Context.current(), raw, new RawMessageGetter())))
            .startSpan();

        try (Scope scope = span.makeCurrent()) {
            // ① 前置钩子
            onBeforeParse(raw);

            // ② 解析
            ParsedRecord parsed = parser.parse(raw);
            span.addEvent("parsed");

            // ③ 字段映射 + 清洗 + 校验
            MappedRecord mapped = fieldMapper.map(parsed);
            span.addEvent("mapped",
                Attributes.of(AttributeKey.stringKey("warnings"),
                    String.valueOf(mapped.getWarnings().size())));

            // ④ 基础推导
            DerivedFields derived = derivationEngine.derive(mapped);
            span.addEvent("derived");

            // ⑤ 构建 Proto（含扩展封装 + Schema Registry 校验）
            UnifiedPosition proto = protoBuilder.build(mapped, derived);
            span.addEvent("proto_built",
                Attributes.of(AttributeKey.stringKey("extensions_count"),
                    String.valueOf(proto.getExtensionsCount())));

            // ⑥ 发布到 Kafka
            publisher.publish(proto, TracingContext.fromSpan(span));
            span.addEvent("published");

            // ⑦ 后置钩子
            onAfterPublish(proto);

            span.setStatus(StatusCode.OK);
            return Mono.empty();

        } catch (ParseException | MappingException e) {
            span.recordException(e).setStatus(StatusCode.ERROR);
            return Mono.error(new AdaptorPipelineException(raw, e, Stage.PARSE_OR_MAP));
        } catch (Exception e) {
            span.recordException(e).setStatus(StatusCode.ERROR);
            return Mono.error(new AdaptorPipelineException(raw, e, Stage.BUILD_OR_PUBLISH));
        } finally {
            span.end();
        }
    }

    // ─── 错误处理 ───
    private void handleError(Throwable error) {
        log.error("[{}] Pipeline error", adaptorName(), error);
        metrics.incrementPipelineError();
    }

    // ─── 钩子方法：子类可选覆写 ───
    protected void onBeforeParse(RawMessage raw) {}           // 前置处理
    protected void onAfterPublish(UnifiedPosition proto) {}   // 后置处理（如上报对账信息）
    protected abstract String adaptorName();                  // 适配器标识
}
```

---

## 13. 市场具体实现示例

### 13.1 中国市场 Adaptor（Kafka 流）

```java
public class ChinaAdaptor extends AbstractAdaptorPipeline {

    public ChinaAdaptor(ChinaAdaptorConfig config) {
        this.sourceConnector = new KafkaSourceConnector(
            config.getBootstrapServers(), "genie.position.realtime", "harbour-china-adaptor");
        this.parser = new ProtoMessageParser();     // Genie 上游已是 Proto
        this.fieldMapper = new ChinaFieldMapper();
        this.derivationEngine = new DerivationEngine(List.of(
            new ChinaVenueDerivationRule()
        ));
        this.protoBuilder = new ProtoBuilder(List.of(
            new ChinaExtensionPacker(ExtensionRegistry.getInstance()),
            new OptionExtensionPacker(ExtensionRegistry.getInstance())
        ), "CHINA_GENIE", config.getSchemaRegistryClient());
        this.publisher = new ProtoPublisher(/* ... */);
    }

    @Override
    protected String adaptorName() { return "Harbour-China-Adaptor"; }
}
```

### 13.2 印度市场 Adaptor（HTTP 轮询）

```java
public class IndiaAdaptor extends AbstractAdaptorPipeline {

    public IndiaAdaptor(IndiaAdaptorConfig config) {
        this.sourceConnector = new HttpPollSourceConnector(
            config.getPrecisionUrl(), Duration.ofSeconds(5));
        this.parser = new JsonMessageParser();
        this.fieldMapper = new IndiaFieldMapper();
        this.derivationEngine = new DerivationEngine(List.of());  // 印度无特殊推导
        this.protoBuilder = new ProtoBuilder(List.of(
            new IndiaExtensionPacker(ExtensionRegistry.getInstance())
        ), "INDIA_PRECISION", config.getSchemaRegistryClient());
        this.publisher = new ProtoPublisher(/* ... */);
    }

    @Override
    protected String adaptorName() { return "Harbour-India-Adaptor"; }
}
```

### 13.3 某 Legacy 市场 Adaptor（FTP CSV 文件）

```java
public class LegacyFtpAdaptor extends AbstractAdaptorPipeline {

    public LegacyFtpAdaptor(LegacyFtpConfig config) {
        // FTP/SFTP + CSV 组合
        FtpSourceConnector.FtpConfig ftpConfig = new FtpSourceConnector.FtpConfig();
        ftpConfig.setHost(config.getFtpHost());
        ftpConfig.setRemoteDir("/data/daily/");
        ftpConfig.setFilePattern(Pattern.compile("\\d{8}_positions\\.csv"));
        ftpConfig.setScanInterval(Duration.ofMinutes(5));
        ftpConfig.setHasHeader(true);

        this.sourceConnector = new FtpSourceConnector(ftpConfig);

        CsvMessageParser.CsvConfig csvConfig = new CsvMessageParser.CsvConfig();
        csvConfig.setDelimiter(",");
        csvConfig.setColumns(List.of(
            "trade_id", "account", "instrument", "side", "qty", "price", "time", "date"
        ));
        this.parser = new CsvMessageParser(csvConfig);
        this.fieldMapper = new LegacyFieldMapper();
        this.derivationEngine = new DerivationEngine(List.of());
        this.protoBuilder = new ProtoBuilder(List.of(),
            "LEGACY_FTP", config.getSchemaRegistryClient());
        this.publisher = new ProtoPublisher(/* ... */);
    }

    @Override
    protected String adaptorName() { return "Harbour-Legacy-FTP-Adaptor"; }
}
```

### 13.4 GMI 数据库 Adaptor（SQL SOD 快照）

```java
public class GmiDbAdaptor extends AbstractAdaptorPipeline {

    public GmiDbAdaptor(GmiDbConfig config) {
        SqlSourceConnector.SqlConfig sqlConfig = new SqlSourceConnector.SqlConfig();
        sqlConfig.setName("GMI_DB");
        sqlConfig.setJdbcUrl(config.getJdbcUrl());
        sqlConfig.setUsername(config.getDbUser());
        sqlConfig.setPassword(config.getDbPassword());
        sqlConfig.setQuery("""
            SELECT sod_id, investor_id, sub_inv_id, instrument_code,
                   position_qty, avg_cost, pos_direction, trade_date
            FROM sod_positions
            WHERE trade_date = ?
            """);
        sqlConfig.setPageSize(1000);
        sqlConfig.setPeriodSec(300);  // 每 5 分钟

        this.sourceConnector = new SqlSourceConnector(sqlConfig);
        this.parser = new JsonMessageParser();  // ResultSet → JSON → ParsedRecord
        this.fieldMapper = new GmiDbFieldMapper();
        this.derivationEngine = new DerivationEngine(List.of());
        this.protoBuilder = new ProtoBuilder(List.of(),
            "GMI_DB", config.getSchemaRegistryClient());
        this.publisher = new ProtoPublisher(/* ... */);
    }

    @Override
    protected String adaptorName() { return "Harbour-GMI-DB-Adaptor"; }
}
```

---

## 14. 支撑服务 Class

```java
// ─── SequenceManager：幂等序列号管理 ───
public class SequenceManager {
    // 每 (source, account_id, business_date) 自增
    private final ConcurrentHashMap<String, AtomicLong> sequences = new ConcurrentHashMap<>();

    public long next(String source, String accountId, String businessDate) {
        String key = source + ":" + accountId + ":" + businessDate;
        return sequences.computeIfAbsent(key, k -> new AtomicLong(0))
            .incrementAndGet();
    }

    @Scheduled(cron = "0 5 0 * * ?")
    public void purgePreviousDay() {
        String today = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);
        sequences.keySet().removeIf(key -> !key.endsWith(today));
    }
}

// ─── ExtensionRegistry：扩展注册表运行时校验 ───
public class ExtensionRegistry {
    private final Map<String, ExtensionEntry> registry;

    public static ExtensionRegistry loadFromYaml(String path) {
        // 解析 extensions_registry.yaml
        Map<String, ExtensionEntry> map = new HashMap<>();
        // ... YAML 解析逻辑
        return new ExtensionRegistry(map);
    }

    public void validate(ExtensionKey key, Any value) {
        ExtensionEntry entry = registry.get(key.name());
        if (entry == null) {
            throw new IllegalExtensionException("未注册的扩展 Key: " + key);
        }
        if (!value.getTypeUrl().contains(entry.protoMessageType)) {
            throw new IllegalExtensionException(
                "类型不匹配: expected " + entry.protoMessageType
                + ", got " + value.getTypeUrl());
        }
    }

    public boolean isRegistered(ExtensionKey key) {
        return registry.containsKey(key.name());
    }

    public boolean matchesType(ExtensionKey key, Any value) {
        ExtensionEntry entry = registry.get(key.name());
        return entry != null && value.getTypeUrl().contains(entry.protoMessageType);
    }

    @Data
    public static class ExtensionEntry {
        private String enumKey;           // "EXT_CHINA_POSITION"
        private String protoMessageType;  // "harbour.ext.ChinaPositionExt"
    }
}

// ─── IdempotencyKey：幂等键构造 ───
public class IdempotencyKey {
    public static String of(String source, String accountId,
                            String businessDate, long sequence) {
        return source + ":" + accountId + ":" + businessDate + ":" + sequence;
    }
}

// ─── TracingContext：OpenTelemetry 工具 ───
public class TracingContext {
    private final byte[] traceParent;

    public static TracingContext fromSpan(Span span) {
        // W3C Trace Context 格式提取
        // version-traceid-parentid-traceflags
        return new TracingContext(/* ... */);
    }

    public byte[] getTraceParentBytes() { return traceParent; }
}

// ─── MappingConfig：YAML 映射配置加载 ───
public class MappingConfig {
    private String source;
    private List<FieldMapping> fieldMappings;

    public static MappingConfig load(String path) {
        // 使用 SnakeYAML 或 Jackson YAML 解析
        // ...
    }
}

@Data
public class FieldMapping {
    private String sourceField;      // 源字段名
    private String targetField;      // 目标字段名（Harbour 统一名称）
    private String transformer;      // Transformer 名称（从 TransformerRegistry 查找）
    private boolean required = false;
    private Map<String, String> enumValues;  // enum_map 专用：{"B":"LONG","S":"SHORT"}
}
```

---

## 15. Execution → Position Delta 职责分界

### 15.1 问题场景

如果上游有一个 Kafka Topic 是交易系统发出的 Execution（成交）消息：

```json
{
  "trade_id": "T20260115-00001",
  "account_id": "A01",
  "symbol": "IF2403",
  "side": "BUY",
  "qty": 2,
  "price": 3612.0,
  "trade_time": "2026-01-15T09:31:05"
}
```

应该由谁将 Execution 转换为 Position？Adaptor 还是计算模块？

### 15.2 答案：Adaptor 做单条推导，计算模块做聚合

```
  交易系统 Execution Topic
         │
         ▼
  ┌──────────────────────────────────────────────┐
  │  Adaptor（单条消息、无状态）                    │
  │                                              │
  │  Execution → UnifiedPosition (per message)   │
  │  • side=BUY → direction=LONG                 │
  │  • qty=2, direction=LONG → position_delta=+2 │
  │  • 字段映射 + 清洗 + 扩展封装                   │
  │                                              │
  │  ❌ 不做：累加今天所有 execution 得到总持仓      │
  │  ❌ 不做：保证金计算                            │
  │  ❌ 不做：跨账户聚合                            │
  └──────────────────────┬───────────────────────┘
                         │
                         │ Kafka: position.intraday
                         ▼
  ┌──────────────────────────────────────────────┐
  │  计算模块（多条消息、有状态）                    │
  │                                              │
  │  ① SOD 基线到达 → 建立当日起始持仓             │
  │  ② 逐条消费 intraday UnifiedPosition         │
  │  ③ SOD + Σ(intraday.position_delta) → 总持仓  │
  │  ④ 总持仓 × 保证金规则 → 保证金                │
  │  ⑤ 子账户聚合 + 跨市场汇总                     │
  └──────────────────────────────────────────────┘
```

### 15.3 职责边界参照表

| 操作 | Adaptor | 计算模块 | 依据 |
|------|:------:|:------:|------|
| 字段映射 (side → direction) | ✅ | ❌ | 白名单：字段映射 |
| direction × qty → position_delta | ✅ | ❌ | 白名单：从成交推导基础字段 |
| 今仓/昨仓封装为扩展 | ✅ | ❌ | 白名单：扩展封装 |
| 今日所有 position_delta 累加 | ❌ | ✅ | 红线：跨记录聚合 |
| SOD 基线 + Intraday 合并 | ❌ | ✅ | 红线：需要状态管理 |
| 保证金规则计算 | ❌ | ✅ | 红线：业务规则计算 |
| 子账户层级聚合 | ❌ | ✅ | 红线：子账户层级推导 |
| 跨市场汇总 | ❌ | ✅ | 红线：需要多市场数据 |

### 15.4 设计原理

- **Adaptor = 无状态管道**：每条消息独立处理，不保留任何「今天已经处理了多少」的状态。可以随时重启、扩容、缩容而不丢失状态。如果 Adaptor 做了累加，故障重启会导致累加值丢失或重复。
- **计算模块 = 有状态引擎**：通过 Changelog Topic（文档 8.3）持久化状态，故障恢复时从 Changelog 秒级重建，Exactly-Once 语义保证累加正确。

---

## 16. Class 清单与复用度汇总

### 16.1 完整清单

| 层级 | Class / Interface | 复用度 | 新市场需新建? |
|------|------------------|:------:|:------------:|
| **接口** | `SourceConnector` | 100% | ❌ |
| **接口** | `MessageParser<T>` | 100% | ❌ |
| **接口** | `FieldMapper` | 100% | ❌ |
| **接口** | `ExtensionPacker` | 100% | ❌ |
| **接口** | `FieldValidator` | 100% | ❌ |
| **接口** | `DerivationRule` | 100% | ❌ |
| **接口** | `ValueTransformer` | 100% | ❌ |
| **骨架** | `AbstractAdaptorPipeline` | 100% | ❌ |
| **骨架** | `AbstractFieldMapper` | 100% | ❌ |
| **通用** | `DerivationEngine` | 100% | ❌ |
| **通用** | `ProtoBuilder` | 100% | ❌ |
| **通用** | `ProtoPublisher` | 100% | ❌ |
| **通用** | `SequenceManager` | 100% | ❌ |
| **通用** | `ExtensionRegistry` | 100% | ❌ |
| **通用** | `SchemaRegistryClient` | 100% | ❌ |
| **通用** | `AdaptorMetrics` | 100% | ❌ |
| **通用** | `TracingContext` | 100% | ❌ |
| **通用** | `IdempotencyKey` | 100% | ❌ |
| **通用** | `MappingConfig` | 100% | ❌ |
| **Connector** | `KafkaSourceConnector` | 100% | ❌ |
| **Connector** | `HttpPollSourceConnector` | 100% | ❌ |
| **Connector** | `FtpSourceConnector` | 100% | ❌ |
| **Connector** | `SqlSourceConnector` | 100% | ❌ |
| **Parser** | `JsonMessageParser` | 100% | ❌ |
| **Parser** | `XmlMessageParser` | 100% | ❌ |
| **Parser** | `CsvMessageParser` | 100% | ❌ |
| **Parser** | `FixedWidthParser` | 100% | ❌ |
| **Parser** | `ProtoMessageParser` | 100% | ❌ |
| **Validator** | `RequiredFieldValidator` | 100% | ❌ |
| **Validator** | `EnumRangeValidator` | 100% | ❌ |
| **Validator** | `NumericRangeValidator` | 100% | ❌ |
| **Validator** | `TimestampReasonablenessValidator` | 100% | ❌ |
| **Transformer** | `PassthroughTransformer` | 100% | ❌ |
| **Transformer** | `DecimalStringTransformer` | 100% | ❌ |
| **Transformer** | `EnumMapTransformer` | 100% | ❌ |
| **Transformer** | `EpochMillisTransformer` | 100% | ❌ |
| **Transformer** | `DateFormatTransformer` | 100% | ❌ |
| **Transformer** | `MicLookupTransformer` | 100% | ❌ |
| **Transformer** | `BooleanFlagTransformer` | 100% | ❌ |
| **数据载体** | `RawMessage` | 100% | ❌ |
| **数据载体** | `ParsedRecord` | 100% | ❌ |
| **数据载体** | `MappedRecord` | 100% | ❌ |
| **数据载体** | `DerivedFields` | 100% | ❌ |
| **数据载体** | `ValidationResult` | 100% | ❌ |
| **YAML 配置** | `{market}_field_mapping.yaml` | 0% | ✅ 新建 |
| **市场专属** | `{Market}Adaptor` | 0% | ✅ 新建（组装类） |
| **市场专属** | `{Market}FieldMapper` | 0% | ✅ 新建 |
| **市场专属** | `{Market}ExtensionPacker` | 0% | ✅ 新建（如有市场特有语义） |
| **市场专属** | `{Market}DerivationRule` | 0% | ✅ 新建（如有特殊推导） |

### 16.2 新市场接入工作量

新市场接入时，**需要新建的 class 只有 4-5 个** + 1 个 YAML 配置：

| 产出物 | 数量 | 工作量 |
|--------|:----:|--------|
| `{Market}Adaptor`（组装类，20 行） | 1 | 极小 |
| `{Market}FieldMapper`（继承基类，15 行 + YAML） | 1 | 小（主要是填写 YAML） |
| `{market}_field_mapping.yaml`（字段映射配置） | 1 | 主要工作 |
| `{Market}ExtensionPacker`（如有特有语义） | 0-1 | 小 |
| `{Market}DerivationRule`（如有特殊推导） | 0-1 | 极小 |
| **总计** | **4-5 个文件** | **1-2 天** |

> 其余 44 个 class / interface / connector / parser / validator / transformer **全部复用，零修改**。

---

> **关联文档**：`最新的文档.md` — 完整架构设计 V1.3  
> **更新日期**：2026-01-15
