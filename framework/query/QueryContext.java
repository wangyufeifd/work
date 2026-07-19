package harbour.query;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Dependency-injection context for the Query Service.
 */
public interface QueryContext {

    /** Query service configuration. */
    QueryConfig config();

    /**
     * The tiered cache that backs all queries.
     * Layers: Caffeine (hot) → Redis (warm) → RocksDB (cold).
     */
    CacheLayer cache();

    /**
     * Register a consumer for a result topic.
     * The consumer feeds materialized views in the cache.
     */
    ResultConsumer resultConsumer();

    /** Micrometer meter registry. */
    MeterRegistry metrics();

    /** Tracing. */
    harbour.adaptor.Tracing tracing();

    /** Report health. */
    void reportHealth(QueryHealth health);
}
