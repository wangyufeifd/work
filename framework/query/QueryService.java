package harbour.query;

import java.io.Closeable;

/**
 * Top-level contract for the Harbour CQRS Query Service.
 *
 * The Query Service consumes result Kafka topics (carrying
 * {@code OctaneMessage} envelopes), builds materialized views, and
 * exposes REST / gRPC endpoints for synchronous point queries.
 *
 * <h3>Lifecycle</h3>
 * <ol>
 *   <li>{@link #init(QueryContext)} — wire cache layers, consumers, config.</li>
 *   <li>{@link #start()} — begin consuming result topics and hydrating caches.</li>
 *   <li>{@link #stop()} — graceful shutdown, persist cold storage.</li>
 * </ol>
 *
 * <h3>Read/write separation</h3>
 * The Query Service never writes to business Kafka topics.
 * It is a read-only materialized view of the calculation results.
 */
public interface QueryService extends Closeable {

    /** Unique name (e.g. "query-service"). */
    String name();

    void init(QueryContext context);

    void start();

    void stop();

    QueryHealth health();

    @Override
    default void close() {
        stop();
    }
}
