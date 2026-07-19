package harbour.adaptor;

import java.io.Closeable;

/**
 * Top-level contract for every Harbour adaptor.
 *
 * An adaptor is responsible for ingesting one upstream data source,
 * normalizing it, and publishing {@code OctaneMessage} envelopes to the
 * Kafka unified message bus.  Each market / data source gets its own
 * adaptor implementation.
 *
 * <h3>Lifecycle</h3>
 * <ol>
 *   <li>{@link #init(AdaptorContext)} — called once before start; wire
 *       dependencies from the context.</li>
 *   <li>{@link #start()} — begin consuming upstream data and publishing
 *       to Kafka.  Non-blocking; the adaptor runs on its own threads.</li>
 *   <li>{@link #stop()} — graceful shutdown: stop consuming, drain
 *       in-flight messages, close connections.</li>
 * </ol>
 *
 * <h3>What belongs in an adaptor (allowed)</h3>
 * <ul>
 *   <li>Parse upstream raw format (JSON, XML, binary, …)</li>
 *   <li>Field mapping and type conversion</li>
 *   <li>Data cleaning (nulls, illegal enums, out-of-bounds)</li>
 *   <li>Single-message field derivation (e.g. direction × qty → delta)</li>
 *   <li>Market-specific extension packing</li>
 * </ul>
 *
 * <h3>What does NOT belong in an adaptor (prohibited)</h3>
 * <ul>
 *   <li>Margin / P&L / risk calculation</li>
 *   <li>Cross-message or cross-account aggregation</li>
 *   <li>Sub-account hierarchy resolution</li>
 * </ul>
 */
public interface Adaptor extends Closeable {

    /** Unique identifier for this adaptor (e.g. "china-adaptor"). */
    String name();

    /**
     * Called once before {@link #start()}.
     * Use the context to obtain Kafka producers, metrics registry,
     * extension registry, and configuration.
     */
    void init(AdaptorContext context);

    /** Begin the ingestion → normalize → publish loop. */
    void start();

    /** Graceful shutdown.  May be called more than once (idempotent). */
    void stop();

    /** Health probe for liveness / readiness checks. */
    AdaptorHealth health();

    /** Convenience: stop + release resources. */
    @Override
    default void close() {
        stop();
    }
}
