package harbour.calculation;

import java.io.Closeable;

/**
 * Top-level contract for every Harbour calculation service.
 *
 * A calculation service subscribes to one or more Kafka topics (carrying
 * {@code OctaneMessage} envelopes), executes business logic (position merge,
 * margin computation, aggregation, …), and publishes results to result
 * Kafka topics.
 *
 * <h3>Lifecycle</h3>
 * <ol>
 *   <li>{@link #init(CalculationContext)} — obtain consumers, producers,
 *       state store, and configuration from the context.</li>
 *   <li>{@link #start()} — begin the consume → compute → publish loop.
 *       Non-blocking.</li>
 *   <li>{@link #stop()} — graceful shutdown: commit offsets, drain
 *       in-flight messages, close state stores.</li>
 * </ol>
 *
 * <h3>Key design constraints</h3>
 * <ul>
 *   <li>Calculation services never talk to upstream data sources directly —
 *       they only consume Kafka topics.</li>
 *   <li>Inter-service communication happens exclusively via Kafka result
 *       topics (e.g. Position Calc → position topic → Margin Calc).</li>
 *   <li>Stateful processing MUST use the provided {@link StateStore}
 *       abstraction so the platform can manage changelog backup and
 *       recovery.</li>
 * </ul>
 */
public interface CalculationService extends Closeable {

    /** Unique identifier (e.g. "china-position-calc"). */
    String name();

    /**
     * Called once before {@link #start()}.
     * Wire Kafka consumers, state stores, result publishers, and config.
     */
    void init(CalculationContext context);

    /** Begin consuming and processing. */
    void start();

    /** Graceful shutdown. Idempotent. */
    void stop();

    /** Health probe. */
    CalculationHealth health();

    @Override
    default void close() {
        stop();
    }
}
