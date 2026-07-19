package harbour.calculation;

import harbour.adaptor.OctaneMessagePublisher;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Dependency-injection context for {@link CalculationService#init}.
 *
 * Provides Kafka consumers (already configured with correct topic
 * subscriptions and isolation level), a state store factory, the result
 * publisher, and infrastructure hooks.
 */
public interface CalculationContext {

    /** Configuration for this calculation service. */
    CalculationConfig config();

    /**
     * A consumer bound to the service's input topic(s).
     * The consumer already has {@code isolation.level=read_committed}
     * and appropriate group.id / auto-offset-reset settings.
     */
    MessageConsumer consumer();

    /**
     * Publisher for result messages.
     * Each calculation service writes to its own result topic.
     */
    OctaneMessagePublisher resultPublisher();

    /**
     * Create or retrieve a named state store.
     *
     * The platform manages changelog backup, compaction, and recovery.
     * The service just reads and writes key-value pairs.
     */
    <K, V> StateStore<K, V> stateStore(String name, Class<K> keyType, Class<V> valueType);

    /** Micrometer meter registry. */
    MeterRegistry metrics();

    /** Tracing. */
    harbour.adaptor.Tracing tracing();

    /** Report health. */
    void reportHealth(CalculationHealth health);
}
