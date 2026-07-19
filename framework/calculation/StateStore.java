package harbour.calculation;

import java.util.Optional;

/**
 * Key-value state store for calculation services that need to maintain
 * state across messages (e.g. SOD baselines, execution_id dedup sets,
 * account merge status).
 *
 * <p>The platform manages persistence, changelog backup, and recovery.
 * Services treat this as a local embedded map.
 *
 * <p>All operations are scoped to the current Kafka partition — a service
 * instance only sees state for the partitions it owns.
 *
 * @param <K> key type
 * @param <V> value type (must be Protobuf-serializable)
 */
public interface StateStore<K, V> {

    /** Get a value, or empty if absent. */
    Optional<V> get(K key);

    /** Put a value. */
    void put(K key, V value);

    /** Delete a key. */
    void delete(K key);

    /**
     * Execute multiple reads and writes atomically.
     * Used when a single input message requires updating several keys
     * (e.g. update position + add to dedup set).
     */
    void batch(StateBatch<K, V> batch);

    /** Approximate number of keys in this partition's store. */
    long size();

    /** Functional interface for an atomic batch of state mutations. */
    @FunctionalInterface
    interface StateBatch<K, V> {
        void execute(StateStore<K, V> store);
    }
}
