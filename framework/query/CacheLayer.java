package harbour.query;

import java.util.Optional;

/**
 * Tiered caching layer backing the Query Service.
 *
 * <h3>Tiers</h3>
 * <ol>
 *   <li><b>Hot (Caffeine)</b> — accounts updated in the last ~5 minutes.
 *       Local, < 1ms latency.</li>
 *   <li><b>Warm (Redis)</b> — accounts updated in the last ~7 days.
 *       Shared across instances, < 5ms latency.</li>
 *   <li><b>Cold (RocksDB)</b> — full history.  Local persistent storage,
 *       < 20ms latency.  Survives restarts without Kafka replay.</li>
 * </ol>
 *
 * <h3>Query flow</h3>
 * {@code get()} checks hot → warm → cold, backfilling warmer tiers on
 * a miss.  {@code put()} writes hot + warm synchronously, cold async.
 *
 * <h3>Implementation note</h3>
 * Values stored in the cache are Protobuf-serialized bytes.  The
 * query endpoints deserialize them into the response format.
 */
public interface CacheLayer {

    /**
     * Get a value by key, checking all tiers.
     *
     * @param family logical group (e.g. "position", "margin")
     * @param key    the lookup key (typically {@code account_id})
     * @return the value if found in any tier, or empty
     */
    Optional<byte[]> get(String family, String key);

    /**
     * Write a value to the cache.
     * Hot + warm written synchronously, cold async.
     */
    void put(String family, String key, byte[] value);

    /** Remove a key from all tiers. */
    void invalidate(String family, String key);

    /** Approximate number of entries in the hot tier. */
    long hotSize();

    /** Approximate number of entries in the warm tier. */
    long warmSize();

    /** Hot tier hit rate (0.0 – 1.0). */
    double hitRate();
}
