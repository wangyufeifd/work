package harbour.query;

import harbour.common.OctaneMessage;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Consumer of result Kafka topics, feeding data into the cache layer.
 *
 * Each result topic has its own consumer.  On each incoming
 * {@code OctaneMessage}, the registered callbacks update the
 * corresponding materialized view in the cache.
 */
public interface ResultConsumer {

    /**
     * Register a callback for messages with a specific {@code RoutingKey}.
     *
     * The callback receives the full {@code OctaneMessage}; it is
     * responsible for deserializing {@code body} and calling
     * {@link CacheLayer#put} to update the view.
     */
    void on(Enum<?> routingKey, Consumer<OctaneMessage> handler);

    /** Seek to last committed offset (normal resume). */
    void seekToCommitted();

    /**
     * Current lag (number of messages behind the latest offset).
     * Used to detect if the query service is falling behind the
     * calculation modules.
     */
    long lag();
}
