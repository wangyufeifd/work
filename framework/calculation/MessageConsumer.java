package harbour.calculation;

import harbour.common.OctaneMessage;

/**
 * Abstracts Kafka consumer operations so calculation services never
 * touch the raw Kafka client.
 *
 * A service declares which topics and {@code RoutingKey} values it
 * is interested in; the platform delivers matching messages.
 */
public interface MessageConsumer {

    /**
     * Register a handler for a specific {@code RoutingKey} value.
     *
     * When a message arrives with the matching key, the handler is called.
     * Multiple handlers may be registered; each message is delivered to
     * at most one handler (first match wins).
     */
    void on(Enum<?> routingKey, MessageHandler handler);

    /**
     * Seek to a specific timestamp for catch-up / replay.
     * Used when the service needs to replay historical data
     * (e.g. hydrate T-1 executions onto a SOD baseline).
     */
    void seekToTimestamp(long epochMillis);

    /**
     * Seek to the last committed offset (normal resume after restart).
     */
    void seekToCommitted();
}
