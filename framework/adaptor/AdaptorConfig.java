package harbour.adaptor;

import java.util.Map;

/**
 * Adaptor configuration, typically sourced from a YAML / properties file
 * or environment variables.  Keys are adaptor-specific; the platform
 * provides common keys (Kafka bootstrap servers, topic names, etc.).
 */
public interface AdaptorConfig {

    /** Raw string value by key. */
    String get(String key);

    /** Typed value by key. */
    <T> T get(String key, Class<T> type);

    /** All key-value pairs (for debugging / audit). */
    Map<String, String> getAll();
}
