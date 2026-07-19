package harbour.query;

import java.util.Map;

/** Query service configuration. */
public interface QueryConfig {

    String get(String key);

    <T> T get(String key, Class<T> type);

    Map<String, String> getAll();
}
