package harbour.calculation;

import java.util.Map;

/** Configuration for a calculation service. */
public interface CalculationConfig {

    String get(String key);

    <T> T get(String key, Class<T> type);

    Map<String, String> getAll();
}
