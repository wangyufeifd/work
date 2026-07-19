package harbour.adaptor;

import harbour.common.OctaneMessage;
import harbour.common.ExtensionRegistry;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Dependency-injection context passed to {@link Adaptor#init(AdaptorContext)}.
 *
 * Provides everything an adaptor needs at runtime without hard-coding
 * infrastructure concerns.
 */
public interface AdaptorContext {

    /** The adaptor's own configuration, keyed by property name. */
    AdaptorConfig config();

    /** Publisher for sending {@code OctaneMessage} envelopes to Kafka. */
    OctaneMessagePublisher publisher();

    /** Registry that validates extension keys and types at runtime. */
    ExtensionRegistry extensionRegistry();

    /** Micrometer meter registry for metrics (throughput, latency, errors). */
    MeterRegistry metrics();

    /** Tracing helper — create spans, inject traceparent, etc. */
    Tracing tracing();

    /** Report adaptor health to the platform health endpoint. */
    void reportHealth(AdaptorHealth health);
}
