package harbour.adaptor;

import harbour.common.OctaneMessage;
import java.util.concurrent.CompletableFuture;

/**
 * Publishing contract for sending {@link OctaneMessage} envelopes to Kafka.
 *
 * All adaptors and calculation services publish through this interface —
 * never via raw Kafka producer APIs directly.  This keeps the platform
 * in control of serialization, partitioning, retries, and metrics.
 */
public interface OctaneMessagePublisher {

    /**
     * Publish a single message asynchronously.
     *
     * @param message the fully-built {@code OctaneMessage} envelope
     * @return future that completes when Kafka acknowledges the produce
     */
    CompletableFuture<RecordMetadata> publish(OctaneMessage message);

    /** Flush any buffered messages. */
    void flush();
}
