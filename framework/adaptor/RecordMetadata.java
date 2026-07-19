package harbour.adaptor;

/**
 * Record metadata returned after a successful Kafka produce.
 */
public interface RecordMetadata {

    /** Kafka topic the message was written to. */
    String topic();

    /** Partition the message landed in. */
    int partition();

    /** Committed offset. */
    long offset();

    /** Wall-clock timestamp assigned by the broker. */
    long timestamp();
}
