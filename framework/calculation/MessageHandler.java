package harbour.calculation;

import harbour.common.OctaneMessage;

/**
 * A handler for incoming {@link OctaneMessage} envelopes.
 *
 * <p>The platform deserializes the envelope and dispatches to the
 * handler registered for the message's {@code RoutingKey}.  The handler
 * unpacks {@code body} with the expected Protobuf schema, performs
 * business logic, and optionally returns a result message to publish.
 *
 * <h3>Contract</h3>
 * <ul>
 *   <li>Must be non-blocking and thread-safe.</li>
 *   <li>Return {@code null} or an empty Optional to indicate "no result
 *       to publish" (e.g. filtered or skipped messages).</li>
 *   <li>Throw an exception to signal a processing failure; the platform
 *       handles retry and DLQ routing.</li>
 * </ul>
 */
@FunctionalInterface
public interface MessageHandler {

    /**
     * Process one message.
     *
     * @param message the fully-deserialized {@code OctaneMessage}
     * @return a result message to publish, or empty if nothing to output
     */
    java.util.Optional<OctaneMessage> handle(OctaneMessage message) throws Exception;
}
