package harbour.adaptor;

/**
 * Thin tracing abstraction so adaptors can create spans without
 * depending on a specific OpenTelemetry version.
 */
public interface Tracing {

    /** Start a new span as a child of the current context. */
    Span startSpan(String name);

    /** Extract trace context from upstream message headers/carriers. */
    Span extract(Object carrier);
}
