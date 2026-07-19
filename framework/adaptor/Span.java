package harbour.adaptor;

import java.io.Closeable;

/** A single trace span — close to finish. */
public interface Span extends Closeable {

    /** Set a string attribute (tag). */
    Span setAttribute(String key, String value);

    /** Record an exception on this span. */
    Span recordException(Throwable t);

    /** Mark the span as errored. */
    Span setError(boolean error);

    /** Finish the span. */
    @Override
    void close();
}
