package io.hekate.util.trace;

/**
 * Generic interface for traceable objects.
 */
public interface Traceable {
    /**
     * Returns tracing information.
     *
     * @return Tracing information.
     */
    TraceInfo traceInfo();
}
