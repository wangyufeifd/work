package harbour.adaptor;

/** Adaptor health status for liveness / readiness probes. */
public enum AdaptorHealth {
    /** Not yet started. */
    INIT,
    /** Running normally. */
    HEALTHY,
    /** Running but degraded (e.g. elevated error rate). */
    DEGRADED,
    /** Stopped or failed. */
    UNHEALTHY
}
