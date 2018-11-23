package io.hekate.messaging.retry;

/**
 * Template interface for policies that can re-route operations upon retry.
 *
 * @param <P> Policy type.
 */
public interface RetryRoutingSupport<P extends RetryRoutingSupport<P>> {
    /**
     * Routing policy in case of a retry.
     *
     * @param policy Policy.
     *
     * @return This instance.
     */
    P route(RetryRoutingPolicy policy);

    /**
     * Instructs to apply the {@link RetryRoutingPolicy#RE_ROUTE} policy.
     *
     * @return This instance.
     */
    default P alwaysReRoute() {
        return route(RetryRoutingPolicy.RE_ROUTE);
    }

    /**
     * Instructs to apply the {@link RetryRoutingPolicy#RETRY_SAME_NODE} policy.
     *
     * @return This instance.
     */
    default P alwaysTrySameNode() {
        return route(RetryRoutingPolicy.RETRY_SAME_NODE);
    }

    /**
     * Instructs to apply the {@link RetryRoutingPolicy#PREFER_SAME_NODE} policy.
     *
     * @return This instance.
     */
    default P preferSameNode() {
        return route(RetryRoutingPolicy.PREFER_SAME_NODE);
    }
}
