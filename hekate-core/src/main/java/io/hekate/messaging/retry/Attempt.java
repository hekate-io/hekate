package io.hekate.messaging.retry;

import io.hekate.cluster.ClusterNode;
import java.util.Set;

/**
 * Attempt.
 */
public interface Attempt {
    /**
     * Returns the current attempts (starting with zero for the first attempt).
     *
     * @return Attempts count.
     */
    int attempt();

    /**
     * Returns the last tried node.
     *
     * @return Last tried node.
     */
    ClusterNode lastTriedNode();

    /**
     * Returns an immutable set of all tried nodes.
     *
     * @return Immutable set of all tried nodes.
     */
    Set<ClusterNode> allTriedNodes();

    /**
     * Returns {@code true} if the specified node is in the {@link #allTriedNodes()} set.
     *
     * @param node Node to check.
     *
     * @return {@code true} if the specified node is in the {@link #allTriedNodes()} set.
     */
    default boolean hasTriedNode(ClusterNode node) {
        return allTriedNodes().contains(node);
    }

    /**
     * Returns {@code true} if this is the first attempt ({@link #attempt()} == 0).
     *
     * @return {@code true} if this is the first attempt.
     */
    default boolean isFirstAttempt() {
        return attempt() == 0;
    }
}
