package io.hekate.cluster.event;

import io.hekate.cluster.split.SplitBrainDetector;
import io.hekate.core.Hekate;

/**
 * Reason of a {@link ClusterLeaveEvent}.
 *
 * @see ClusterLeaveEvent#reason()
 */
public enum ClusterLeaveReason {
    /** Leaving because of the {@link Hekate#leave()} method call. */
    LEAVE,

    /** Leaving because of the {@link Hekate#terminate()} method call. */
    TERMINATE,

    /** Leaving because of the cluster split-brain had been detected by the {@link SplitBrainDetector}. */
    SPLIT_BRAIN
}
