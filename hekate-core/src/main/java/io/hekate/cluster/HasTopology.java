package io.hekate.cluster;

/**
 * Marker interface for objects that have (or are) {@link ClusterTopology}.
 */
public interface HasTopology {
    /**
     * Returns the cluster topology.
     *
     * @return Cluster topology.
     */
    ClusterTopology topology();
}
