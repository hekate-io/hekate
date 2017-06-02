package io.hekate.cluster;

/**
 * Marker interface for objects that have (or are) {@link ClusterTopology}.
 */
public interface ClusterTopologySupport {
    /**
     * Returns the cluster topology.
     *
     * @return Cluster topology.
     */
    ClusterTopology topology();
}
