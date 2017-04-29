package io.hekate.cluster;

/**
 * Marker interface for objects that have (or are) {@link ClusterNodeId}.
 */
public interface HasClusterNodeId {
    /**
     * Converts this object to {@link ClusterNodeId}.
     *
     * @return {@link ClusterNodeId}.
     */
    ClusterNodeId asNodeId();
}
