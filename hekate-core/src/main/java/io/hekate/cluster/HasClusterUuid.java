package io.hekate.cluster;

/**
 * Marker interface for objects that have (or are) {@link ClusterUuid}.
 */
public interface HasClusterUuid {
    /**
     * Converts this object to {@link ClusterUuid}.
     *
     * @return {@link ClusterUuid}.
     */
    ClusterUuid asClusterUuid();
}
