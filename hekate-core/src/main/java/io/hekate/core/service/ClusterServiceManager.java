package io.hekate.core.service;

/**
 * Cluster service manager.
 */
public interface ClusterServiceManager extends Service {
    /**
     * Asynchronously starts joining the cluster.
     */
    void joinAsync();
}
