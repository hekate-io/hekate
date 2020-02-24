/*
 * Copyright 2020 The Hekate Project
 *
 * The Hekate Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.hekate.cluster.seed.etcd;

import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for {@link EtcdSeedNodeProvider}.
 *
 * @see EtcdSeedNodeProvider#EtcdSeedNodeProvider(EtcdSeedNodeProviderConfig)
 */
public class EtcdSeedNodeProviderConfig {
    /** Default value (={@value}) for {@link #setCleanupInterval(int)}. */
    public static final int DEFAULT_CLEANUP_INTERVAL = 60 * 1000;

    /** Default value (={@value}) for {@link #setBasePath(String)}. */
    public static final String DEFAULT_BASE_PATH = "/hekate/cluster";

    /** See {@link #setEndpoints(List)}. */
    private List<String> endpoints;

    /** See {@link #setUsername(String)}. */
    private String username;

    /** See {@link #setPassword(String)}. */
    @ToStringIgnore
    private String password;

    /** See {@link #setBasePath(String)}. */
    private String basePath = DEFAULT_BASE_PATH;

    /** See {@link #setCleanupInterval(int)}. */
    private int cleanupInterval = DEFAULT_CLEANUP_INTERVAL;

    /**
     * Returns the list of Etcd endpoint addresses (see {@link #setEndpoints(List)}).
     *
     * @return List of Etcd endpoints.
     */
    public List<String> getEndpoints() {
        return endpoints;
    }

    /**
     * Sets the list of Etcd endpoint addresses. Each address must be a valid string representation of {@link URI}.
     *
     * <p>
     * This parameter is mandatory and requires at least one Etcd endpoint address to be specified.
     * </p>
     *
     * @param endpoints Etcd endpoints.
     */
    public void setEndpoints(List<String> endpoints) {
        this.endpoints = endpoints;
    }

    /**
     * Fluent-style version of {@link #setEndpoints(List)}.
     *
     * @param endpoints Etcd endpoints.
     *
     * @return This instance.
     */
    public EtcdSeedNodeProviderConfig withEndpoints(List<String> endpoints) {
        setEndpoints(endpoints);

        return this;
    }

    /**
     * Fluent-style version of {@link #setEndpoints(List)}.
     *
     * @param endpoint Etcd endpoint.
     *
     * @return This instance.
     */
    public EtcdSeedNodeProviderConfig withEndpoint(String endpoint) {
        if (getEndpoints() == null) {
            setEndpoints(new ArrayList<>());
        }

        getEndpoints().add(endpoint);

        return this;
    }

    /**
     * Returns the Etcd username (see {@link #setUsername(String)}).
     *
     * @return Etcd username.
     */
    public String getUsername() {
        return username;
    }

    /**
     * Sets the Etcd username.
     *
     * <p>
     * This parameter is optional if Etcd doesn't require authorization.
     * </p>
     *
     * @param username Etcd username.
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Fluent-style version of {@link #setUsername(String)}.
     *
     * @param username Etcd username.
     *
     * @return This instance.
     */
    public EtcdSeedNodeProviderConfig withUsername(String username) {
        setUsername(username);

        return this;
    }

    /**
     * Returns the Etcd password (see {@link #setPassword(String)}).
     *
     * @return Etcd password.
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets the Etcd password.
     *
     * <p>
     * This parameter is optional if Etcd doesn't require authorization.
     * </p>
     *
     * @param password Etcd password.
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Fluent-style version of {@link #setPassword(String)}.
     *
     * @param password Etcd password.
     *
     * @return This instance.
     */
    public EtcdSeedNodeProviderConfig withPassword(String password) {
        setPassword(password);

        return this;
    }

    /**
     * Returns the base path to store seed nodes information in Etcd (see {@link #setBasePath(String)}).
     *
     * @return Base path to store seed nodes.
     */
    public String getBasePath() {
        return basePath;
    }

    /**
     * Sets the base path to store seed nodes information in Etcd.
     *
     * <p>
     * Default value of this parameter is {@value #DEFAULT_BASE_PATH}.
     * </p>
     *
     * @param basePath Base path to store seed nodes.
     */
    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    /**
     * Fluent-style version of {@link #setBasePath(String)}.
     *
     * @param basePath Base path to store seed nodes.
     *
     * @return This instance.
     */
    public EtcdSeedNodeProviderConfig withBasePath(String basePath) {
        setBasePath(basePath);

        return this;
    }

    /**
     * Returns the time interval in milliseconds between stale node cleanup runs (see {@link #setCleanupInterval(int)}).
     *
     * @return Time interval in milliseconds.
     */
    public int getCleanupInterval() {
        return cleanupInterval;
    }

    /**
     * Sets the time interval in milliseconds between stale node cleanup runs.
     *
     * <p>Default value of this parameter is {@value #DEFAULT_CLEANUP_INTERVAL}.</p>
     *
     * <p>
     * For more details please see the documentation of {@link SeedNodeProvider}.
     * </p>
     *
     * @param cleanupInterval Time interval in milliseconds.
     *
     * @see SeedNodeProvider#cleanupInterval()
     */
    public void setCleanupInterval(int cleanupInterval) {
        this.cleanupInterval = cleanupInterval;
    }

    /**
     * Fluent-style version of {@link #setCleanupInterval(int)}.
     *
     * @param cleanupInterval Time interval in milliseconds.
     *
     * @return This instance.
     */
    public EtcdSeedNodeProviderConfig withCleanupInterval(int cleanupInterval) {
        setCleanupInterval(cleanupInterval);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
