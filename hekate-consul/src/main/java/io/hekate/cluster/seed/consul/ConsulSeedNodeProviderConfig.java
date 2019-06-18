/*
 * Copyright 2019 The Hekate Project
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

package io.hekate.cluster.seed.consul;

import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.util.format.ToString;
import java.net.URI;

/**
 * Configuration for {@link ConsulSeedNodeProvider}.
 *
 * @see ConsulSeedNodeProvider#ConsulSeedNodeProvider(ConsulSeedNodeProviderConfig)
 */
public class ConsulSeedNodeProviderConfig {
    /** Default value (={@value}) for {@link #setCleanupInterval(long)}. */
    public static final long DEFAULT_CLEANUP_INTERVAL = 60 * 1000;

    /** Default value (={@value}) for {@link #setBasePath(String)}. */
    public static final String DEFAULT_BASE_PATH = "/hekate/cluster";

    /** Default consul's path separator. */
    public static final char SEPARATOR = '/';

    private String url;

    private long cleanupInterval = DEFAULT_CLEANUP_INTERVAL;

    private String basePath = DEFAULT_BASE_PATH;

    /**
     * Returns url of the Consul (see {@link #setUrl(String)} (String)}).
     *
     * @return Consul url.
     */
    public String getUrl() {
        return url;
    }

    /**
     * Sets path to Consul endpoint address. Url must be a valid string representation of {@link URI}.
     *
     * <p>
     * This parameter is mandatory.
     * </p>
     *
     * @param url Consul url.
     */
    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * Returns the time interval in milliseconds between stale node cleanup runs (see {@link #setCleanupInterval(long)}).
     *
     * @return Time interval in milliseconds.
     */
    public long getCleanupInterval() {
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
    public void setCleanupInterval(long cleanupInterval) {
        this.cleanupInterval = cleanupInterval;
    }

    /**
     * Returns the base path to store seed nodes information in Consul (see {@link #setBasePath(String)}).
     *
     * @return Base path to store seed nodes.
     */
    public String getBasePath() {
        return basePath;
    }

    /**
     * Sets the base path to store seed nodes information in Consul.
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

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
