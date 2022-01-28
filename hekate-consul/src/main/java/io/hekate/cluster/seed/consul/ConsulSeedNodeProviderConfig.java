/*
 * Copyright 2022 The Hekate Project
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
    public static final long DEFAULT_CLEANUP_INTERVAL = 60_000;

    /** Default value (={@value}) for {@link #setBasePath(String)}. */
    public static final String DEFAULT_BASE_PATH = "/hekate/cluster";

    /** See {@link #setUrl(String)}. */
    private String url;

    /** See {@link #setCleanupInterval(long)}. */
    private long cleanupInterval = DEFAULT_CLEANUP_INTERVAL;

    /** See {@link #setBasePath(String)}. */
    private String basePath = DEFAULT_BASE_PATH;

    /** See {@link #setConnectTimeout(Long)}. */
    private Long connectTimeout;

    /** See {@link #setReadTimeout(Long)}. */
    private Long readTimeout;

    /** See {@link #setWriteTimeout(Long)}. */
    private Long writeTimeout;

    /** See {@link #setAclToken(String)}. */
    private String aclToken;

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
     * Fluent-style version of {@link #setUrl(String)}.
     *
     * @param url Consul url.
     *
     * @return This instance.
     */
    public ConsulSeedNodeProviderConfig withUrl(String url) {
        setUrl(url);

        return this;
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
     * Fluent-style version of {@link #setCleanupInterval(long)}.
     *
     * @param cleanupInterval Time interval in milliseconds.
     *
     * @return This instance.
     */
    public ConsulSeedNodeProviderConfig withCleanupInterval(long cleanupInterval) {
        setCleanupInterval(cleanupInterval);

        return this;
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

    /**
     * Fluent-style version of {@link #setBasePath(String)}.
     *
     * @param basePath Base path to store seed nodes.
     *
     * @return This instance.
     */
    public ConsulSeedNodeProviderConfig withBasePath(String basePath) {
        setBasePath(basePath);

        return this;
    }

    /**
     * Returns the connect timeout value in milliseconds (see {@link #setConnectTimeout(Long)}).
     *
     * @return Timeout in milliseconds.
     */
    public Long getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Set the connect timeout value in milliseconds.
     *
     * <p>
     * This parameter is optional, and if it is not specified, the Consul’s client library will use its default value.
     * </p>
     *
     * @param connectTimeout Timeout in milliseconds.
     */
    public void setConnectTimeout(Long connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /**
     * Fluent style version of {@link #setConnectTimeout(Long)}.
     *
     * @param connectTimeout Timeout in milliseconds.
     *
     * @return This instance.
     */
    public ConsulSeedNodeProviderConfig withConnectTimeout(Long connectTimeout) {
        setConnectTimeout(connectTimeout);

        return this;
    }

    /**
     * Returns the read timeout value in milliseconds (see {@link #setReadTimeout(Long)}).
     *
     * @return Timeout in milliseconds.
     */
    public Long getReadTimeout() {
        return readTimeout;
    }

    /**
     * Set the read timeout value in milliseconds.
     *
     * <p>
     * This parameter is optional, and if it is not specified, the Consul’s client library will use its default value.
     * </p>
     *
     * @param readTimeout Timeout in milliseconds.
     */
    public void setReadTimeout(Long readTimeout) {
        this.readTimeout = readTimeout;
    }

    /**
     * Fluent style version of {@link #setReadTimeout(Long)}.
     *
     * @param readTimeout Timeout in milliseconds.
     *
     * @return This instance.
     */
    public ConsulSeedNodeProviderConfig withReadTimeout(Long readTimeout) {
        setReadTimeout(readTimeout);

        return this;
    }

    /**
     * Returns the write timeout value in milliseconds (see {@link #setWriteTimeout(Long)}).
     *
     * @return Timeout in milliseconds.
     */
    public Long getWriteTimeout() {
        return writeTimeout;
    }

    /**
     * Set the write timeout value in milliseconds.
     *
     * <p>
     * This parameter is optional, and if it is not specified, the Consul’s client library will use its default value.
     * </p>
     *
     * @param writeTimeout Timeout in milliseconds.
     */
    public void setWriteTimeout(Long writeTimeout) {
        this.writeTimeout = writeTimeout;
    }

    /**
     * Fluent style version of {@link #setWriteTimeout(Long)}.
     *
     * @param writeTimeout Timeout in milliseconds.
     *
     * @return This instance.
     */
    public ConsulSeedNodeProviderConfig withWriteTimeout(Long writeTimeout) {
        setWriteTimeout(writeTimeout);

        return this;
    }

    /**
     * Returns the ACL token to access Consul (see {@link #setAclToken(String)}).
     *
     * @return ACL token to access Consul.
     */
    public String getAclToken() {
        return aclToken;
    }

    /**
     * Sets ACL token to access Consul.
     *
     * @param aclToken ACL token.
     */
    public void setAclToken(String aclToken) {
        this.aclToken = aclToken;
    }

    /**
     * Fluent style version of {@link #setAclToken(String)} .
     *
     * @param aclToken ACL token.
     *
     * @return This instance.
     */
    public ConsulSeedNodeProviderConfig withAclToken(String aclToken) {
        setAclToken(aclToken);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
