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

package io.hekate.cluster.seed.jclouds;

import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.Properties;
import org.jclouds.blobstore.BlobStore;

/**
 * Configuration for {@link CloudStoreSeedNodeProvider}.
 *
 * @see CloudStoreSeedNodeProvider
 */
public class CloudStoreSeedNodeProviderConfig extends CloudPropertiesBase<CloudStoreSeedNodeProviderConfig> {
    /** Default value (={@value}) for {@link #setCleanupInterval(long)}. */
    public static final long DEFAULT_CLEANUP_INTERVAL = 60000;

    /** See {@link #setProvider(String)}. */
    private String provider;

    /** See {@link #setProperties(Properties)}. */
    private Properties properties;

    /** See {@link #setCleanupInterval(long)}. */
    private long cleanupInterval = DEFAULT_CLEANUP_INTERVAL;

    /** See {@link #setContainer(String)}. */
    private String container;

    /** See {@link #setCredentials(CredentialsSupplier)}. */
    @ToStringIgnore
    private CredentialsSupplier credentials;

    /**
     * Returns the name of a {@link BlobStore} provider (see {@link #setProvider(String)}).
     *
     * @return Name of a {@link BlobStore} provider.
     */
    public String getProvider() {
        return provider;
    }

    /**
     * Sets the name of a {@link BlobStore} provider (f.e {@code "aws-s3"}).
     *
     * @param provider Name of a {@link BlobStore} provider.
     */
    public void setProvider(String provider) {
        this.provider = provider;
    }

    /**
     * Fluent-style version of {@link #setProvider(String)}.
     *
     * @param provider Name of a {@link BlobStore} provider.
     *
     * @return This instance.
     */
    public CloudStoreSeedNodeProviderConfig withProvider(String provider) {
        setProvider(provider);

        return this;
    }

    /**
     * Returns the supplier of {@link BlobStore} credentials (see {@link #setCredentials(CredentialsSupplier)}).
     *
     * @return Supplier of {@link BlobStore} credentials.
     */
    public CredentialsSupplier getCredentials() {
        return credentials;
    }

    /**
     * Sets the supplier of {@link BlobStore} credentials.
     *
     * @param credentials Supplier of {@link BlobStore} credentials.
     */
    public void setCredentials(CredentialsSupplier credentials) {
        this.credentials = credentials;
    }

    /**
     * Fluent-style version of {@link #setCredentials(CredentialsSupplier)}.
     *
     * @param credentials Supplier of {@link BlobStore} credentials.
     *
     * @return This instance.
     */
    public CloudStoreSeedNodeProviderConfig withCredentials(CredentialsSupplier credentials) {
        setCredentials(credentials);

        return this;
    }

    /**
     * Returns the {@link BlobStore} provider-specific properties (see {@link #setProperties(Properties)}).
     *
     * @return Provider properties.
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Sets the {@link BlobStore} provider-specific properties.
     *
     * @param properties Provider properties.
     */
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    /**
     * Fluent-style version of {@link #setCredentials(CredentialsSupplier)}.
     *
     * @param key Property key.
     * @param value Property value.
     *
     * @return This instance.
     */
    public CloudStoreSeedNodeProviderConfig withProperty(String key, String value) {
        if (properties == null) {
            properties = new Properties();
        }

        properties.setProperty(key, value);

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
    public CloudStoreSeedNodeProviderConfig withCleanupInterval(long cleanupInterval) {
        setCleanupInterval(cleanupInterval);

        return this;
    }

    /**
     * Returns the name of a {@link BlobStore} container (see {@link #setContainer(String)}).
     *
     * @return Name of a container (f.e. Amazon S3 bucket) to store all information about the cluster seed nodes.
     */
    public String getContainer() {
        return container;
    }

    /**
     * Returns the name of a {@link BlobStore} container (f.e. Amazon S3 bucket) to store all information about the cluster seed nodes.
     *
     * @param container Name of a container (f.e. Amazon S3 bucket) to store all information about the cluster seed nodes.
     */
    public void setContainer(String container) {
        this.container = container;
    }

    /**
     * Fluent-style version of {@link #setContainer(String)}.
     *
     * @param container Name of a container (f.e. Amazon S3 bucket) to store all information about the cluster seed nodes.
     *
     * @return This instance.
     */
    public CloudStoreSeedNodeProviderConfig withContainer(String container) {
        setContainer(container);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
