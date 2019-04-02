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

import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.jclouds.ContextBuilder;
import org.jclouds.compute.ComputeService;

/**
 * Configuration for {@link CloudSeedNodeProvider}.
 *
 * @see CloudSeedNodeProvider
 */
public class CloudSeedNodeProviderConfig extends CloudPropertiesBase<CloudSeedNodeProviderConfig> {
    /** See {@link #setProvider(String)}. */
    private String provider;

    /** See {@link #setEndpoint(String)}. */
    private String endpoint;

    /** See {@link #setRegions(Set)}. */
    private Set<String> regions;

    /** See {@link #setZones(Set)}. */
    private Set<String> zones;

    /** See {@link #setTags(Map)}. */
    private Map<String, String> tags;

    /** See {@link #setProperties(Properties)}. */
    private Properties properties;

    /** See {@link #setCredentials(CredentialsSupplier)}. */
    @ToStringIgnore
    private CredentialsSupplier credentials;

    /**
     * Returns the name of a {@link ComputeService} provider (see {@link #setProvider(String)}).
     *
     * @return Name of a {@link ComputeService} provider.
     */
    public String getProvider() {
        return provider;
    }

    /**
     * Sets the name of a {@link ComputeService} provider (f.e. {@code "aws-ec2"}).
     *
     * @param provider Name of a {@link ComputeService} provider.
     *
     * @see ContextBuilder#newBuilder(String)
     */
    public void setProvider(String provider) {
        this.provider = provider;
    }

    /**
     * Fluent-style version of {@link #setProvider(String)}.
     *
     * @param provider Name of a {@link ComputeService} provider.
     *
     * @return This instance.
     */
    public CloudSeedNodeProviderConfig withProvider(String provider) {
        setProvider(provider);

        return this;
    }

    /**
     * Returns the {@link ComputeService} provider endpoint (see {@link #setEndpoint(String)}).
     *
     * @return Endpoint.
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * Sets the {@link ComputeService} endpoint (f.e {@code "https://ec2.us-east-1.amazonaws.com"}).
     *
     * <p>
     * If not specified then the {@link #setProvider(String) provider}'s default endpoint will be used.
     * </p>
     *
     * @param endpoint Endpoint.
     *
     * @see ContextBuilder#endpoint(String)
     */
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * Fluent-style version of {@link #setEndpoint(String)}.
     *
     * @param endpoint Endpoint.
     *
     * @return This instance.
     */
    public CloudSeedNodeProviderConfig withEndpoint(String endpoint) {
        setEndpoint(endpoint);

        return this;
    }

    /**
     * Returns the supplier of credentials for {@link ComputeService}.
     *
     * @return Supplier of {@link ComputeService} credentials.
     */
    public CredentialsSupplier getCredentials() {
        return credentials;
    }

    /**
     * Sets the supplier of credentials for {@link ComputeService}.
     *
     * @param credentials Supplier of {@link ComputeService} credentials.
     */
    public void setCredentials(CredentialsSupplier credentials) {
        this.credentials = credentials;
    }

    /**
     * Fluent-style version of {@link #setCredentials(CredentialsSupplier)}.
     *
     * @param credentials Supplier of {@link ComputeService} credentials.
     *
     * @return This instance.
     */
    public CloudSeedNodeProviderConfig withCredentials(CredentialsSupplier credentials) {
        setCredentials(credentials);

        return this;
    }

    /**
     * Returns the {@link ComputeService} provider-specific properties (see {@link #setProperties(Properties)}).
     *
     * @return Provider properties.
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Sets the {@link ComputeService} provider-specific properties.
     *
     * @param properties Provider properties.
     */
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    /**
     * Fluent-style version of {@link #setProperties(Properties)}.
     *
     * @param key Property key.
     * @param value Property value.
     *
     * @return This instance.
     */
    public CloudSeedNodeProviderConfig withProperty(String key, String value) {
        if (properties == null) {
            properties = new Properties();
        }

        properties.setProperty(key, value);

        return this;
    }

    /**
     * Returns a set of regions to search for seed nodes (see @{@link #setRegions(Set)}).
     *
     * @return Regions to search for seed nodes.
     */
    public Set<String> getRegions() {
        return regions;
    }

    /**
     * Sets the set of regions to search for seed nodes (f.e. {@code "eu-central-1" for Amazon}).
     *
     * <p>
     * If not specified then all of the {@link ComputeService} provider's regions will be scanned for seed nodes presence.
     * </p>
     *
     * @param regions Regions to search for seed nodes.
     */
    public void setRegions(Set<String> regions) {
        this.regions = regions;
    }

    /**
     * Fluent-style version of {@link #setRegions(Set)}.
     *
     * @param region Region to search for seed nodes.
     *
     * @return This instance.
     */
    public CloudSeedNodeProviderConfig withRegion(String region) {
        if (regions == null) {
            regions = new HashSet<>();
        }

        regions.add(region);

        return this;
    }

    /**
     * Returns a set of availability zones to search for seed nodes (see {@link #setZones(Set)}).
     *
     * @return Zones to search for seed nodes.
     */
    public Set<String> getZones() {
        return zones;
    }

    /**
     * Sets the set of availability zones to search for seed nodes (f.e. {@code "eu-central-1a" for Amazon}).
     *
     * <p>
     * If not specified then filtering by availability zones will not be applied.
     * </p>
     *
     * @param zones Zones to search for seed nodes.
     */
    public void setZones(Set<String> zones) {
        this.zones = zones;
    }

    /**
     * Fluent-style version of {@link #setZones(Set)}.
     *
     * @param zone Zone to search for seed nodes.
     *
     * @return This instance.
     */
    public CloudSeedNodeProviderConfig withZone(String zone) {
        if (zones == null) {
            zones = new HashSet<>();
        }

        zones.add(zone);

        return this;
    }

    /**
     * Returns a map of compute instance's tag names/values to search for seed nodes (see @{@link #setTags(Map)}).
     *
     * @return Tag names and values to search for seed nodes.
     */
    public Map<String, String> getTags() {
        return tags;
    }

    /**
     * Sets the map of a compute instance's tag names and tag values to search for seed nodes.
     *
     * <p>
     * If specified then only those compute instances that have the specified tag names and values will be scanned for seed nodes presence.
     * </p>
     *
     * @param tags Map of tag names and values.
     */
    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    /**
     * Fluent-style version of {@link #setTags(Map)}.
     *
     * @param name Tag name.
     * @param value Tag value.
     *
     * @return This instance.
     */
    public CloudSeedNodeProviderConfig withTag(String name, String value) {
        if (tags == null) {
            tags = new HashMap<>();
        }

        tags.put(name, value);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
