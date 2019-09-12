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

package io.hekate.cluster.seed;

import io.hekate.core.internal.util.StreamUtils;
import io.hekate.util.format.ToString;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration options for {@link SeedNodeProviderGroup}.
 *
 * @see SeedNodeProviderGroup#SeedNodeProviderGroup(SeedNodeProviderGroupConfig)
 */
public class SeedNodeProviderGroupConfig {
    /** See {@link #setPolicy(SeedNodeProviderGroupPolicy)}. */
    private SeedNodeProviderGroupPolicy policy = SeedNodeProviderGroupPolicy.FAIL_ON_FIRST_ERROR;

    /** See {@link #setProviders(List)}. */
    private List<SeedNodeProvider> providers;

    /**
     * Returns the error handling policy of this group (see {@link #setPolicy(SeedNodeProviderGroupPolicy)}).
     *
     * @return Error handling policy of this group.
     */
    public SeedNodeProviderGroupPolicy getPolicy() {
        return policy;
    }

    /**
     * Sets the error handling policy for this group.
     *
     * <p>
     * Default value of this parameter is {@link SeedNodeProviderGroupPolicy#FAIL_ON_FIRST_ERROR}.
     * </p>
     *
     * @param policy Error handling policy of this group.
     */
    public void setPolicy(SeedNodeProviderGroupPolicy policy) {
        this.policy = policy;
    }

    /**
     * Fluent-style version of {@link #setPolicy(SeedNodeProviderGroupPolicy)}.
     *
     * @param policy Error handling policy of this group.
     *
     * @return This instance.
     */
    public SeedNodeProviderGroupConfig withPolicy(SeedNodeProviderGroupPolicy policy) {
        setPolicy(policy);

        return this;
    }

    /**
     * Returns the list of providers of this group (see {@link #setProviders(List)}).
     *
     * @return Providers of this group.
     */
    public List<SeedNodeProvider> getProviders() {
        return providers;
    }

    /**
     * Sets the list of providers for this group.
     *
     * @param providers List of providers for this group.
     */
    public void setProviders(List<SeedNodeProvider> providers) {
        this.providers = providers;
    }

    /**
     * Fluent-style version of {@link #setProviders(List)}.
     *
     * @param provider Provider.
     *
     * @return This instance.
     */
    public SeedNodeProviderGroupConfig withProvider(SeedNodeProvider provider) {
        if (getProviders() == null) {
            setProviders(new ArrayList<>());
        }

        getProviders().add(provider);

        return this;
    }

    /**
     * Fluent-style version of {@link #setProviders(List)}.
     *
     * @param providers Providers.
     *
     * @return This instance.
     */
    public SeedNodeProviderGroupConfig withProviders(List<SeedNodeProvider> providers) {
        setProviders(providers);

        return this;
    }

    /**
     * Returns {@code true} if this configuration has at least one provider (see {@link #setProviders(List)}).
     *
     * @return {@code true} if this configuration has at least one provider (see {@link #setProviders(List)}).
     */
    public boolean hasProviders() {
        return StreamUtils.nullSafe(providers).count() > 0;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
