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

package io.hekate.election;

import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.ServiceFactory;
import io.hekate.election.internal.DefaultElectionService;
import io.hekate.util.format.ToString;
import java.util.ArrayList;
import java.util.List;

/**
 * Factory for {@link ElectionService}.
 *
 * <p>
 * This class represents a configurable factory for {@link ElectionService}. Instances of this class can be
 * {@link HekateBootstrap#withService(ServiceFactory) registered} within the {@link HekateBootstrap} in order to customize options of the
 * {@link ElectionService}.
 * </p>
 *
 * <p>
 * For more details about the {@link ElectionService} and its capabilities please see the documentation of {@link ElectionService}
 * interface.
 * </p>
 *
 * @see ElectionService
 */
public class ElectionServiceFactory implements ServiceFactory<ElectionService> {
    private List<CandidateConfig> candidates;

    private List<CandidateConfigProvider> configProviders;

    /**
     * Returns the list of election candidate configurations that should be automatically registered during the leader election service
     * startup (see {@link #setCandidates(List)}).
     *
     * @return List of election candidate configurations.
     */
    public List<CandidateConfig> getCandidates() {
        return candidates;
    }

    /**
     * Sets the list of election candidate configurations that should be automatically registered during the leader election service
     * startup.
     *
     * @param candidates Election candidate configurations.
     */
    public void setCandidates(List<CandidateConfig> candidates) {
        this.candidates = candidates;
    }

    /**
     * Fluent-style version of {@link #setCandidates(List)}.
     *
     * @param candidate Election candidate configurations.
     *
     * @return This instance.
     */
    public ElectionServiceFactory withCandidate(CandidateConfig candidate) {
        if (candidates == null) {
            candidates = new ArrayList<>();
        }

        candidates.add(candidate);

        return this;
    }

    /**
     * Fluent-style shortcut to register a new {@link CandidateConfig} with the specified
     * {@link CandidateConfig#setGroup(String) group}.
     *
     * @param group Group name (see {@link CandidateConfig#setGroup(String)}).
     *
     * @return New configuration.
     */
    public CandidateConfig withCandidate(String group) {
        CandidateConfig candidate = new CandidateConfig(group);

        withCandidate(candidate);

        return candidate;
    }

    /**
     * Returns the list of leader election configuration providers (see {@link #setConfigProviders(List)}).
     *
     * @return Leader election configuration providers.
     */
    public List<CandidateConfigProvider> getConfigProviders() {
        return configProviders;
    }

    /**
     * Sets the list of leader election configuration providers.
     *
     * @param configProviders Leader election configuration providers.
     *
     * @see CandidateConfigProvider
     */
    public void setConfigProviders(List<CandidateConfigProvider> configProviders) {
        this.configProviders = configProviders;
    }

    /**
     * Fluent-style version of {@link #setConfigProviders(List)}.
     *
     * @param configProvider Leader election configuration provider.
     *
     * @return This instance.
     */
    public ElectionServiceFactory withConfigProvider(CandidateConfigProvider configProvider) {
        if (configProviders == null) {
            configProviders = new ArrayList<>();
        }

        configProviders.add(configProvider);

        return this;
    }

    @Override
    public ElectionService createService() {
        return new DefaultElectionService(this);
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
