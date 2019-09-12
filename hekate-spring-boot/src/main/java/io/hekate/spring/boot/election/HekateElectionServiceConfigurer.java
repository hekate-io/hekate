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

package io.hekate.spring.boot.election;

import io.hekate.core.Hekate;
import io.hekate.election.CandidateConfig;
import io.hekate.election.ElectionService;
import io.hekate.election.ElectionServiceFactory;
import io.hekate.spring.bean.election.ElectionServiceBean;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import io.hekate.spring.boot.HekateConfigurer;
import java.util.List;
import java.util.Optional;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

/**
 * <span class="startHere">&laquo; start here</span>Auto-configuration for {@link ElectionService}.
 *
 * <h2>Overview</h2>
 * <p>
 * This auto-configuration constructs a {@link Bean} of {@link ElectionServiceFactory} type and automatically {@link
 * ElectionServiceFactory#setCandidates(List) registers} all {@link Bean}s of {@link CandidateConfig} type.
 * </p>
 *
 * <p>
 * <b>Note: </b> this auto-configuration is available only if application doesn't provide its own {@link Bean} of {@link
 * ElectionServiceFactory} type and if there is at least one {@link Bean} of {@link CandidateConfig} type within the application context.
 * </p>
 *
 * @see ElectionService
 * @see HekateConfigurer
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateConfigurer.class)
@ConditionalOnBean(CandidateConfig.class)
@ConditionalOnMissingBean(ElectionServiceFactory.class)
public class HekateElectionServiceConfigurer {
    private final List<CandidateConfig> candidates;

    /**
     * Constructs new instance.
     *
     * @param candidates {@link CandidateConfig}s that were found in the application context.
     */
    public HekateElectionServiceConfigurer(Optional<List<CandidateConfig>> candidates) {
        this.candidates = candidates.orElse(null);
    }

    /**
     * Constructs the {@link ElectionServiceFactory}.
     *
     * @return Service factory.
     */
    @Bean
    public ElectionServiceFactory electionServiceFactory() {
        ElectionServiceFactory factory = new ElectionServiceFactory();

        factory.setCandidates(candidates);

        return factory;
    }

    /**
     * Returns the factory bean that makes it possible to inject {@link ElectionService} directly into other beans instead of accessing it
     * via {@link Hekate#election()} method.
     *
     * @return Service bean.
     */
    @Lazy
    @Bean
    public ElectionServiceBean electionService() {
        return new ElectionServiceBean();
    }
}
