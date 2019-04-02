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

package io.hekate.spring.boot.coordination;

import io.hekate.coordinate.CoordinationProcessConfig;
import io.hekate.coordinate.CoordinationService;
import io.hekate.coordinate.CoordinationServiceFactory;
import io.hekate.core.Hekate;
import io.hekate.spring.bean.coordinate.CoordinationServiceBean;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import io.hekate.spring.boot.HekateConfigurer;
import java.util.List;
import java.util.Optional;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

/**
 * <span class="startHere">&laquo; start here</span>Auto-configuration for {@link CoordinationService}.
 *
 * <h2>Overview</h2>
 * <p>
 * This auto-configuration constructs a {@link Bean} of {@link CoordinationServiceFactory} type and automatically {@link
 * CoordinationServiceFactory#setProcesses(List) registers} all {@link Bean}s of {@link CoordinationProcessConfig} type.
 * </p>
 *
 * <p>
 * <b>Note: </b> this auto-configuration is available only if application doesn't provide its own {@link Bean} of {@link
 * CoordinationServiceFactory} type and if there is at least one {@link Bean} of {@link CoordinationProcessConfig} type within the
 * application context.
 * </p>
 *
 * <h2>Configuration Properties</h2>
 * <p>
 * It is possible to configure {@link CoordinationServiceFactory} via application properties prefixed with {@code 'hekate.coordination'}.
 * For example:
 * </p>
 * <ul>
 * <li>{@link CoordinationServiceFactory#setNioThreads(int) 'hekate.coordination.nio-threads'}</li>
 * <li>{@link CoordinationServiceFactory#setRetryInterval(long) 'hekate.coordination.retry-interval'}</li>
 * <li>{@link CoordinationServiceFactory#setIdleSocketTimeout(long)} 'hekate.coordination.idle-socket-timeout'}</li>
 * </ul>
 *
 * @see CoordinationService
 * @see HekateConfigurer
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateConfigurer.class)
@ConditionalOnBean(CoordinationProcessConfig.class)
@ConditionalOnMissingBean(CoordinationServiceFactory.class)
public class HekateCoordinationServiceConfigurer {
    private final List<CoordinationProcessConfig> processes;

    /**
     * Constructs new instance.
     *
     * @param processes {@link CoordinationProcessConfig}s that were found in the application context.
     */
    public HekateCoordinationServiceConfigurer(Optional<List<CoordinationProcessConfig>> processes) {
        this.processes = processes.orElse(null);
    }

    /**
     * Constructs the {@link CoordinationServiceFactory}.
     *
     * @return Service factory.
     */
    @Bean
    @ConfigurationProperties(prefix = "hekate.coordination")
    public CoordinationServiceFactory coordinationServiceFactory() {
        CoordinationServiceFactory factory = new CoordinationServiceFactory();

        factory.setProcesses(processes);

        return factory;
    }

    /**
     * Returns the factory bean that makes it possible to inject {@link CoordinationService} directly into other beans instead of accessing
     * it via {@link Hekate#coordination()} method.
     *
     * @return Service bean.
     */
    @Lazy
    @Bean
    public CoordinationServiceBean coordinationService() {
        return new CoordinationServiceBean();
    }
}
