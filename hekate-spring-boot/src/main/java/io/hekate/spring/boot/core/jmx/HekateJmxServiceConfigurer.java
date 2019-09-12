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

package io.hekate.spring.boot.core.jmx;

import io.hekate.core.jmx.JmxService;
import io.hekate.core.jmx.JmxServiceFactory;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import io.hekate.spring.boot.HekateConfigurer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * <span class="startHere">&laquo; start here</span>Auto-configuration for {@link JmxService}.
 *
 * <p>
 * This auto-configuration constructs a {@link Bean} of {@link JmxServiceFactory} type if application doesn't provide its own {@link Bean}
 * of {@link JmxServiceFactory} type and if {@code 'hekate.jmx.enable'} application property is set to {@code true}.
 * </p>
 *
 * @see JmxService
 * @see HekateConfigurer
 */
@ConditionalOnHekateEnabled
@ConditionalOnMissingBean(JmxServiceFactory.class)
@ConditionalOnProperty(value = "hekate.jmx.enable", havingValue = "true")
public class HekateJmxServiceConfigurer {
    /**
     * Constructs the {@link JmxServiceFactory}.
     *
     * @return Service factory.
     */
    @Bean
    @ConfigurationProperties(prefix = "hekate.jmx")
    public JmxServiceFactory jmxServiceFactory() {
        return new JmxServiceFactory();
    }
}
