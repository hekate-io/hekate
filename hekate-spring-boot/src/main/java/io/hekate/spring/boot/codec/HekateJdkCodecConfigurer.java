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

package io.hekate.spring.boot.codec;

import io.hekate.codec.JdkCodecFactory;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import io.hekate.spring.boot.HekateConfigurer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for {@link JdkCodecFactory}.
 *
 * <h2>Configuration</h2>
 * <p>
 * This auto-configuration can be enabled by setting the {@code 'hekate.codec'} property to {@code 'jdk'} in the application configuration.
 * </p>
 *
 * <p>
 * This auto-configuration doesn't provide any additional configuration properties.
 * </p>
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateConfigurer.class)
@ConditionalOnProperty(name = "hekate.codec", havingValue = "jdk")
public class HekateJdkCodecConfigurer {
    /**
     * Constructs a new instance of {@link JdkCodecFactory}.
     *
     * @return Codec factory.
     */
    @Bean
    @Qualifier("default")
    @ConfigurationProperties(prefix = "hekate.codec.jdk")
    public JdkCodecFactory<Object> jdkCodecFactory() {
        return new JdkCodecFactory<>();
    }
}
