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

import io.hekate.codec.kryo.KryoCodecFactory;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import io.hekate.spring.boot.HekateConfigurer;
import java.util.List;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for {@link KryoCodecFactory}.
 *
 * <h2>Module Dependency</h2>
 * <p>
 * Kryo integration requires {@code io.hekate:hekate-codec-kryo} to be placed on the project classpath.
 * </p>
 *
 * <h2>Configuration</h2>
 * <p>
 * This auto-configuration can be enabled by setting the {@code 'hekate.codec'} property to {@code 'kryo'} in the application configuration.
 * </p>
 *
 * <p>
 * The following properties can be used to customize the auto-configured {@link KryoCodecFactory} instance:
 * </p>
 * <ul>
 * <li>{@link KryoCodecFactory#setKnownTypes(List) 'hekate.codec.kryo.known-types'}</li>
 * <li>{@link KryoCodecFactory#setRegistrationRequired(boolean) 'hekate.codec.kryo.registration-required'}</li>
 * <li>{@link KryoCodecFactory#setUnsafeIo(boolean) 'hekate.codec.kryo.unsafe-io'}</li>
 * <li>{@link KryoCodecFactory#setReferences(Boolean) 'hekate.codec.kryo.references'}</li>
 * </ul>
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateConfigurer.class)
@ConditionalOnProperty(name = "hekate.codec", havingValue = "kryo")
public class HekateKryoCodecConfigurer {
    /**
     * Constructs a new instance of {@link KryoCodecFactory}.
     *
     * @return Codec factory.
     */
    @Bean
    @Qualifier("default")
    @ConfigurationProperties(prefix = "hekate.codec.kryo")
    public KryoCodecFactory<Object> kryoCodecFactory() {
        return new KryoCodecFactory<>();
    }
}
