/*
 * Copyright 2017 The Hekate Project
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

package io.hekate.partition;

import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.ServiceFactory;
import io.hekate.partition.internal.DefaultPartitionService;
import io.hekate.util.format.ToString;
import java.util.ArrayList;
import java.util.List;

/**
 * Factory for {@link PartitionService}.
 *
 * <p>
 * This class represents a configurable factory for {@link PartitionService}. Instances of this class can be
 * {@link HekateBootstrap#withService(ServiceFactory) registered} within the {@link HekateBootstrap} in order to customize options of the
 * {@link PartitionService}.
 * </p>
 *
 * <p>
 * For more details about the {@link PartitionService} and its capabilities please see the documentation of {@link PartitionService}
 * interface.
 * </p>
 */
public class PartitionServiceFactory implements ServiceFactory<PartitionService> {
    private List<PartitionMapperConfig> mappers;

    private List<PartitionConfigProvider> configProviders;

    /**
     * Returns the list of partition mappers configuration (see {@link #setMappers(List)}).
     *
     * @return Partition mappers configuration.
     */
    public List<PartitionMapperConfig> getMappers() {
        return mappers;
    }

    /**
     * Sets the list of partition mappers configurations that should be registered to the {@link PartitionService}.
     *
     * @param mappers Partition mappers configuration.
     *
     * @see PartitionService#mapper(String)
     */
    public void setMappers(List<PartitionMapperConfig> mappers) {
        this.mappers = mappers;
    }

    /**
     * Fluent-style version of {@link #setMappers(List)}.
     *
     * @param mapper Partition mappers configuration.
     *
     * @return This instance.
     */
    public PartitionServiceFactory withMapper(PartitionMapperConfig mapper) {
        if (mappers == null) {
            mappers = new ArrayList<>();
        }

        mappers.add(mapper);

        return this;
    }

    /**
     * Fluent-style shortcut to register a new {@link PartitionMapperConfig} with the specified
     * {@link PartitionMapperConfig#setName(String) name}.
     *
     * @param name Mapper name (see {@link PartitionMapperConfig#setName(String)}).
     *
     * @return New mapper configuration.
     */
    public PartitionMapperConfig withMapper(String name) {
        PartitionMapperConfig mapper = new PartitionMapperConfig(name);

        withMapper(mapper);

        return mapper;
    }

    /**
     * Returns the list of partition mapper configuration providers (see {@link #setConfigProviders(List)}).
     *
     * @return Partition mapper configuration providers.
     */
    public List<PartitionConfigProvider> getConfigProviders() {
        return configProviders;
    }

    /**
     * Sets the list of partition mapper configuration providers.
     *
     * @param configProviders Partition mapper configuration providers.
     *
     * @see PartitionConfigProvider
     */
    public void setConfigProviders(List<PartitionConfigProvider> configProviders) {
        this.configProviders = configProviders;
    }

    /**
     * Fluent-style version of {@link #setConfigProviders(List)}.
     *
     * @param configProvider Partition mapper configuration provider.
     *
     * @return This instance.
     */
    public PartitionServiceFactory withConfigProvider(PartitionConfigProvider configProvider) {
        if (configProviders == null) {
            configProviders = new ArrayList<>();
        }

        configProviders.add(configProvider);

        return this;
    }

    @Override
    public PartitionService createService() {
        return new DefaultPartitionService(this);
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
