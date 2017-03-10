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

import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.DefaultServiceFactory;
import io.hekate.core.service.Service;
import java.util.List;

/**
 * <span class="startHere">&laquo; start here</span>Main entry point to data partitioning API.
 *
 * <h2>Overview</h2>
 * <p>
 * {@link PartitionService} provides support for consistent data mapping to cluster nodes. It utilizes
 * <a href="https://en.wikipedia.org/wiki/Rendezvous_hashing" target="_blank">Rendezvous Hashing</a> for consistent distribution of data
 * keys between the cluster nodes with automatic re-mapping in case of cluster topology changes.
 * </p>
 *
 * <p>
 * Main working units in the partition service are {@link PartitionMapper}s. Partition mappers are responsible for performing actual
 * mapping of data to cluster nodes. Each mapper can be configured independently of others so that applications could use different
 * partitioning options for different data sets.
 * </p>
 *
 * <p>
 * Each {@link PartitionMapper} splits overall data set into a {@link PartitionMapperConfig#setPartitions(int) configurable} fixed set of
 * {@link Partition}s. Then each partition is assigned with a primary cluster node (and an optional set of backup nodes) according to the
 * <a href="https://en.wikipedia.org/wiki/Rendezvous_hashing" target="_blank">Rendezvous Hashing</a> algorithm.
 * In case of changes in the cluster topology each mapper automatically re-assigns partitions to remaining nodes.
 * </p>
 *
 * <h2>Service configuration</h2>
 * <p>
 * {@link PartitionService} can be configured and registered within the {@link HekateBootstrap} via the {@link PartitionServiceFactory}
 * class as in the example below:
 * ${source: partition/PartitionServiceJavadocTest.java#configure}
 * </p>
 *
 * <h2>Accessing service</h2>
 * <p>
 * {@link PartitionService} can be accessed via the {@link Hekate#get(Class)} method as in the example below:
 * ${source: partition/PartitionServiceJavadocTest.java#access}
 * </p>
 *
 * <h2>Usage example</h2>
 * <p>
 * The code example below illustrates the usage of {@link PartitionService}:
 * ${source: partition/PartitionServiceJavadocTest.java#usage}
 * </p>
 *
 * @see PartitionMapper
 */
@DefaultServiceFactory(PartitionServiceFactory.class)
public interface PartitionService extends Service {
    /**
     * Returns all partition mappers that are {@link PartitionServiceFactory#setMappers(List) registered} within this service.
     *
     * @return Partition mappers or an empty list if none are registered.
     */
    List<PartitionMapper> getMappers();

    /**
     * Returns a partition mapper for the specified {@link PartitionMapperConfig#setName(String) name}. If there is no such mapper then
     * an error will be thrown.
     *
     * <p>
     * Partition mappers can be registered via {@link PartitionServiceFactory#withMapper(PartitionMapperConfig)}.
     * </p>
     *
     * @param name Mapper name (see {@link PartitionMapperConfig#setName(String)}).
     *
     * @return Mapper.
     *
     * @see PartitionServiceFactory#withMapper(PartitionMapperConfig)
     */
    PartitionMapper get(String name);

    /**
     * Returns {@code true} if this service has a partition mapper with the specified name.
     *
     * @param name Mapper name (see {@link PartitionMapperConfig#setName(String)}).
     *
     * @return {@code true} if mapper exists.
     */
    boolean has(String name);
}
