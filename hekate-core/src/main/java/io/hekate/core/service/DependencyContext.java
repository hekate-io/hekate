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

package io.hekate.core.service;

import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Context for {@link DependentService}.
 *
 * @see DependentService#resolve(DependencyContext)
 */
public interface DependencyContext {
    /**
     * Returns the node node.
     *
     * @return Name of this node.
     *
     * @see HekateBootstrap#setNodeName(String)
     */
    String nodeName();

    /**
     * Returns the cluster name.
     *
     * @return Name of the local node's cluster.
     *
     * @see HekateBootstrap#setClusterName(String)
     */
    String clusterName();

    /**
     * Returns the {@link Hekate} instance that manages this service.
     * <p>
     * <b>Notice:</b> the returned instance may be not fully initialized. The main purpose of this method is to provide a reference
     * for the service back to its owning {@link Hekate} instance, so that it could be used during the further lifecycle phases.
     * </p>
     *
     * @return {@link Hekate} instance that manages this service.
     */
    Hekate hekate();

    /**
     * Returns a reference to a required service. If such service can't be found then {@link ServiceDependencyException}  will be thrown.
     *
     * @param service Service type.
     * @param <T> Service type.
     *
     * @return Service.
     *
     * @throws ServiceDependencyException If required service couldn't be found.
     */
    <T extends Service> T require(Class<T> service) throws ServiceDependencyException;

    /**
     * Returns a reference to an optional service. If there is no such service then this method returns {@code null}.
     *
     * @param service Service type.
     * @param <T> Service type.
     *
     * @return Service or {@code null} if the is no such service.
     */
    <T extends Service> T optional(Class<T> service);

    /**
     * Returns metrics registry.
     *
     * @return Metrics registry.
     */
    MeterRegistry metrics();
}
