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

/**
 * Lifecycle interface for services that depend on some other services.
 *
 * <p>
 * {@link Service}s can implement this interface in order to obtain references to some other services that they depend on. The {@link
 * #resolve(DependencyContext)} method of this interface gets called only once by a {@link Hekate} instance right after the service
 * gets {@link ServiceFactory#createService() constructed} and never gets called again even if node leaves and then rejoins the cluster.
 * </p>
 *
 * <p>
 * The order of dependencies resolution also influences on the services {@link InitializingService initialization} and {@link
 * TerminatingService termination} order. If service {@code A} depends on services {@code B} and {@code C} then it is guaranteed that
 * services {@code B} and {@code C} will be {@link InitializingService initialized} before the service {@code A} and will be {@link
 * TerminatingService terminated} after the service {@code A}.
 * </p>
 *
 * @see InitializingService
 * @see TerminatingService
 */
public interface DependentService {
    /**
     * Resolves dependencies of this service.
     *
     * @param ctx Dependency resolution context.
     */
    void resolve(DependencyContext ctx);
}
