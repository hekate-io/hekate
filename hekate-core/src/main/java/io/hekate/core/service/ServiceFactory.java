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

import io.hekate.core.HekateBootstrap;
import io.hekate.network.NetworkServiceFactory;
import java.util.List;

/**
 * Factory for {@link Service services}.
 *
 * <p>
 * This interface provides an abstraction of service configuration and construction. All service configuration options
 * are expected to be exposed as setters/getters of a factory implementation class (see {@link NetworkServiceFactory} for example).
 * </p>
 *
 * <p>
 * Implementations of this interface can be registered via {@link HekateBootstrap#setServices(List)} method.
 * </p>
 *
 * @param <T> Type of a service that is produced by this factory.
 *
 * @see DefaultServiceFactory
 */
public interface ServiceFactory<T extends Service> {
    /**
     * Constructs a new service instance based on the configuration options of this factory.
     *
     * @return New service instance.
     */
    T createService();
}
