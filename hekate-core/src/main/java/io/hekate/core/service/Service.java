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
import java.util.List;

/**
 * <span class="startHere">&laquo; start here</span>Entry point to core API of {@link Hekate} services.
 *
 * <h2>Overview</h2>
 * <p>
 * {@link Hekate} utilizes the Service-oriented approach in order to provide a fine grained control over resources utilization and
 * functionality of each individual {@link Hekate} instance. Each instance can be configured with only those services that are required for
 * application needs without an overhead of managing services that are never used by the application.
 * </p>
 *
 * <p>
 * <b>Note:</b> Typically applications are not required to implement their own custom services and can act purely as clients of services
 * that are provided by {@link Hekate} out of the box. Custom services should be implemented only in order to extend functionality of
 * existing services or provide some new functionality that is not covered by {@link Hekate} API.
 * </p>
 *
 * <h2>Service Interface</h2>
 * <p>
 * All services must implement {@link Service} marker interface in order to be accessible via {@link Hekate#get(Class)} method and can also
 * optionally implement a set of callback interfaces in order to participate in {@link Hekate} instance lifecycle.
 * </p>
 * <p>
 * The following lifecycle interfaces are available (in invocation order):
 * </p>
 * <ul>
 * <li>{@link DependentService} - for resolving dependencies on other services.</li>
 * <li>{@link ConfigurableService} - for preparing and validating service configuration</li>
 * <li>{@link InitializingService} - for service state initialization before the {@link Hekate} node starts joining the cluster</li>
 * <li>{@link TerminatingService} - for service state cleanup when the {@link Hekate} node leaves the cluster</li>
 * </ul>
 *
 * <p>
 * Please see the documentation of those interfaces for more details.
 * </p>
 *
 * <h2>Service Factory</h2>
 * <p>
 * Each service must have a {@link ServiceFactory} that is responsible for configuring and creating new service instances. Service
 * factories can be registered via {@link HekateBootstrap#setServices(List)} method.
 * </p>
 *
 * <p>
 * <b>Note:</b> Each service gets {@link ServiceFactory#createService() created} only once by each {@link Hekate} node during the
 * initialization phase and never gets re-created even if node leaves and then rejoins the cluster.
 * </p>
 *
 * @see Hekate#get(Class)
 */
public interface Service {
    // No-op.
}
