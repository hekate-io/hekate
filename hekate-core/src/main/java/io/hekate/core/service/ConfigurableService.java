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
 * Lifecycle interface for services that require a one time configuration pre-processing.
 *
 * <p>
 * {@link Service}s can implement this interface in order to perform a one time configuration pre-processing and validation.
 * The {@link #configure(ConfigurationContext)} method of this interface gets called only once by a {@link Hekate} instance right after the
 * service gets {@link ServiceFactory#createService() constructed} and its dependencies are {@link DependentService resolved}. This method
 * never gets called again even if node leaves and then rejoins the cluster.
 * </p>
 */
public interface ConfigurableService {
    /**
     * Configures this service.
     *
     * @param ctx Configuration context.
     */
    void configure(ConfigurationContext ctx);
}
