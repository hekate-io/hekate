/*
 * Copyright 2022 The Hekate Project
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

package io.hekate.network;

import java.util.Collection;
import java.util.List;

/**
 * Provider of {@link NetworkConnector} configurations.
 *
 * <p>
 * Instances of this interface are responsible for providing {@link NetworkConnector} configurations that were obtained from some third
 * party sources or constructed dynamically based on the provider-specific rules. Instances of this interface can be registered via {@link
 * NetworkServiceFactory#setConfigProviders(List)} method.
 * </p>
 *
 * <p>
 * Another approach to register {@link NetworkConnector} configurations is to use {@link NetworkServiceFactory#setConnectors(List)} method.
 * </p>
 */
public interface NetworkConfigProvider {
    /**
     * Returns a collection of connector configurations that should be registered within the {@link NetworkService}.
     *
     * @return Channels configuration.
     */
    Collection<NetworkConnectorConfig<?>> configureNetwork();
}
