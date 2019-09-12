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

import io.hekate.core.ServiceInfo;
import java.util.Collection;

/**
 * Context for {@link ConfigurableService}.
 *
 * @see ConfigurableService#configure(ConfigurationContext)
 */
public interface ConfigurationContext {
    /**
     * Adds the specified {@link ServiceInfo#properties() service property}.
     *
     * @param name Property name.
     * @param value Property value (must be not-{@code null} and not empty).
     */
    void setStringProperty(String name, String value);

    /**
     * Adds the specified {@link ServiceInfo#properties() service property}.
     *
     * @param name Property name.
     * @param value Property value.
     */
    void setIntProperty(String name, int value);

    /**
     * Adds the specified {@link ServiceInfo#properties() service property}.
     *
     * @param name Property name.
     * @param value Property value.
     */
    void setLongProperty(String name, long value);

    /**
     * Adds the specified {@link ServiceInfo#properties() service property}.
     *
     * @param name Property name.
     * @param value Property value.
     */
    void setBoolProperty(String name, boolean value);

    /**
     * Searches for all components of the specified type.
     *
     * @param type Component type.
     * @param <T> Component type.
     *
     * @return Collections of matching components of an empty collection if there are no such components.
     */
    <T> Collection<T> findComponents(Class<T> type);
}
