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

package io.hekate.core;

import io.hekate.cluster.ClusterNode;
import io.hekate.core.ServiceProperty.Type;
import java.util.Map;

/**
 * Provides information about a {@link Hekate#get(Class) service}.
 *
 * @see ClusterNode#services()
 */
public interface ServiceInfo {
    /**
     * Returns the service class name.
     *
     * @return Service class name.
     */
    String type();

    /**
     * Returns an immutable map of service properties.
     *
     * @return Service properties.
     */
    Map<String, ServiceProperty<?>> properties();

    /**
     * Returns the property value or {@code null} if there is no such property.
     *
     * @param name Property name.
     *
     * @return Property value or {@code null}.
     */
    ServiceProperty<?> property(String name);

    /**
     * Returns the property value or {@code null} if there is no such property or if property is not of {@link Type#INTEGER} type.
     *
     * @param name Property name.
     *
     * @return Property value or {@code null}.
     */
    default Integer intProperty(String name) {
        ServiceProperty<?> prop = property(name);

        return prop != null && prop.type() == Type.INTEGER ? (Integer)prop.value() : null;
    }

    /**
     * Returns the property value or {@code null} if there is no such property or if property is not of {@link Type#LONG} type.
     *
     * @param name Property name.
     *
     * @return Property value or {@code null}.
     */
    default Long longProperty(String name) {
        ServiceProperty<?> prop = property(name);

        return prop != null && prop.type() == Type.LONG ? (Long)prop.value() : null;
    }

    /**
     * Returns the property value or {@code null} if there is no such property or if property is not of {@link Type#BOOLEAN} type.
     *
     * @param name Property name.
     *
     * @return Property value or {@code null}.
     */
    default Boolean boolProperty(String name) {
        ServiceProperty<?> prop = property(name);

        return prop != null && prop.type() == Type.BOOLEAN ? (Boolean)prop.value() : null;
    }

    /**
     * Returns the property value or {@code null} if there is no such property or if property is not of {@link Type#STRING} type.
     *
     * @param name Property name.
     *
     * @return Property value or {@code null}.
     */
    default String stringProperty(String name) {
        ServiceProperty<?> prop = property(name);

        return prop != null && prop.type() == Type.STRING ? (String)prop.value() : null;
    }
}
