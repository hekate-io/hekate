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

package io.hekate.core;

import io.hekate.cluster.ClusterNode;
import java.util.Map;
import java.util.Set;

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
    Map<String, Set<String>> properties();

    /**
     * Returns an immutable set of property values.
     *
     * @param name Property name.
     *
     * @return Immutable set of property values.
     */
    Set<String> property(String name);
}
