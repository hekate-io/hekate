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
import java.util.List;
import java.util.Map;

/**
 * Provider of node properties for {@link HekateBootstrap}.
 *
 * <p>
 * Implementations of this interface are responsible for providing some dynamically obtained node properties (possibly from some third-party
 * sources) to {@link HekateBootstrap}. Such properties become a part of {@link HekateBootstrap#setNodeProperties(Map) node properties}.
 * </p>
 *
 * @see HekateBootstrap#setNodePropertyProviders(List)
 */
public interface NodePropertyProvider {
    /**
     * Returns a map of node properties that should become a part of {@link ClusterNode#getProperties() node properties}.
     *
     * <p>
     * Note that this method gets called only once during {@link Hekate} instance construction. All subsequent modifications of the returned
     * map will have no effect on {@link Hekate} instance.
     * </p>
     *
     * @return Map of node properties.
     */
    Map<String, String> getProperties();
}
