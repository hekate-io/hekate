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

package io.hekate.cluster;

import io.hekate.core.service.Service;

/**
 * Base interface for types that support cluster topology filtering.
 *
 * @param <T> Real type that implements/extends this interface.
 */
public interface ClusterFilterSupport<T extends ClusterFilterSupport<T>> {
    /**
     * Returns a copy of this instance that will use the specified filter along with all the filter criteria that were inherited from this
     * instance.
     *
     * <p>
     * <b>Notice:</b> for performance reasons it is highly recommended to cache and reuse the filtered instance instead of re-building it
     * over and over again.
     * ${source: cluster/ClusterFilterSupportJavadocTest.java#bad_and_good_usage}
     * </p>
     *
     * @param filter Filter.
     *
     * @return Filtered instance.
     */
    T filterAll(ClusterFilter filter);

    /**
     * Returns a copy of this instance that will use the specified filter along with all the filter criteria that were inherited from this
     * instance.
     *
     * <p>
     * <b>Notice:</b> for performance reasons it is highly recommended to cache and reuse the filtered instance instead of re-building it
     * over and over again.
     * ${source: cluster/ClusterFilterSupportJavadocTest.java#bad_and_good_usage}
     * </p>
     *
     * @param filter Filter.
     *
     * @return Filtered instance.
     *
     * @see #forRemotes()
     * @see #forRole(String)
     * @see #forProperty(String)
     * @see #forNode(ClusterNode)
     * @see #forOldest()
     * @see #forYoungest()
     */
    default T filter(ClusterNodeFilter filter) {
        return filterAll(ClusterFilters.forFilter(filter));
    }

    /**
     * Returns a copy of this instance that will use {@link ClusterFilters#forRole(String)} filter along with all the filter criteria that
     * were inherited from this instance.
     *
     * <p>
     * <b>Notice:</b> for performance reasons it is highly recommended to cache and reuse the filtered instance instead of re-building it
     * over and over again.
     * ${source: cluster/ClusterFilterSupportJavadocTest.java#bad_and_good_usage}
     * </p>
     *
     * @param role Role (see {@link ClusterNode#hasRole(String)}).
     *
     * @return Filtered instance.
     */
    default T forRole(String role) {
        return filterAll(ClusterFilters.forRole(role));
    }

    /**
     * Returns a copy of this instance that will use {@link ClusterFilters#forProperty(String)} filter along with all the filter criteria
     * that were inherited from this instance.
     *
     * <p>
     * <b>Notice:</b> for performance reasons it is highly recommended to cache and reuse the filtered instance instead of re-building it
     * over and over again.
     * ${source: cluster/ClusterFilterSupportJavadocTest.java#bad_and_good_usage}
     * </p>
     *
     * @param name Property name (see {@link ClusterNode#hasProperty(String)}).
     *
     * @return Filtered instance.
     */
    default T forProperty(String name) {
        return filterAll(ClusterFilters.forProperty(name));
    }

    /**
     * Returns a copy of this instance that will use {@link ClusterFilters#forProperty(String, String)} filter along with all the filter
     * criteria that were inherited from this instance.
     *
     * <p>
     * <b>Notice:</b> for performance reasons it is highly recommended to cache and reuse the filtered instance instead of re-building it
     * over and over again.
     * ${source: cluster/ClusterFilterSupportJavadocTest.java#bad_and_good_usage}
     * </p>
     *
     * @param name Property name (see {@link ClusterNode#property(String)}).
     * @param value Property value.
     *
     * @return Filtered instance.
     */
    default T forProperty(String name, String value) {
        return filterAll(ClusterFilters.forProperty(name, value));
    }

    /**
     * Returns a copy of this instance that will use {@link ClusterFilters#forService(Class)} filter along with all the filter criteria that
     * were inherited from this instance.
     *
     * <p>
     * <b>Notice:</b> for performance reasons it is highly recommended to cache and reuse the filtered instance instead of re-building it
     * over and over again.
     * ${source: cluster/ClusterFilterSupportJavadocTest.java#bad_and_good_usage}
     * </p>
     *
     * @param type Service type (see {@link ClusterNode#hasService(Class)})
     *
     * @return Filtered instance.
     */
    default T forService(Class<? extends Service> type) {
        return filterAll(ClusterFilters.forService(type));
    }

    /**
     * Returns a copy of this instance that will use {@link ClusterFilters#forRemotes()} filter along with all the filter criteria that
     * were inherited from this instance.
     *
     * <p>
     * <b>Notice:</b> for performance reasons it is highly recommended to cache and reuse the filtered instance instead of re-building it
     * over and over again.
     * ${source: cluster/ClusterFilterSupportJavadocTest.java#bad_and_good_usage}
     * </p>
     *
     * @return Filtered instance.
     */
    default T forRemotes() {
        return filterAll(ClusterFilters.forRemotes());
    }

    /**
     * Returns a copy of this instance that will use {@link ClusterFilters#forNode(ClusterNode)} filter along with all the filter criteria
     * that were inherited from this instance.
     *
     * <p>
     * <b>Notice:</b> for performance reasons it is highly recommended to cache and reuse the filtered instance instead of re-building it
     * over and over again.
     * ${source: cluster/ClusterFilterSupportJavadocTest.java#bad_and_good_usage}
     * </p>
     *
     * @param node Node.
     *
     * @return Filtered instance.
     */
    default T forNode(ClusterNode node) {
        return filterAll(ClusterFilters.forNode(node));
    }

    /**
     * Returns a copy of this instance that will use {@link ClusterFilters#forNode(ClusterNodeId)} filter along with all the filter criteria
     * that were inherited from this instance.
     *
     * <p>
     * <b>Notice:</b> for performance reasons it is highly recommended to cache and reuse the filtered instance instead of re-building it
     * over and over again.
     * ${source: cluster/ClusterFilterSupportJavadocTest.java#bad_and_good_usage}
     * </p>
     *
     * @param id Node.
     *
     * @return Filtered instance.
     */
    default T forNode(ClusterNodeId id) {
        return filterAll(ClusterFilters.forNode(id));
    }

    /**
     * Returns a copy of this instance that will use {@link ClusterFilters#forOldest()} filter along with all the filter criteria that were
     * inherited from this instance.
     *
     * <p>
     * <b>Notice:</b> for performance reasons it is highly recommended to cache and reuse the filtered instance instead of re-building it
     * over and over again.
     * ${source: cluster/ClusterFilterSupportJavadocTest.java#bad_and_good_usage}
     * </p>
     *
     * @return Filter.
     */
    default T forOldest() {
        return filterAll(ClusterFilters.forOldest());
    }

    /**
     * Returns a copy of this instance that will use {@link ClusterFilters#forYoungest()} filter along with all the filter criteria that
     * were inherited from this instance.
     *
     * <p>
     * <b>Notice:</b> for performance reasons it is highly recommended to cache and reuse the filtered instance instead of re-building it
     * over and over again.
     * ${source: cluster/ClusterFilterSupportJavadocTest.java#bad_and_good_usage}
     * </p>
     *
     * @return Filtered instance.
     */
    default T forYoungest() {
        return filterAll(ClusterFilters.forYoungest());
    }
}
