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

package io.hekate.cluster;

import io.hekate.core.service.Service;

/**
 * Base interface for types that support cluster topology filtering.
 *
 * @param <T> Real type that implements/extends this interface.
 */
public interface ClusterFilterSupport<T extends ClusterFilterSupport<T>> {
    /**
     * Applies the specified filter to this instance.
     *
     * @param filter Filter.
     *
     * @return Filtered instance.
     */
    T filterAll(ClusterFilter filter);

    /**
     * Applies the specified filter to this instance.
     *
     * @param filter Filter.
     *
     * @return Filtered instance.
     */
    default T filter(ClusterNodeFilter filter) {
        return filterAll(ClusterFilters.forFilter(filter));
    }

    /**
     * Applies {@link ClusterFilters#forRole(String)} to this instance.
     *
     * @param role Role (see {@link ClusterNode#hasRole(String)}).
     *
     * @return Filtered instance.
     */
    default T forRole(String role) {
        return filterAll(ClusterFilters.forRole(role));
    }

    /**
     * Applies {@link ClusterFilters#forProperty(String)} to this instance.
     *
     * @param name Property name (see {@link ClusterNode#hasProperty(String)}).
     *
     * @return Filtered instance.
     */
    default T forProperty(String name) {
        return filterAll(ClusterFilters.forProperty(name));
    }

    /**
     * Applies {@link ClusterFilters#forProperty(String, String)} to this instance.
     *
     * @param name Property name (see {@link ClusterNode#getProperty(String)}).
     * @param value Property value.
     *
     * @return Filtered instance.
     */
    default T forProperty(String name, String value) {
        return filterAll(ClusterFilters.forProperty(name, value));
    }

    /**
     * Applies {@link ClusterFilters#forService(Class)} to this instance.
     *
     * @param type Service type (see {@link ClusterNode#hasService(String)})
     *
     * @return Filtered instance.
     */
    default T forService(Class<? extends Service> type) {
        return filterAll(ClusterFilters.forService(type));
    }

    /**
     * Applies {@link ClusterFilters#forRemotes()} to this instance.
     *
     * @return Filtered instance.
     */
    default T forRemotes() {
        return filterAll(ClusterFilters.forRemotes());
    }

    /**
     * Applies {@link ClusterFilters#forNode(ClusterNode)} to this instance.
     *
     * @param node Node.
     *
     * @return Filtered instance.
     */
    default T forNode(ClusterNode node) {
        return filterAll(ClusterFilters.forNode(node));
    }

    /**
     * Applies {@link ClusterFilters#forNode(ClusterNodeId)} to this instance.
     *
     * @param id Node.
     *
     * @return Filtered instance.
     */
    default T forNode(ClusterNodeId id) {
        return filterAll(ClusterFilters.forNode(id));
    }

    /**
     * Applies {@link ClusterFilters#forOldest()} to this instance.
     *
     * @return Filter.
     */
    default T forOldest() {
        return filterAll(ClusterFilters.forOldest());
    }

    /**
     * Applies {@link ClusterFilters#forYoungest()} to this instance.
     *
     * @return Filtered instance.
     */
    default T forYoungest() {
        return filterAll(ClusterFilters.forYoungest());
    }

    /**
     * Applies {@link ClusterFilters#forNext()} to this instance.
     *
     * @return Filtered instance.
     */
    default T forNext() {
        return filterAll(ClusterFilters.forNext());
    }

    /**
     * Applies {@link ClusterFilters#forNextInJoinOrder()} to this instance.
     *
     * @return Filtered instance.
     */
    default T forNextInJoinOrder() {
        return filterAll(ClusterFilters.forNextInJoinOrder());
    }
}
