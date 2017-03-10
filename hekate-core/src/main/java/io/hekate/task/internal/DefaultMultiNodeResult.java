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

package io.hekate.task.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.task.MultiNodeResult;
import io.hekate.util.format.ToString;
import java.util.Map;
import java.util.Set;

class DefaultMultiNodeResult<T> implements MultiNodeResult<T> {
    private final Set<ClusterNode> nodes;

    private final Map<ClusterNode, T> results;

    private final Map<ClusterNode, Throwable> errors;

    public DefaultMultiNodeResult(Set<ClusterNode> nodes, Map<ClusterNode, Throwable> errors, Map<ClusterNode, T> results) {
        this.nodes = nodes;
        this.errors = errors;
        this.results = results;
    }

    @Override
    public Set<ClusterNode> getNodes() {
        return nodes;
    }

    @Override
    public boolean isSuccess() {
        return errors.isEmpty();
    }

    @Override
    public boolean isSuccess(ClusterNode node) {
        return !errors.containsKey(node);
    }

    @Override
    public T getResult(ClusterNode node) {
        return results.get(node);
    }

    @Override
    public Throwable getError(ClusterNode node) {
        return errors.get(node);
    }

    @Override
    public Map<ClusterNode, T> getResults() {
        return results;
    }

    @Override
    public Map<ClusterNode, Throwable> getErrors() {
        return errors;
    }

    @Override
    public String toString() {
        return ToString.format(MultiNodeResult.class, this);
    }
}
