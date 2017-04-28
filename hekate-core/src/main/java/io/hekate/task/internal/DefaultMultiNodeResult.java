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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

class DefaultMultiNodeResult<T> implements MultiNodeResult<T> {
    private final List<ClusterNode> nodes;

    private final Map<ClusterNode, T> results;

    private final Map<ClusterNode, Throwable> errors;

    public DefaultMultiNodeResult(List<ClusterNode> nodes, Map<ClusterNode, Throwable> errors, Map<ClusterNode, T> results) {
        assert nodes != null : "Nodes set is null.";
        assert errors != null : "Errors map is null.";
        assert results != null : "Result map is null.";

        this.nodes = nodes;
        this.errors = errors;
        this.results = results;
    }

    @Override
    public List<ClusterNode> nodes() {
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
    public T resultOf(ClusterNode node) {
        return results.get(node);
    }

    @Override
    public Throwable errorOf(ClusterNode node) {
        return errors.get(node);
    }

    @Override
    public Collection<T> results() {
        return resultsByNode().values();
    }

    @Override
    public Stream<T> stream() {
        return results().stream();
    }

    @Override
    public Map<ClusterNode, T> resultsByNode() {
        return results;
    }

    @Override
    public Map<ClusterNode, Throwable> errors() {
        return errors;
    }

    @Override
    public String toString() {
        return ToString.format(MultiNodeResult.class, this);
    }
}
