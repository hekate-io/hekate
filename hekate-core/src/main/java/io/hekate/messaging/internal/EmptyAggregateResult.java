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

package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.messaging.broadcast.AggregateResult;
import io.hekate.util.format.ToString;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

class EmptyAggregateResult<T> implements AggregateResult<T> {
    private final T request;

    public EmptyAggregateResult(T request) {
        assert request != null : "Request is null.";

        this.request = request;
    }

    @Override
    public T request() {
        return request;
    }

    @Override
    public Set<ClusterNode> nodes() {
        return Collections.emptySet();
    }

    @Override
    public Map<ClusterNode, Throwable> errors() {
        return Collections.emptyMap();
    }

    @Override
    public Throwable errorOf(ClusterNode node) {
        return null;
    }

    @Override
    public boolean isSuccess() {
        return true;
    }

    @Override
    public boolean isSuccess(ClusterNode node) {
        return false;
    }

    @Override
    public Collection<T> results() {
        return Collections.emptyList();
    }

    @Override
    public Stream<T> stream() {
        return Stream.empty();
    }

    @Override
    public Map<ClusterNode, T> resultsByNode() {
        return Collections.emptyMap();
    }

    @Override
    public T resultOf(ClusterNode node) {
        return null;
    }

    @Override
    public String toString() {
        return ToString.format(AggregateResult.class, this);
    }
}
