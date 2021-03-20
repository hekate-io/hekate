/*
 * Copyright 2021 The Hekate Project
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
import io.hekate.messaging.operation.AggregateResult;
import io.hekate.util.format.ToString;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

class EmptyAggregateResult<T> implements AggregateResult<T> {
    private final T request;

    public EmptyAggregateResult(T request) {
        this.request = request;
    }

    @Override
    public T request() {
        return request;
    }

    @Override
    public List<ClusterNode> nodes() {
        return emptyList();
    }

    @Override
    public Map<ClusterNode, Throwable> errors() {
        return emptyMap();
    }

    @Override
    public Map<ClusterNode, T> resultsByNode() {
        return emptyMap();
    }

    @Override
    public String toString() {
        return ToString.format(AggregateResult.class, this);
    }
}
