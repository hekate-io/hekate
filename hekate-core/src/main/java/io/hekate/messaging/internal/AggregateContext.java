/*
 * Copyright 2022 The Hekate Project
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
import io.hekate.messaging.MessageTimeoutException;
import io.hekate.messaging.operation.AggregateFuture;
import io.hekate.messaging.operation.AggregateResult;
import io.hekate.messaging.operation.Response;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

class AggregateContext<T> implements AggregateResult<T> {
    private final T request;

    private final Map<ClusterNode, T> results;

    @ToStringIgnore
    private final List<ClusterNode> nodes;

    @ToStringIgnore
    private final AggregateFuture<T> future;

    private Map<ClusterNode, Throwable> errors;

    public AggregateContext(T request, List<ClusterNode> nodes, AggregateFuture<T> future) {
        this.request = request;
        this.nodes = new ArrayList<>(nodes); // Copy since node list can be modified.
        this.future = future;
        this.results = new HashMap<>(nodes.size(), 1.0f);
    }

    @Override
    public T request() {
        return request;
    }

    @Override
    public List<ClusterNode> nodes() {
        synchronized (this) {
            return nodes;
        }
    }

    @Override
    public Map<ClusterNode, Throwable> errors() {
        synchronized (this) {
            return errors == null ? emptyMap() : errors;
        }
    }

    @Override
    public Map<ClusterNode, T> resultsByNode() {
        synchronized (this) {
            return results;
        }
    }

    boolean forgetNode(ClusterNode node) {
        synchronized (this) {
            nodes.remove(node);

            return isReady();
        }
    }

    boolean onReplySuccess(ClusterNode node, Response<T> rsp) {
        synchronized (this) {
            results.put(node, rsp.payload());

            return isReady();
        }
    }

    boolean onReplyFailure(ClusterNode node, Throwable error) {
        synchronized (this) {
            if (errors == null) {
                errors = new HashMap<>(nodes.size(), 1.0f);
            }

            errors.put(node, error);

            return isReady();
        }
    }

    void complete() {
        future.complete(this);
    }

    boolean isReady() {
        assert Thread.holdsLock(this) : "Thread must hold lock on mutex.";

        return nodes.size() == results.size() + (errors == null ? 0 : errors.size());
    }

    boolean isTimedOut() {
        return errors().values().stream().anyMatch(e -> e instanceof MessageTimeoutException);
    }

    AggregateFuture<T> future() {
        return future;
    }

    @Override
    public String toString() {
        return ToString.format(AggregateResult.class, this);
    }
}
