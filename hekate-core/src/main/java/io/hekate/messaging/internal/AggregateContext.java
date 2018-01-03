/*
 * Copyright 2018 The Hekate Project
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
import io.hekate.messaging.broadcast.AggregateCallback;
import io.hekate.messaging.broadcast.AggregateResult;
import io.hekate.messaging.unicast.Response;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;

class AggregateContext<T> implements AggregateResult<T> {
    private static final Logger log = LoggerFactory.getLogger(AggregateContext.class);

    private final T request;

    private final Map<ClusterNode, T> results;

    @ToStringIgnore
    private final AggregateCallback<T> callback;

    private List<ClusterNode> nodes;

    private Map<ClusterNode, Throwable> errors;

    public AggregateContext(T request, List<ClusterNode> nodes, AggregateCallback<T> callback) {
        assert request != null : "Request is null.";
        assert nodes != null : "Nodes set is null.";
        assert !nodes.isEmpty() : "Nodes set is empty.";
        assert callback != null : "Callback is null.";

        this.request = request;
        this.nodes = nodes;
        this.callback = callback;
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
            return errors == null ? Collections.emptyMap() : errors;
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
            nodes = Collections.unmodifiableList(nodes.stream().filter(n -> !n.equals(node)).collect(toList()));

            return isReady();
        }
    }

    boolean onReplySuccess(ClusterNode node, Response<T> rsp) {
        boolean ready = false;

        synchronized (this) {
            results.put(node, rsp.get());

            if (isReady()) {
                ready = true;
            }
        }

        try {
            callback.onReplySuccess(rsp, node);
        } catch (RuntimeException | Error e) {
            log.error("Got an unexpected runtime error while notifying aggregation callback.", e);
        }

        return ready;
    }

    boolean onReplyFailure(ClusterNode node, Throwable error) {
        boolean ready = false;

        synchronized (this) {
            if (errors == null) {
                errors = new HashMap<>(nodes.size(), 1.0f);
            }

            errors.put(node, error);

            if (isReady()) {
                ready = true;
            }
        }

        try {
            callback.onReplyFailure(request, node, error);
        } catch (RuntimeException | Error e) {
            log.error("Got an unexpected runtime error while notifying aggregation callback.", e);
        }

        return ready;
    }

    void complete() {
        try {
            callback.onComplete(null, this);
        } catch (RuntimeException | Error e) {
            log.error("Got an unexpected runtime error while notifying aggregation callback.", e);
        }
    }

    boolean isReady() {
        assert Thread.holdsLock(this) : "Thread must hold lock on mutex.";

        return nodes.size() == results.size() + (errors == null ? 0 : errors.size());
    }

    @Override
    public String toString() {
        return ToString.format(AggregateResult.class, this);
    }
}
