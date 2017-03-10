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
import io.hekate.messaging.broadcast.AggregateCallback;
import io.hekate.messaging.broadcast.AggregateResult;
import io.hekate.messaging.unicast.Reply;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AggregateContext<T> implements AggregateResult<T> {
    private static final Logger log = LoggerFactory.getLogger(AggregateContext.class);

    private final T request;

    private final Set<ClusterNode> nodes;

    private final Map<ClusterNode, Reply<T>> responses;

    @ToStringIgnore
    private final AggregateCallback<T> callback;

    private Map<ClusterNode, Throwable> errors;

    public AggregateContext(T request, Set<ClusterNode> nodes, AggregateCallback<T> callback) {
        assert request != null : "Request is null.";
        assert nodes != null : "Nodes set is null.";
        assert !nodes.isEmpty() : "Nodes set is empty.";
        assert callback != null : "Callback is null.";

        this.request = request;
        this.nodes = nodes;
        this.callback = callback;
        this.responses = new HashMap<>(nodes.size(), 1.0f);
    }

    @Override
    public T getRequest() {
        return request;
    }

    @Override
    public Set<ClusterNode> getNodes() {
        return nodes;
    }

    @Override
    public Map<ClusterNode, Throwable> getErrors() {
        synchronized (this) {
            return errors == null ? Collections.emptyMap() : errors;
        }
    }

    @Override
    public Throwable getError(ClusterNode node) {
        synchronized (this) {
            return errors != null ? errors.get(node) : null;
        }
    }

    @Override
    public boolean isSuccess() {
        // Safe to access in non-synchronized context since this method can be called only after all errors were gathered.
        return errors == null;
    }

    @Override
    public boolean isSuccess(ClusterNode node) {
        return getError(node) == null;
    }

    @Override
    public Map<ClusterNode, Reply<T>> getReplies() {
        // Safe to access in non-synchronized context since this method can be called only after all responses were received.
        return responses;
    }

    @Override
    public Reply<T> getReply(ClusterNode node) {
        // Safe to access in non-synchronized context since this method can be called only after all responses were received.
        return responses.get(node);
    }

    boolean onReplySuccess(ClusterNode node, Reply<T> reply) {
        boolean ready = false;

        // Do not count partial replies.
        if (!reply.isPartial()) {
            synchronized (this) {
                responses.put(node, reply);

                if (isReady()) {
                    ready = true;
                }
            }
        }

        try {
            callback.onReplySuccess(reply, node);
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

    private boolean isReady() {
        assert Thread.holdsLock(this) : "Thread must hold lock on mutex.";

        return nodes.size() == responses.size() + (errors == null ? 0 : errors.size());
    }

    @Override
    public String toString() {
        return ToString.format(AggregateResult.class, this);
    }
}
