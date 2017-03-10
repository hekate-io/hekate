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
import io.hekate.messaging.broadcast.BroadcastCallback;
import io.hekate.messaging.broadcast.BroadcastResult;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BroadcastContext<T> implements BroadcastResult<T> {
    private static final Logger log = LoggerFactory.getLogger(BroadcastContext.class);

    private final T message;

    private final Set<ClusterNode> nodes;

    @ToStringIgnore
    private final BroadcastCallback<T> callback;

    private Map<ClusterNode, Throwable> errors;

    @ToStringIgnore
    private int remaining;

    public BroadcastContext(T message, Set<ClusterNode> nodes, BroadcastCallback<T> callback) {
        assert message != null : "Message is null.";
        assert nodes != null : "Nodes set is null.";
        assert !nodes.isEmpty() : "Nodes set is empty.";
        assert callback != null : "Callback is null.";

        this.message = message;
        this.nodes = nodes;
        this.callback = callback;

        remaining = nodes.size();
    }

    @Override
    public T getMessage() {
        return message;
    }

    @Override
    public Set<ClusterNode> getNodes() {
        return nodes;
    }

    @Override
    public Map<ClusterNode, Throwable> getErrors() {
        // Safe to access in non-synchronized context since this method can be called only after all errors were gathered.
        return errors == null ? Collections.emptyMap() : errors;
    }

    @Override
    public Throwable getError(ClusterNode node) {
        // Safe to access in non-synchronized context since this method can be called only after all errors were gathered.
        return errors != null ? errors.get(node) : null;
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

    boolean onSendSuccess(ClusterNode node) {
        boolean ready = false;

        synchronized (this) {
            remaining--;

            if (remaining == 0) {
                ready = true;
            }
        }

        try {
            callback.onSendSuccess(message, node);
        } catch (RuntimeException | Error e) {
            log.error("Got an unexpected runtime error while notifying a broadcast callback.", e);
        }

        return ready;
    }

    boolean onSendFailure(ClusterNode node, Throwable error) {
        boolean ready = false;

        synchronized (this) {
            if (errors == null) {
                errors = new HashMap<>(remaining, 1.0f);
            }

            errors.put(node, error);

            remaining--;

            if (remaining == 0) {
                ready = true;
            }
        }

        try {
            callback.onSendFailure(message, node, error);
        } catch (RuntimeException | Error e) {
            log.error("Got an unexpected runtime error while notifying a broadcast callback.", e);
        }

        return ready;
    }

    void complete() {
        try {
            callback.onComplete(null, this);
        } catch (RuntimeException | Error e) {
            log.error("Got an unexpected runtime error while notifying a broadcast callback.", e);
        }
    }

    @Override
    public String toString() {
        return ToString.format(BroadcastResult.class, this);
    }
}
