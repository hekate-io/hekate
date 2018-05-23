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
import io.hekate.messaging.broadcast.BroadcastCallback;
import io.hekate.messaging.broadcast.BroadcastResult;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;

class BroadcastContext<T> implements BroadcastResult<T> {
    private static final Logger log = LoggerFactory.getLogger(BroadcastContext.class);

    private final T message;

    @ToStringIgnore
    private final BroadcastCallback<T> callback;

    private List<ClusterNode> nodes;

    private Map<ClusterNode, Throwable> errors;

    @ToStringIgnore
    private int remaining;

    public BroadcastContext(T message, List<ClusterNode> nodes, BroadcastCallback<T> callback) {
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
    public T message() {
        return message;
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

    public boolean forgetNode(ClusterNode node) {
        synchronized (this) {
            nodes = Collections.unmodifiableList(nodes.stream().filter(n -> !n.equals(node)).collect(toList()));

            remaining--;

            return remaining == 0;
        }
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
