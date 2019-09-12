/*
 * Copyright 2019 The Hekate Project
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
import io.hekate.messaging.operation.BroadcastFuture;
import io.hekate.messaging.operation.BroadcastResult;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class BroadcastContext<T> implements BroadcastResult<T> {
    private final T message;

    private final List<ClusterNode> nodes;

    @ToStringIgnore
    private final BroadcastFuture<T> future;

    private Map<ClusterNode, Throwable> errors;

    @ToStringIgnore
    private int remaining;

    public BroadcastContext(T message, List<ClusterNode> nodes, BroadcastFuture<T> future) {
        assert message != null : "Message is null.";
        assert nodes != null : "Nodes set is null.";
        assert !nodes.isEmpty() : "Nodes set is empty.";
        assert future != null : "Future is null.";

        this.message = message;
        this.nodes = new ArrayList<>(nodes); // Copy since node list can be modified.
        this.remaining = nodes.size();
        this.future = future;
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
            if (nodes.remove(node)) {
                remaining--;
            }

            return remaining == 0;
        }
    }

    public BroadcastFuture<T> future() {
        return future;
    }

    boolean onSendSuccess() {
        synchronized (this) {
            remaining--;

            return remaining == 0;
        }
    }

    boolean onSendFailure(ClusterNode node, Throwable error) {
        synchronized (this) {
            if (errors == null) {
                errors = new HashMap<>(remaining, 1.0f);
            }

            errors.put(node, error);

            remaining--;

            return remaining == 0;
        }
    }

    void complete() {
        future.complete(this);
    }

    @Override
    public String toString() {
        return ToString.format(BroadcastResult.class, this);
    }
}
