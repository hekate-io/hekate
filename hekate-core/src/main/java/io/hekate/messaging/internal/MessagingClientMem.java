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
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.network.NetworkFuture;
import io.hekate.util.async.Waiting;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.Collections;
import java.util.List;

class MessagingClientMem<T> implements MessagingClient<T> {
    private static class AsyncEnforcedExecutor implements MessagingExecutor {
        private final MessagingExecutor delegate;

        public AsyncEnforcedExecutor(MessagingExecutor delegate) {
            assert delegate != null : "Delegate is null.";

            this.delegate = delegate;
        }

        @Override
        public boolean isAsync() {
            // Force in-memory clients to perform all operations asynchronously.
            return true;
        }

        @Override
        public MessagingWorker workerFor(int affinity) {
            return delegate.workerFor(affinity);
        }

        @Override
        public MessagingWorker pooledWorker() {
            return delegate.pooledWorker();
        }

        @Override
        public Waiting terminate() {
            return delegate.terminate();
        }

        @Override
        public int poolSize() {
            return delegate.poolSize();
        }

        @Override
        public int activeTasks() {
            return delegate.activeTasks();
        }

        @Override
        public long completedTasks() {
            return delegate.completedTasks();
        }
    }

    private final ClusterNode node;

    @ToStringIgnore
    private final MessagingConnectionMem<T> conn;

    public MessagingClientMem(ClusterNode node, MessagingGatewayContext<T> ctx) {
        assert node != null : "Cluster node is null.";
        assert ctx != null : "Messaging context is null.";

        this.node = node;

        AsyncEnforcedExecutor asyncEnforced = new AsyncEnforcedExecutor(ctx.async());

        conn = new MessagingConnectionMem<>(ctx, asyncEnforced);
    }

    @Override
    public ClusterNode node() {
        return node;
    }

    @Override
    public void send(MessageAttempt<T> attempt, SendCallback callback) {
        conn.send(attempt, callback);
    }

    @Override
    public void request(MessageAttempt<T> attempt, InternalRequestCallback<T> callback) {
        conn.request(attempt, callback);
    }

    @Override
    public void disconnectIfIdle() {
        // No-op.
    }

    @Override
    public void touch() {
        // No-op.
    }

    @Override
    public List<NetworkFuture<MessagingProtocol>> close() {
        return Collections.emptyList();
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
