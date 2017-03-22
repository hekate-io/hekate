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
import io.hekate.messaging.unicast.RequestCallback;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.network.NetworkFuture;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

class InMemoryClientPool<T> implements ClientPool<T> {
    private static class AlwaysAsyncExecutor implements AffinityExecutor {
        private final AffinityExecutor delegate;

        public AlwaysAsyncExecutor(AffinityExecutor delegate) {
            assert delegate != null : "Delegate is null.";

            this.delegate = delegate;
        }

        @Override
        public boolean isAsync() {
            // Force in-memory clients to perform all operations asynchronously.
            return true;
        }

        @Override
        public AffinityWorker workerFor(int affinity) {
            return delegate.workerFor(affinity);
        }

        @Override
        public void terminate() {
            delegate.terminate();
        }

        @Override
        public void awaitTermination() throws InterruptedException {
            delegate.awaitTermination();
        }

        @Override
        public int getThreadPoolSize() {
            return delegate.getThreadPoolSize();
        }
    }

    private final ClusterNode node;

    private final Optional<ClusterNode> nodeOpt;

    @ToStringIgnore
    private final InMemoryReceiverContext<T> client;

    public InMemoryClientPool(ClusterNode node, MessagingGateway<T> gateway) {
        assert node != null : "Cluster node is null.";
        assert gateway != null : "Gateway is null.";

        this.node = node;
        this.nodeOpt = Optional.of(node);

        AlwaysAsyncExecutor wrapAsync = new AlwaysAsyncExecutor(gateway.getAsync());

        client = new InMemoryReceiverContext<>(gateway, 0, 1, wrapAsync);
    }

    @Override
    public ClusterNode getNode() {
        return node;
    }

    @Override
    public Optional<ClusterNode> getNodeOpt() {
        return nodeOpt;
    }

    @Override
    public void disconnectIfIdle() {
        // No-op.
    }

    @Override
    public void send(MessageContext<T> ctx, SendCallback callback) {
        client.sendNotification(ctx, callback);
    }

    @Override
    public void request(MessageContext<T> ctx, RequestCallback<T> callback) {
        client.sendRequest(ctx, callback);
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
