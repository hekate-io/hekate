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

import io.hekate.messaging.MessagingEndpoint;
import io.hekate.messaging.MessagingException;
import io.hekate.messaging.internal.MessagingProtocol.Connect;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkClientCallback;
import io.hekate.network.NetworkFuture;
import io.hekate.network.NetworkMessage;
import java.nio.channels.ClosedChannelException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

class MessagingConnectionNetOut<T> extends MessagingConnectionNetBase<T> {
    interface DisconnectCallback {
        void onDisconnect();
    }

    private static final AtomicIntegerFieldUpdater<MessagingConnectionNetOut> EPOCH_UPDATER = newUpdater(
        MessagingConnectionNetOut.class,
        "connectEpoch"
    );

    private final NetworkClient<MessagingProtocol> net;

    private final DisconnectCallback callback;

    private final Object mux;

    @SuppressWarnings("unused") // <-- Updated via AtomicIntegerFieldUpdater.
    private volatile int connectEpoch;

    public MessagingConnectionNetOut(
        NetworkClient<MessagingProtocol> net,
        MessagingGatewayContext<T> ctx,
        MessagingEndpoint<T> endpoint,
        Object mux,
        DisconnectCallback callback
    ) {
        super(net, ctx, endpoint);

        assert endpoint != null : "Endpoint is null.";
        assert mux != null : "Mutex must be not null.";
        assert callback != null : "Disconnect callback is null.";

        this.net = net;
        this.mux = mux;
        this.callback = callback;
    }

    public NetworkFuture<MessagingProtocol> connect() {
        Connect payload = new Connect(
            remoteAddress().id(),
            gateway().localNode().address(),
            gateway().channelId()
        );

        synchronized (mux) {
            // Update the connection's epoch.
            int localEpoch = EPOCH_UPDATER.incrementAndGet(this);

            return net.connect(remoteAddress().socket(), payload, new NetworkClientCallback<MessagingProtocol>() {
                @Override
                public void onMessage(NetworkMessage<MessagingProtocol> message, NetworkClient<MessagingProtocol> from) {
                    receive(message, from);
                }

                @Override
                public void onDisconnect(NetworkClient<MessagingProtocol> client, Optional<Throwable> cause) {
                    // Notify only if this is an internal disconnect.
                    synchronized (mux) {
                        if (localEpoch == connectEpoch) {
                            callback.onDisconnect();
                        }
                    }

                    discardRequests(localEpoch, wrapError(cause));
                }
            });
        }
    }

    @Override
    public NetworkFuture<MessagingProtocol> disconnect() {
        synchronized (mux) {
            return net.disconnect();
        }
    }

    public NetworkClient.State state() {
        return net.state();
    }

    @Override
    protected int epoch() {
        // Volatile read.
        return connectEpoch;
    }

    private MessagingException wrapError(Optional<Throwable> cause) {
        String msg = "Messaging operation failed [node=" + remoteAddress() + ']';

        return new MessagingException(msg, cause.orElseGet(ClosedChannelException::new));
    }
}
