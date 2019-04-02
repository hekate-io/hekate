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

import io.hekate.cluster.ClusterAddress;
import io.hekate.messaging.MessagingEndpoint;
import io.hekate.messaging.MessagingException;
import io.hekate.network.NetworkEndpoint;

abstract class MessagingConnection<T> {
    private final NetworkEndpoint<MessagingProtocol> net;

    private final MessagingGatewayContext<T> gateway;

    private final MessagingEndpoint<T> endpoint;

    private final ReceivePressureGuard receivePressure;

    public MessagingConnection(
        MessagingGatewayContext<T> gateway,
        MessagingEndpoint<T> endpoint,
        NetworkEndpoint<MessagingProtocol> net
    ) {
        assert gateway != null : "Messaging context is null.";
        assert endpoint != null : "Messaging endpoint is null.";
        assert net != null : "Network endpoint is null.";

        this.gateway = gateway;
        this.endpoint = endpoint;
        this.net = net;
        this.receivePressure = gateway.receiveGuard();
    }

    public MessagingGatewayContext<T> gateway() {
        return gateway;
    }

    public MessagingEndpoint<T> endpoint() {
        return endpoint;
    }

    public ClusterAddress remoteAddress() {
        return endpoint.remoteAddress();
    }

    public NetworkEndpoint<MessagingProtocol> network() {
        return net;
    }

    protected MessagingException wrapError(Throwable err) {
        if (err instanceof MessagingException) {
            return (MessagingException)err;
        } else {
            return new MessagingException("Messaging operation failure [node=" + remoteAddress() + ']', err);
        }
    }

    protected final void onReceiveAsyncEnqueue(NetworkEndpoint<MessagingProtocol> from) {
        if (receivePressure != null) {
            receivePressure.onEnqueue(from);
        }
    }

    protected final void onReceiveAsyncDequeue() {
        if (receivePressure != null) {
            receivePressure.onDequeue();
        }
    }
}
