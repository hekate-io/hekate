/*
 * Copyright 2020 The Hekate Project
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

import io.hekate.messaging.intercept.ClientMessageInterceptor;
import io.hekate.messaging.intercept.ClientReceiveContext;
import io.hekate.messaging.intercept.ClientSendContext;
import io.hekate.messaging.intercept.MessageInterceptor;
import io.hekate.messaging.intercept.ServerInboundContext;
import io.hekate.messaging.intercept.ServerMessageInterceptor;
import io.hekate.messaging.intercept.ServerReceiveContext;
import io.hekate.messaging.intercept.ServerSendContext;
import java.util.Collection;

import static io.hekate.core.internal.util.StreamUtils.nullSafe;

class MessageInterceptors<T> {
    private final ClientMessageInterceptor<T>[] clients;

    private final ServerMessageInterceptor<T>[] servers;

    @SuppressWarnings("unchecked")
    public MessageInterceptors(Collection<MessageInterceptor> interceptors) {
        this.clients = nullSafe(interceptors)
            .filter(it -> it instanceof ClientMessageInterceptor)
            .map(it -> (ClientMessageInterceptor)it)
            .toArray(ClientMessageInterceptor[]::new);

        this.servers = nullSafe(interceptors)
            .filter(it -> it instanceof ServerMessageInterceptor)
            .map(it -> (ServerMessageInterceptor)it)
            .toArray(ServerMessageInterceptor[]::new);
    }

    public void clientSend(ClientSendContext<T> ctx) {
        for (ClientMessageInterceptor<T> interceptor : clients) {
            interceptor.interceptClientSend(ctx);
        }
    }

    public void clientReceive(ClientReceiveContext<T> ctx) {
        for (ClientMessageInterceptor<T> interceptor : clients) {
            interceptor.interceptClientReceiveResponse(ctx);
        }
    }

    public void clientReceiveError(Throwable err, ClientSendContext<T> ctx) {
        for (ClientMessageInterceptor<T> interceptor : clients) {
            interceptor.interceptClientReceiveError(ctx, err);
        }
    }

    public void clientReceiveConfirmation(ClientSendContext<T> ctx) {
        for (ClientMessageInterceptor<T> interceptor : clients) {
            interceptor.interceptClientReceiveAck(ctx);
        }
    }

    public void serverReceive(ServerReceiveContext<T> ctx) {
        for (ServerMessageInterceptor<T> interceptor : servers) {
            interceptor.interceptServerReceive(ctx);
        }
    }

    public void serverReceiveComplete(ServerInboundContext<T> ctx) {
        for (ServerMessageInterceptor<T> interceptor : servers) {
            interceptor.interceptServerReceiveComplete(ctx);
        }
    }

    public void serverSend(ServerSendContext<T> ctx) {
        for (ServerMessageInterceptor<T> interceptor : servers) {
            interceptor.interceptServerSend(ctx);
        }
    }
}
