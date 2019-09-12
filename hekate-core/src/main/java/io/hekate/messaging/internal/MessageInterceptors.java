package io.hekate.messaging.internal;

import io.hekate.core.internal.util.StreamUtils;
import io.hekate.messaging.intercept.ClientMessageInterceptor;
import io.hekate.messaging.intercept.ClientReceiveContext;
import io.hekate.messaging.intercept.ClientSendContext;
import io.hekate.messaging.intercept.MessageInterceptor;
import io.hekate.messaging.intercept.ServerInboundContext;
import io.hekate.messaging.intercept.ServerMessageInterceptor;
import io.hekate.messaging.intercept.ServerReceiveContext;
import io.hekate.messaging.intercept.ServerSendContext;
import java.util.Collection;

class MessageInterceptors<T> {
    private final ClientMessageInterceptor<T>[] clients;

    private final ServerMessageInterceptor<T>[] servers;

    @SuppressWarnings("unchecked")
    public MessageInterceptors(Collection<MessageInterceptor> interceptors) {
        this.clients = StreamUtils.nullSafe(interceptors)
            .filter(it -> it instanceof ClientMessageInterceptor)
            .map(it -> (ClientMessageInterceptor)it)
            .toArray(ClientMessageInterceptor[]::new);

        this.servers = StreamUtils.nullSafe(interceptors)
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
