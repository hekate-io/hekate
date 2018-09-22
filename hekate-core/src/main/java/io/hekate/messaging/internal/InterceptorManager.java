package io.hekate.messaging.internal;

import io.hekate.core.internal.util.StreamUtils;
import io.hekate.messaging.intercept.ClientMessageInterceptor;
import io.hekate.messaging.intercept.ClientReceiveContext;
import io.hekate.messaging.intercept.ClientSendContext;
import io.hekate.messaging.intercept.MessageInterceptor;
import io.hekate.messaging.intercept.ServerMessageInterceptor;
import io.hekate.messaging.intercept.ServerReceiveContext;
import io.hekate.messaging.intercept.ServerSendContext;
import java.util.Collection;

class InterceptorManager<T> {
    private final ClientMessageInterceptor<T>[] clients;

    private final ServerMessageInterceptor<T>[] servers;

    @SuppressWarnings("unchecked")
    public InterceptorManager(Collection<MessageInterceptor<?>> interceptors) {
        this.clients = StreamUtils.nullSafe(interceptors)
            .filter(it -> it instanceof ClientMessageInterceptor)
            .toArray(ClientMessageInterceptor[]::new);

        this.servers = StreamUtils.nullSafe(interceptors)
            .filter(it -> it instanceof ServerMessageInterceptor)
            .toArray(ServerMessageInterceptor[]::new);
    }

    public T clientSend(T msg, ClientSendContext ctx) {
        for (ClientMessageInterceptor<T> interceptor : clients) {
            T transformed = interceptor.beforeClientSend(msg, ctx);

            if (transformed != null) {
                msg = transformed;
            }
        }

        return msg;
    }

    public T clientReceive(T msg, ClientReceiveContext rsp, ClientSendContext ctx) {
        for (ClientMessageInterceptor<T> interceptor : clients) {
            T transformed = interceptor.beforeClientReceiveResponse(msg, rsp, ctx);

            if (transformed != null) {
                msg = transformed;
            }
        }

        return msg;
    }

    public void clientReceiveError(Throwable err, ClientSendContext ctx) {
        for (ClientMessageInterceptor<T> interceptor : clients) {
            interceptor.onClientReceiveError(err, ctx);
        }
    }

    public void clientReceiveVoid(ClientSendContext ctx) {
        for (ClientMessageInterceptor<T> interceptor : clients) {
            interceptor.onClientReceiveConfirmation(ctx);
        }
    }

    public T serverReceive(T msg, ServerReceiveContext ctx) {
        for (ServerMessageInterceptor<T> interceptor : servers) {
            T transformed = interceptor.beforeServerReceive(msg, ctx);

            if (transformed != null) {
                msg = transformed;
            }
        }

        return msg;
    }

    public void serverReceiveComplete(T msg, ServerReceiveContext rcvCtx) {
        for (ServerMessageInterceptor<T> interceptor : servers) {
            interceptor.onServerReceiveComplete(msg, rcvCtx);
        }
    }

    public T serverSend(T msg, ServerSendContext rspCtx, ServerReceiveContext rcvCtx) {
        for (ServerMessageInterceptor<T> interceptor : servers) {
            T transformed = interceptor.beforeServerSend(msg, rspCtx, rcvCtx);

            if (transformed != null) {
                msg = transformed;
            }
        }

        return msg;
    }
}
