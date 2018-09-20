package io.hekate.messaging.internal;

import io.hekate.core.internal.util.StreamUtils;
import io.hekate.messaging.intercept.ClientReceiveContext;
import io.hekate.messaging.intercept.ClientSendContext;
import io.hekate.messaging.intercept.MessageInterceptor;
import io.hekate.messaging.intercept.ServerSendContext;
import io.hekate.messaging.intercept.ServerReceiveContext;
import java.util.Collection;
import java.util.List;

import static java.util.stream.Collectors.toList;

class InterceptorManager<T> {
    private static final MessageInterceptor[] EMPTY_INTERCEPTORS = new MessageInterceptor[0];

    private MessageInterceptor<T>[] interceptors;

    @SuppressWarnings("unchecked")
    public InterceptorManager(Collection<MessageInterceptor<?>> interceptors) {
        List<MessageInterceptor<?>> interceptorList = StreamUtils.nullSafe(interceptors).collect(toList());

        if (interceptorList.isEmpty()) {
            this.interceptors = null;
        } else {
            this.interceptors = interceptors.toArray(EMPTY_INTERCEPTORS);
        }
    }

    public T clientSend(T msg, ClientSendContext ctx) {
        if (interceptors != null) {
            for (MessageInterceptor<T> interceptor : interceptors) {
                T transformed = interceptor.beforeClientSend(msg, ctx);

                if (transformed != null) {
                    msg = transformed;
                }
            }

            return msg;
        }

        return msg;
    }

    public T clientReceive(T msg, ClientReceiveContext rsp, ClientSendContext ctx) {
        if (interceptors != null) {
            for (MessageInterceptor<T> interceptor : interceptors) {
                T transformed = interceptor.beforeClientReceiveResponse(msg, rsp, ctx);

                if (transformed != null) {
                    msg = transformed;
                }
            }

            return msg;
        }

        return msg;
    }

    public void clientReceiveError(Throwable err, ClientSendContext ctx) {
        if (interceptors != null) {
            for (MessageInterceptor<T> interceptor : interceptors) {
                interceptor.onClientReceiveError(err, ctx);
            }
        }
    }

    public void clientReceiveVoid(ClientSendContext ctx) {
        if (interceptors != null) {
            for (MessageInterceptor<T> interceptor : interceptors) {
                interceptor.onClientReceiveConfirmation(ctx);
            }
        }
    }

    public T serverReceive(T msg, ServerReceiveContext ctx) {
        if (interceptors != null) {
            for (MessageInterceptor<T> interceptor : interceptors) {
                T transformed = interceptor.beforeServerReceive(msg, ctx);

                if (transformed != null) {
                    msg = transformed;
                }
            }

            return msg;
        }

        return msg;
    }

    public void serverReceiveComplete(ServerReceiveContext rcvCtx) {
        if (interceptors != null) {
            for (MessageInterceptor<T> interceptor : interceptors) {
                interceptor.onServerReceiveComplete(rcvCtx);
            }
        }
    }

    public T serverSend(T msg, ServerSendContext rspCtx, ServerReceiveContext rcvCtx) {
        if (interceptors != null) {
            for (MessageInterceptor<T> interceptor : interceptors) {
                T transformed = interceptor.beforeServerSend(msg, rspCtx, rcvCtx);

                if (transformed != null) {
                    msg = transformed;
                }
            }

            return msg;
        }

        return msg;
    }
}
