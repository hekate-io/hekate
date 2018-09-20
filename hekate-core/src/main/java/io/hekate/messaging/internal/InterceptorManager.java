package io.hekate.messaging.internal;

import io.hekate.core.internal.util.StreamUtils;
import io.hekate.messaging.intercept.ClientSendContext;
import io.hekate.messaging.intercept.MessageInterceptor;
import io.hekate.messaging.intercept.ResponseContext;
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
                T transformed = interceptor.interceptClientSend(msg, ctx);

                if (transformed != null) {
                    msg = transformed;
                }
            }

            return msg;
        }

        return msg;
    }

    public T clientReceive(T msg, ResponseContext rsp, ClientSendContext ctx) {
        if (interceptors != null) {
            for (MessageInterceptor<T> interceptor : interceptors) {
                T transformed = interceptor.interceptClientReceive(msg, rsp, ctx);

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
                interceptor.interceptClientReceiveError(err, ctx);
            }
        }
    }

    public void clientReceiveVoid(ClientSendContext ctx) {
        if (interceptors != null) {
            for (MessageInterceptor<T> interceptor : interceptors) {
                interceptor.interceptClientReceiveVoid(ctx);
            }
        }
    }

    public T serverReceive(T msg, ServerReceiveContext ctx) {
        if (interceptors != null) {
            for (MessageInterceptor<T> interceptor : interceptors) {
                T transformed = interceptor.interceptServerReceive(msg, ctx);

                if (transformed != null) {
                    msg = transformed;
                }
            }

            return msg;
        }

        return msg;
    }

    public T serverSend(T msg, ResponseContext rspCtx, ServerReceiveContext rcvCtx) {
        if (interceptors != null) {
            for (MessageInterceptor<T> interceptor : interceptors) {
                T transformed = interceptor.interceptServerSend(msg, rspCtx, rcvCtx);

                if (transformed != null) {
                    msg = transformed;
                }
            }

            return msg;
        }

        return msg;
    }
}
