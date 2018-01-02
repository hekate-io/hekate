package io.hekate.rpc.internal;

import io.hekate.messaging.Message;
import io.hekate.messaging.MessagingEndpoint;
import io.hekate.rpc.RpcMethodInfo;
import io.hekate.rpc.internal.RpcProtocol.CompactCallRequest;
import io.hekate.rpc.internal.RpcProtocol.ErrorResponse;
import io.hekate.rpc.internal.RpcProtocol.NullResponse;
import io.hekate.rpc.internal.RpcProtocol.ObjectResponse;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RpcMethodHandler {
    private static final Logger log = LoggerFactory.getLogger(RpcMethodHandler.class);

    private final Object target;

    private final RpcMethodInfo method;

    public RpcMethodHandler(RpcMethodInfo method, Object target) {
        assert method != null : "Method info is null.";
        assert target != null : "Target is null.";

        this.method = method;
        this.target = target;
    }

    public Object target() {
        return target;
    }

    public RpcMethodInfo method() {
        return method;
    }

    public void handle(Message<RpcProtocol> msg) {
        // Enforces type check.
        CompactCallRequest call = msg.get(CompactCallRequest.class);

        doHandle(call.args(), msg.endpoint(), (err, result) -> {
            if (err == null) {
                if (result == null) {
                    // Null result (works for void method too).
                    msg.reply(NullResponse.INSTANCE);
                } else {
                    // Non-null result.
                    msg.reply(new ObjectResponse(result));
                }
            } else {
                msg.reply(new ErrorResponse(err));
            }
        });
    }

    protected void doHandle(Object[] args, MessagingEndpoint<RpcProtocol> from, BiConsumer<Throwable, Object> callback) {
        try {
            Object result;

            if (args == null) {
                // Call without arguments.
                result = method.javaMethod().invoke(target);
            } else {
                // Call with arguments.
                result = method.javaMethod().invoke(target, args);
            }

            if (result == null) {
                // Synchronous null result (works for void method too).
                callback.accept(null, null);
            } else if (result instanceof CompletableFuture<?>) {
                // Setup handling of asynchronous result.
                CompletableFuture<?> future = (CompletableFuture<?>)result;

                future.whenComplete((asyncResult, asyncErr) -> {
                    if (asyncErr == null) {
                        callback.accept(null, asyncResult);
                    } else {
                        if (asyncErr instanceof CompletionException && asyncErr.getCause() != null) {
                            // Unwrap asynchronous error.
                            callback.accept(asyncErr.getCause(), null);
                        } else {
                            // Return error as is.
                            callback.accept(asyncErr, null);
                        }
                    }
                });
            } else {
                // Synchronous non-null result.
                callback.accept(null, result);
            }
        } catch (InvocationTargetException e) {
            // Unwrap reflections error.
            Throwable cause = e.getCause();

            if (log.isErrorEnabled()) {
                log.error("RPC failure [from-node-id={}, method={}, target={}]", from.remoteNodeId(), method, target, cause);
            }

            callback.accept(cause, null);
        } catch (Throwable t) {
            if (log.isErrorEnabled()) {
                log.error("RPC failure [from-node-id={}, method={}, target={}]", from.remoteNodeId(), method, target, t);
            }

            // Return error as is.
            callback.accept(t, null);
        }
    }
}
