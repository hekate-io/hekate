package io.hekate.rpc.internal;

import io.hekate.messaging.Message;
import io.hekate.rpc.RpcMethodInfo;
import io.hekate.rpc.internal.RpcProtocol.CompactCallRequest;
import io.hekate.rpc.internal.RpcProtocol.ErrorResponse;
import io.hekate.rpc.internal.RpcProtocol.NullResponse;
import io.hekate.rpc.internal.RpcProtocol.ObjectResponse;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RpcMethodHandler {
    private static final Logger log = LoggerFactory.getLogger(RpcMethodHandler.class);

    private final Object target;

    private final RpcMethodInfo info;

    public RpcMethodHandler(RpcMethodInfo info, Object target) {
        assert info != null : "Method info is null.";
        assert target != null : "Target is null.";

        this.info = info;
        this.target = target;
    }

    public Object target() {
        return target;
    }

    public RpcMethodInfo info() {
        return info;
    }

    public void handle(Message<RpcProtocol> msg) {
        // Enforces type check.
        CompactCallRequest call = msg.get(CompactCallRequest.class);

        try {
            Object result;

            if (call.args() == null) {
                // Call without arguments.
                result = info.javaMethod().invoke(target);
            } else {
                // Call with arguments.
                result = info.javaMethod().invoke(target, call.args());
            }

            if (result == null) {
                // Synchronous null result (works for void method too).
                msg.reply(NullResponse.INSTANCE);
            } else if (result instanceof CompletableFuture<?>) {
                // Setup handling of asynchronous result.
                CompletableFuture<?> future = (CompletableFuture<?>)result;

                future.whenComplete((asyncResult, asyncErr) -> {
                    if (asyncErr == null) {
                        if (asyncResult == null) {
                            // Asynchronous null result (works for void method too).
                            msg.reply(NullResponse.INSTANCE);
                        } else {
                            // Asynchronous non-null result.
                            msg.reply(new ObjectResponse(asyncResult));
                        }
                    } else {
                        if (asyncErr instanceof CompletionException && asyncErr.getCause() != null) {
                            // Unwrap asynchronous error.
                            msg.reply(new ErrorResponse(asyncErr.getCause()));
                        } else {
                            // Return error as is.
                            msg.reply(new ErrorResponse(asyncErr));
                        }
                    }
                });
            } else {
                // Synchronous non-null result.
                msg.reply(new ObjectResponse(result));
            }
        } catch (InvocationTargetException e) {
            // Unwrap reflections error.
            Throwable cause = e.getCause();

            if (log.isErrorEnabled()) {
                log.error("RPC failure [from-node-id={}, method={}, target={}]", msg.from(), info, target, cause);
            }

            msg.reply(new ErrorResponse(cause));
        } catch (Throwable t) {
            if (log.isErrorEnabled()) {
                log.error("RPC failure [from-node-id={}, method={}, target={}]", msg.from(), info, target, t);
            }

            // Return error as is.
            msg.reply(new ErrorResponse(t));
        }
    }
}
