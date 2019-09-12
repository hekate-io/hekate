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

package io.hekate.rpc.internal;

import io.hekate.messaging.Message;
import io.hekate.messaging.MessagingEndpoint;
import io.hekate.rpc.RpcInterfaceInfo;
import io.hekate.rpc.RpcMethodInfo;
import io.hekate.rpc.internal.RpcProtocol.RpcCallError;
import io.hekate.rpc.internal.RpcProtocol.RpcCallNullResult;
import io.hekate.rpc.internal.RpcProtocol.RpcCallResult;
import io.hekate.rpc.internal.RpcProtocol.RpcCompactCall;
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

    private final RpcInterfaceInfo<?> rpc;

    public RpcMethodHandler(RpcInterfaceInfo<?> rpc, RpcMethodInfo method, Object target) {
        assert rpc != null : "RPC interface info is null.";
        assert method != null : "Method info is null.";
        assert target != null : "Target is null.";

        this.rpc = rpc;
        this.method = method;
        this.target = target;
    }

    public RpcInterfaceInfo<?> rpc() {
        return rpc;
    }

    public RpcMethodInfo method() {
        return method;
    }

    public Object target() {
        return target;
    }

    public void handle(Message<RpcProtocol> msg) {
        // Enforces type check.
        RpcCompactCall call = msg.payload(RpcCompactCall.class);

        doHandle(call.args(), msg.endpoint(), (err, result) -> {
            if (err == null) {
                if (result == null) {
                    // Null result (works for void method too).
                    msg.reply(RpcCallNullResult.INSTANCE);
                } else {
                    // Non-null result.
                    msg.reply(new RpcCallResult(result));
                }
            } else {
                msg.reply(new RpcCallError(err));
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
                log.error("RPC failure [from={}, method={}#{}]", from.remoteAddress(), rpc.name(), method.signature(), cause);
            }

            callback.accept(cause, null);
        } catch (Throwable t) {
            if (log.isErrorEnabled()) {
                log.error("RPC failure [from={}, method={}#{}]", from.remoteAddress(), rpc.name(), method.signature(), t);
            }

            // Return error as is.
            callback.accept(t, null);
        }
    }
}
