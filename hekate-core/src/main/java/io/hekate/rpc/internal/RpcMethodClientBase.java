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

import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.rpc.RpcException;
import io.hekate.rpc.RpcInterfaceInfo;
import io.hekate.rpc.RpcMethodInfo;
import java.lang.reflect.Method;
import java.util.concurrent.TimeoutException;

abstract class RpcMethodClientBase<T> {
    private final RpcInterfaceInfo<T> rpc;

    private final String tag;

    private final RpcMethodInfo method;

    private final String methodIdxKey;

    private final MessagingChannel<RpcProtocol> channel;

    public RpcMethodClientBase(RpcInterfaceInfo<T> rpc, String tag, RpcMethodInfo method, MessagingChannel<RpcProtocol> channel) {
        this.rpc = rpc;
        this.tag = tag;
        this.method = method;
        this.channel = channel;

        if (tag == null) {
            methodIdxKey = RpcUtils.methodProperty(rpc, method);
        } else {
            methodIdxKey = RpcUtils.taggedMethodProperty(rpc, method, tag);
        }
    }

    protected abstract Object doInvoke(Object affinity, Object[] args) throws MessagingFutureException, InterruptedException,
        TimeoutException;

    public Object invoke(Object[] args) throws Exception {
        Object affinity;

        if (method.affinityArg().isPresent()) {
            affinity = args[method.affinityArg().getAsInt()];
        } else {
            affinity = null;
        }

        try {
            try {
                return doInvoke(affinity, args);
            } catch (MessagingFutureException e) {
                // Unwrap asynchronous messaging error.
                throw e.getCause();
            }
        } catch (Throwable e) {
            // Try to throw as is.
            tryThrow(method.javaMethod(), e);

            // Re-throw as unchecked exception.
            throw new RpcException("RPC failed [method=" + rpc.name() + '#' + method.signature() + ']', e);
        }
    }

    public RpcInterfaceInfo<T> rpc() {
        return rpc;
    }

    public String tag() {
        return tag;
    }

    public RpcMethodInfo method() {
        return method;
    }

    public MessagingChannel<RpcProtocol> channel() {
        return channel;
    }

    public String methodIdxKey() {
        return methodIdxKey;
    }

    private static void tryThrow(Method meth, Throwable error) throws Exception {
        if (error instanceof RuntimeException) {
            throw (RuntimeException)error;
        } else if (error instanceof Error) {
            throw (Error)error;
        } else {
            for (Class<?> declared : meth.getExceptionTypes()) {
                if (declared.isAssignableFrom(error.getClass())) {
                    throw (Exception)error;
                }
            }
        }
    }
}
