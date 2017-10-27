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

    private final MessagingChannel<RpcProtocol> channel;

    public RpcMethodClientBase(RpcInterfaceInfo<T> rpc, String tag, RpcMethodInfo method, MessagingChannel<RpcProtocol> channel) {
        this.rpc = rpc;
        this.tag = tag;
        this.method = method;
        this.channel = channel;
    }

    protected abstract Object doInvoke(MessagingChannel<RpcProtocol> channel, Object[] args)
        throws MessagingFutureException, InterruptedException, TimeoutException;

    public Object invoke(Object[] args) throws Exception {
        MessagingChannel<RpcProtocol> callChannel;

        if (method.affinityArg().isPresent()) {
            callChannel = this.channel.withAffinity(args[method.affinityArg().getAsInt()]);
        } else {
            callChannel = this.channel;
        }

        try {
            try {
                return doInvoke(callChannel, args);
            } catch (MessagingFutureException e) {
                // Unwrap asynchronous messaging error.
                throw e.getCause();
            }
        } catch (Throwable e) {
            // Try to throw as is.
            tryThrow(method.javaMethod(), e);

            // Re-throw as unchecked exception.
            throw new RpcException("RPC failed [type=" + rpc + ", method=" + method + ']', e);
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
