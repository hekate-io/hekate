package io.hekate.rpc.internal;

import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.unicast.Response;
import io.hekate.messaging.unicast.ResponseFuture;
import io.hekate.rpc.RpcInterfaceInfo;
import io.hekate.rpc.RpcMethodInfo;
import io.hekate.rpc.internal.RpcProtocol.CallRequest;
import io.hekate.rpc.internal.RpcProtocol.ObjectResponse;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

class RpcUnicastMethodClient<T> extends RpcMethodClientBase<T> {
    private static final Function<Response<RpcProtocol>, Object> RESPONSE_CONVERTER = response -> {
        RpcProtocol result = response.get();

        if (result instanceof ObjectResponse) {
            return ((ObjectResponse)result).object();
        } else {
            return null;
        }
    };

    public RpcUnicastMethodClient(RpcInterfaceInfo<T> rpc, String tag, RpcMethodInfo method, MessagingChannel<RpcProtocol> channel) {
        super(rpc, tag, method, channel);
    }

    @Override
    protected Object doInvoke(MessagingChannel<RpcProtocol> callChannel, Object[] args) throws MessagingFutureException,
        InterruptedException, TimeoutException {
        CallRequest<T> request = new CallRequest<>(rpc(), tag(), method(), args);

        ResponseFuture<RpcProtocol> future = callChannel.request(request);

        if (method().isAsync()) {
            return future.thenApply(RESPONSE_CONVERTER);
        } else {
            Response<RpcProtocol> response;

            if (callChannel.timeout() > 0) {
                response = future.get(callChannel.timeout(), TimeUnit.MILLISECONDS);
            } else {
                response = future.get();
            }

            return RESPONSE_CONVERTER.apply(response);
        }
    }
}
