package io.hekate.rpc.internal;

import io.hekate.messaging.MessagingFuture;
import io.hekate.messaging.unicast.Response;
import io.hekate.messaging.unicast.ResponseCallback;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

class RpcSplitAggregateFuture extends MessagingFuture<Object> implements ResponseCallback<RpcProtocol> {
    private final int parts;

    private final List<RpcProtocol> responses;

    private final RpcErrorMappingPolicy errorPolicy;

    private final Function<List<RpcProtocol>, Object> aggregator;

    private int collectedParts;

    private Throwable firstError;

    public RpcSplitAggregateFuture(int parts, RpcErrorMappingPolicy errorPolicy, Function<List<RpcProtocol>, Object> aggregator) {
        this.parts = parts;
        this.errorPolicy = errorPolicy;
        this.aggregator = aggregator;
        this.responses = new ArrayList<>(parts);
    }

    @Override
    public void onComplete(Throwable err, Response<RpcProtocol> rsp) {
        if (!isDone()) {
            Throwable mappedErr = null;

            // Check for errors.
            if (err != null && errorPolicy != null) {
                mappedErr = errorPolicy.apply(err);
            }

            // Collect/aggregate results.
            boolean allDone = false;

            Object okResult = null;
            Throwable errResult = null;

            synchronized (responses) {
                if (mappedErr == null) {
                    if (rsp != null) { // <-- Can be null if there was a real error but it was ignored by the error policy.
                        responses.add(rsp.get());
                    }
                } else {
                    if (firstError == null) {
                        firstError = mappedErr;
                    }
                }

                // Check if we've collected all results.
                collectedParts++;

                if (collectedParts == parts) {
                    allDone = true;

                    // Check if should complete successfully or exceptionally.
                    if (firstError == null) {
                        okResult = aggregator.apply(responses);
                    } else {
                        errResult = firstError;
                    }
                }
            }

            // Complete if we've collected all results.
            if (allDone) {
                if (errResult == null) {
                    complete(okResult);
                } else {
                    completeExceptionally(errResult);
                }
            }
        }
    }
}
