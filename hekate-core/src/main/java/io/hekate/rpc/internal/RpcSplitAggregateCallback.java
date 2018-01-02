package io.hekate.rpc.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

class RpcSplitAggregateCallback implements BiConsumer<Throwable, Object> {
    private final int parts;

    private final List<Object> results;

    private final Function<List<Object>, Object> aggregator;

    private final BiConsumer<Throwable, Object> delegate;

    private int collectedParts;

    private Throwable firstError;

    public RpcSplitAggregateCallback(int parts, Function<List<Object>, Object> aggregator, BiConsumer<Throwable, Object> delegate) {
        this.parts = parts;
        this.aggregator = aggregator;
        this.delegate = delegate;
        this.results = new ArrayList<>(parts);
    }

    @Override
    public void accept(Throwable err, Object result) {
        // Collect/aggregate results.
        boolean allDone = false;

        Object okResult = null;
        Throwable errResult = null;

        synchronized (results) {
            if (err == null) {
                if (result != null) {
                    results.add(result);
                }
            } else {
                if (firstError == null) {
                    firstError = err;
                }
            }

            // Check if we've collected all results.
            collectedParts++;

            if (collectedParts == parts) {
                allDone = true;

                // Check if should complete successfully or exceptionally.
                if (firstError == null) {
                    okResult = aggregator.apply(results);
                } else {
                    errResult = firstError;
                }
            }
        }

        // Complete if we've collected all results.
        if (allDone) {
            delegate.accept(errResult, okResult);
        }
    }
}
