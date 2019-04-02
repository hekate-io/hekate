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
