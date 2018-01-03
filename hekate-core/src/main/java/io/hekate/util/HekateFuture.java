/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.util;

import io.hekate.util.async.AsyncUtils;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Base class for asynchronous operation results.
 *
 * @param <T> Result type.
 * @param <F> Subclass type.
 */
public abstract class HekateFuture<T, F extends HekateFuture<T, F>> extends CompletableFuture<T> {
    /**
     * Constructs new future instance of this class.
     *
     * <p>
     * This method is used by {@link #fork()} to construct a new future objects.
     * </p>
     *
     * @return New instance.
     */
    protected abstract F newInstance();

    /**
     * Returns {@code true} if this future {@link #isDone() completed} successfully without an {@link #isCompletedExceptionally() error}.
     *
     * @return {@code true} if this future completed successfully.
     */
    public boolean isSuccess() {
        return isDone() && !isCompletedExceptionally();
    }

    /**
     * Uninterruptedly waits for this future to complete, and then returns its result.
     *
     * <p>
     * <b>Note:</b> if thread gets interrupted while awaiting for operation to complete then such interruption will be ignored and thread's
     * {@link Thread#isInterrupted() interrupted} flag will be set to {@code true} after operation completion.
     * </p>
     *
     * @return Result value.
     *
     * @throws CancellationException If this future was cancelled.
     * @throws ExecutionException If this future completed exceptionally.
     */
    public T getUninterruptedly() throws ExecutionException {
        return AsyncUtils.getUninterruptedly(this);
    }

    /**
     * Returns a new future instance who's lifecycle depends on this instance but not vice versa.
     *
     * <p>
     * If this instance gets completed (either normally or with an error) then forked instance will be completed too. Completing forked
     * instance has no impact on this instance.
     * </p>
     *
     * @return Fork of this instance.
     */
    public F fork() {
        F dep = newInstance();

        if (isSuccess()) {
            dep.complete(getNow(null));
        } else if (isCancelled()) {
            dep.cancel(false);
        } else {
            whenComplete((result, error) -> {
                if (isCancelled()) {
                    dep.cancel(false);
                } else if (error != null) {
                    if (error instanceof CompletionException && error.getCause() != null) {
                        dep.completeExceptionally(error.getCause());
                    } else {
                        dep.completeExceptionally(error);
                    }
                } else {
                    dep.complete(result);
                }
            });
        }

        return dep;
    }
}
