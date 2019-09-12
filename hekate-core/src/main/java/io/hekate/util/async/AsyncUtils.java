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

package io.hekate.util.async;

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.HekateThreadFactory;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Set of utility methods for asynchronous tasks processing.
 */
public final class AsyncUtils {
    /** See {@link #fallbackExecutor()}. */
    private static final ThreadPoolExecutor FALLBACK_EXECUTOR;

    /** Empty array for {@link Collection#toArray(Object[])}. */
    private static final CompletableFuture[] EMPTY_FUTURES = new CompletableFuture[0];

    /** See {@link #cancelledFuture()}. */
    private static final Future<Object> CANCELLED_FUTURE = new Future<Object>() {
        @Override
        public boolean isCancelled() {
            return true;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public Object get() throws InterruptedException, ExecutionException {
            return null;
        }

        @Override
        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }
    };

    static {
        HekateThreadFactory factory = new HekateThreadFactory("AsyncFallback", null, false);

        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();

        FALLBACK_EXECUTOR = new ThreadPoolExecutor(0, 1, 1, TimeUnit.SECONDS, queue, r -> {
            Thread t = factory.newThread(r);

            t.setDaemon(true);

            return t;
        });

        FALLBACK_EXECUTOR.allowCoreThreadTimeOut(true);
    }

    private AsyncUtils() {
        // No-op.
    }

    /**
     * Fallback executor.
     *
     * <p>
     * Returns a single-threaded executor instance that can be used run tasks that couldn't be executed on their own executors for some
     * reasons. This executor should not be used for execution of a regular flow tasks.
     * </p>
     *
     * @return Fallback executor.
     */
    public static Executor fallbackExecutor() {
        return FALLBACK_EXECUTOR;
    }

    /**
     * {@link ExecutorService#shutdown() Shuts down} the specified executor service and returns a {@link Waiting} that will {@link
     * Waiting#await() await} for {@link ExecutorService#awaitTermination(long, TimeUnit) termination}.
     *
     * @param executor Executor service to shut down.
     *
     * @return Waiting for {@link ExecutorService#awaitTermination(long, TimeUnit) termination}.
     */
    public static Waiting shutdown(ExecutorService executor) {
        if (executor == null) {
            return Waiting.NO_WAIT;
        } else {
            executor.shutdown();

            return () -> {
                // TODO: Strange but looks like it helps to fix a weird issue with thread pool termination on Travis-CI.
                while (!executor.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                    executor.shutdown();
                }
            };
        }
    }

    /**
     * Uninterruptedly waits for the specified {@link Future} to complete, and then returns its result.
     *
     * @param future Future.
     * @param <T> Result type.
     *
     * @return Result of the {@link Future#get()} method.
     *
     * @throws ExecutionException If thrown by the {@link Future#get()} method.
     */
    public static <T> T getUninterruptedly(Future<T> future) throws ExecutionException {
        ArgAssert.notNull(future, "future");

        boolean interrupted = false;

        try {
            while (true) {
                try {
                    return future.get();
                } catch (InterruptedException e) {
                    interrupted = true;

                    // Make sure that interrupted flag is reset to 'false'.
                    Thread.interrupted();
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Version of {@link CompletableFuture#allOf(CompletableFuture[])} that works with a collection instead of an array.
     *
     * @param all Collection of future objects.
     *
     * @return Future that is completed when all of the given CompletableFutures complete.
     *
     * @see CompletableFuture#allOf(CompletableFuture[])
     */
    public static CompletableFuture<Void> allOf(Collection<CompletableFuture<?>> all) {
        return CompletableFuture.allOf(all.toArray(EMPTY_FUTURES));
    }

    /**
     * Returns a cancelled {@link Future}.
     *
     * @return Future.
     */
    public static Future<?> cancelledFuture() {
        return CANCELLED_FUTURE;
    }
}
