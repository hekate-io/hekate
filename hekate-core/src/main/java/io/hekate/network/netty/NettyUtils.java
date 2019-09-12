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

package io.hekate.network.netty;

import io.hekate.util.async.AsyncUtils;
import io.hekate.util.async.Waiting;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.EventExecutorGroup;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Netty-related utilities.
 */
public final class NettyUtils {
    private NettyUtils() {
        // No-op.
    }

    /**
     * {@link EventExecutorGroup#shutdownGracefully(long, long, TimeUnit) Shuts down} the specified executor with {@code 0} graceful
     * shutdown period.
     *
     * @param executor Executor to shutdown (can be {@code null}).
     *
     * @return Waiting.
     */
    public static Waiting shutdown(EventExecutorGroup executor) {
        if (executor == null) {
            return Waiting.NO_WAIT;
        } else {
            return executor.shutdownGracefully(0, Long.MAX_VALUE, TimeUnit.MILLISECONDS)::await;
        }
    }

    /**
     * Executes the task using the provided event loop or falls back to {@link AsyncUtils#fallbackExecutor()} if event loop is already
     * {@link EventLoop#isShuttingDown() shut down}.
     *
     * @param eventLoop Event loop.
     * @param task Task.
     */
    public static void runAtAllCost(EventLoop eventLoop, Runnable task) {
        assert eventLoop != null : "Event loop is null.";
        assert task != null : "Task is null.";

        boolean notified = false;

        // Try to execute via event loop.
        if (!eventLoop.isShuttingDown()) {
            try {
                eventLoop.execute(task);

                notified = true;
            } catch (RejectedExecutionException e) {
                // No-op.
            }
        }

        // If couldn't notify via event loop then use the fallback executor.
        if (!notified) {
            AsyncUtils.fallbackExecutor().execute(task);
        }
    }
}
