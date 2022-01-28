/*
 * Copyright 2022 The Hekate Project
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

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Extended version of {@link ScheduledThreadPoolExecutor}.
 *
 * <p>
 * New methods that are provided by this executor are:
 * </p>
 * <ul>
 * <li>{@link #repeatAtFixedRate(RepeatingRunnable, long, long, TimeUnit)}</li>
 * <li>{@link #repeatWithFixedDelay(RepeatingRunnable, long, long, TimeUnit)}</li>
 * </ul>
 */
public class ExtendedScheduledExecutor extends ScheduledThreadPoolExecutor {
    private static final class RepeatingTask implements Runnable {
        private final RepeatingRunnable task;

        private RunnableScheduledFuture<?> future;

        private RepeatingTask(RepeatingRunnable task) {
            this.task = task;
        }

        public static Runnable of(RepeatingRunnable runnable) {
            return new RepeatingTask(runnable);
        }

        public static <V> RunnableScheduledFuture<V> decorate(Runnable runnable, RunnableScheduledFuture<V> task) {
            if (runnable instanceof RepeatingTask) {
                ((RepeatingTask)runnable).future = task;
            }

            return task;
        }

        @Override
        public void run() {
            if (!task.run()) {
                future.cancel(false);
            }
        }
    }

    /**
     * Constructs a new instance.
     *
     * @param corePoolSize Thread pool size.
     */
    public ExtendedScheduledExecutor(int corePoolSize) {
        super(corePoolSize);
    }

    /**
     * Constructs a new instance.
     *
     * @param corePoolSize Thread pool size.
     * @param threadFactory Thread factory.
     */
    public ExtendedScheduledExecutor(int corePoolSize, ThreadFactory threadFactory) {
        super(corePoolSize, threadFactory);
    }

    /**
     * Constructs a new instance.
     *
     * @param corePoolSize Thread pool size.
     * @param handler Rejected execution handler.
     */
    public ExtendedScheduledExecutor(int corePoolSize, RejectedExecutionHandler handler) {
        super(corePoolSize, handler);
    }

    /**
     * Constructs a new instance.
     *
     * @param corePoolSize Thread pool size.
     * @param threadFactory Thread factory.
     * @param handler Rejected execution handler.
     */
    public ExtendedScheduledExecutor(int corePoolSize, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
        super(corePoolSize, threadFactory, handler);
    }

    /**
     * Schedules the specified repeating command to be executed at fixed rate.
     *
     * <p>
     * Behavior of this method is similar to the behavior of the {@link #scheduleAtFixedRate(Runnable, long, long, TimeUnit)} method,
     * except that it accepts an instance of {@link RepeatingRunnable} instead of a {@link Runnable} interface.
     * </p>
     *
     * <p>
     * After each execution the result of {@link RepeatingRunnable#run()} method's will be checked. If it returns {@code true} then the
     * command will be re-scheduled again; if it returns {@code false} then execution will be cancelled and the command will not be
     * executed anymore.
     * </p>
     *
     * @param command Command.
     * @param initialDelay Initial delay before the first execution.
     * @param period Interval between executions.
     * @param unit Time unit of {@code initialDelay} and {@code period} parameters.
     *
     * @return Future of a scheduled command.
     */
    public ScheduledFuture<?> repeatAtFixedRate(RepeatingRunnable command, long initialDelay, long period, TimeUnit unit) {
        return super.scheduleAtFixedRate(RepeatingTask.of(command), initialDelay, period, unit);
    }

    /**
     * Schedules the specified repeating command to be executed with fixed delay.
     *
     * <p>
     * Behavior of this method is similar to the behavior of the {@link #scheduleWithFixedDelay(Runnable, long, long, TimeUnit)} method,
     * except that it accepts an instance of {@link RepeatingRunnable} instead of a {@link Runnable} interface.
     * </p>
     *
     * <p>
     * After each execution the result of {@link RepeatingRunnable#run()} method's will be checked. If it returns {@code true} then the
     * command will be re-scheduled again; if it returns {@code false} then execution will be cancelled and the command will not be
     * executed  anymore.
     * </p>
     *
     * @param command Command.
     * @param initialDelay Initial delay before the first execution.
     * @param delay Delay between executions.
     * @param unit Time unit of {@code initialDelay} and {@code delay} parameters.
     *
     * @return Future of a scheduled command.
     */
    public ScheduledFuture<?> repeatWithFixedDelay(RepeatingRunnable command, long initialDelay, long delay, TimeUnit unit) {
        return super.scheduleWithFixedDelay(RepeatingTask.of(command), initialDelay, delay, unit);
    }

    @Override
    protected <V> RunnableScheduledFuture<V> decorateTask(Runnable runnable, RunnableScheduledFuture<V> task) {
        return RepeatingTask.decorate(runnable, task);
    }
}
