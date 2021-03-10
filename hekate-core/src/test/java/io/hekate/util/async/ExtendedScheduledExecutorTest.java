/*
 * Copyright 2021 The Hekate Project
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

import io.hekate.HekateTestBase;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExtendedScheduledExecutorTest extends HekateTestBase {
    @Test
    public void testRepeatAtFixedRate() throws Exception {
        ExtendedScheduledExecutor exec = new ExtendedScheduledExecutor(1);

        try {
            AtomicInteger count = new AtomicInteger();

            exec.repeatAtFixedRate(() -> count.incrementAndGet() < 3, 0, 1, TimeUnit.NANOSECONDS);

            busyWait("command executed", () -> count.get() == 3);

            sleep(10);

            assertEquals(3, count.get());
        } finally {
            shutDown(exec);
        }
    }

    @Test
    public void testRepeatWithFixedDelay() throws Exception {
        ExtendedScheduledExecutor exec = new ExtendedScheduledExecutor(1);

        try {
            AtomicInteger count = new AtomicInteger();

            exec.repeatWithFixedDelay(() -> count.incrementAndGet() < 3, 0, 1, TimeUnit.NANOSECONDS);

            busyWait("command executed", () -> count.get() == 3);

            sleep(10);

            assertEquals(3, count.get());
        } finally {
            shutDown(exec);
        }
    }

    @Test
    public void testConstructor1() throws Exception {
        ExtendedScheduledExecutor exec = new ExtendedScheduledExecutor(1);

        assertEquals(1, exec.getCorePoolSize());
    }

    @Test
    public void testConstructor2() throws Exception {
        ThreadFactory factory = threadFactoryMock();

        ExtendedScheduledExecutor exec = new ExtendedScheduledExecutor(1, factory);

        try {
            assertEquals(1, exec.getCorePoolSize());

            exec.submit(() -> { /* No-op. */ }).get();

            verify(factory).newThread(any(Runnable.class));
        } finally {
            shutDown(exec);
        }
    }

    @Test
    public void testConstructor3() throws Exception {
        RejectedExecutionHandler handler = mock(RejectedExecutionHandler.class);

        ExtendedScheduledExecutor exec = new ExtendedScheduledExecutor(1, handler);

        shutDown(exec);

        exec.submit(() -> { /* No-op. */ });

        verify(handler).rejectedExecution(any(), same(exec));
    }

    @Test
    public void testConstructor4() throws Exception {
        ThreadFactory factory = threadFactoryMock();
        RejectedExecutionHandler handler = mock(RejectedExecutionHandler.class);

        ExtendedScheduledExecutor exec = new ExtendedScheduledExecutor(1, factory, handler);

        try {
            assertEquals(1, exec.getCorePoolSize());

            exec.submit(() -> { /* No-op. */ }).get();

            verify(factory).newThread(any(Runnable.class));
        } finally {
            shutDown(exec);
        }

        exec.submit(() -> { /* No-op. */ });

        verify(handler).rejectedExecution(any(), same(exec));
    }

    private void shutDown(ExtendedScheduledExecutor exec) {
        exec.shutdown();

        try {
            exec.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private ThreadFactory threadFactoryMock() {
        ThreadFactory factory = mock(ThreadFactory.class);

        when(factory.newThread(any(Runnable.class))).thenAnswer(call ->
            new Thread((Runnable)call.getArguments()[0])
        );

        return factory;
    }
}
