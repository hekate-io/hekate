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

package io.hekate.util.async;

import io.hekate.HekateTestBase;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.mockito.InOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

public class AsyncUtilsTest extends HekateTestBase {
    @Test
    public void testShutdown() throws Exception {
        assertSame(Waiting.NO_WAIT, AsyncUtils.shutdown(null));

        ExecutorService pool = mock(ExecutorService.class);

        Waiting waiting = AsyncUtils.shutdown(pool);

        assertNotNull(waiting);

        InOrder order = inOrder(pool);

        order.verify(pool).shutdown();

        waiting.await();

        order.verify(pool).awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        order.verifyNoMoreInteractions();
    }

    @Test
    public void testFallbackExecutor() throws Exception {
        assertNotNull(AsyncUtils.fallbackExecutor());

        for (int i = 0; i < 100; i++) {
            CountDownLatch executed = new CountDownLatch(1);

            AsyncUtils.fallbackExecutor().execute(executed::countDown);

            await(executed);
        }
    }

    @Test
    public void testGetUninterruptedlyNoInterrupt() throws Exception {
        CompletableFuture<String> future = new CompletableFuture<>();

        Future<Object> testFuture = runAsync(() ->
            AsyncUtils.getUninterruptedly(future)
        );

        future.complete("something");

        assertEquals("something", get(testFuture));
    }

    @Test
    public void testGetUninterruptedlyInterrupt() throws Exception {
        CompletableFuture<String> future = new CompletableFuture<>();

        CountDownLatch interrupted = new CountDownLatch(1);

        Future<Object> testFuture = runAsync(() -> {
            Thread.currentThread().interrupt();

            interrupted.countDown();

            String result = AsyncUtils.getUninterruptedly(future);

            // Check that interrupted flag was reset.
            assertTrue(Thread.currentThread().isInterrupted());

            return result;
        });

        await(interrupted);

        // Give async thread some time to retry interruption.
        sleep(50);

        future.complete("something");

        assertEquals("something", get(testFuture));
    }

    @Test
    public void testUtilityClass() throws Exception {
        assertValidUtilityClass(AsyncUtils.class);
    }
}
