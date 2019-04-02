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

import io.hekate.HekateTestBase;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.mockito.InOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AsyncUtilsTest extends HekateTestBase {
    @Test
    public void testShutdown() throws Exception {
        assertSame(Waiting.NO_WAIT, AsyncUtils.shutdown(null));

        ExecutorService pool = mock(ExecutorService.class);

        AtomicInteger waitCalls = new AtomicInteger(3);

        when(pool.awaitTermination(anyLong(), any(TimeUnit.class))).then(call -> waitCalls.decrementAndGet() == 0);

        Waiting waiting = AsyncUtils.shutdown(pool);

        assertNotNull(waiting);

        InOrder order = inOrder(pool);

        order.verify(pool).shutdown();

        waiting.await();

        order.verify(pool).awaitTermination(500, TimeUnit.MILLISECONDS);
        order.verify(pool).shutdown();
        order.verify(pool).awaitTermination(500, TimeUnit.MILLISECONDS);
        order.verify(pool).shutdown();
        order.verify(pool).awaitTermination(500, TimeUnit.MILLISECONDS);
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
    public void testAllOf() throws Exception {
        List<CompletableFuture<?>> futures = Arrays.asList(
            new CompletableFuture<>(),
            new CompletableFuture<>(),
            new CompletableFuture<>()
        );

        CompletableFuture<Void> all = AsyncUtils.allOf(futures);

        futures.get(0).complete(null);
        assertFalse(all.isDone());

        futures.get(1).complete(null);
        assertFalse(all.isDone());

        futures.get(2).complete(null);
        assertTrue(all.isDone());
    }

    @Test
    public void testAllOfEmpty() throws Exception {
        CompletableFuture<Void> empty = AsyncUtils.allOf(Collections.emptyList());

        assertTrue(empty.isDone());
    }

    @Test
    public void testCancelledFuture() throws Exception {
        Future<?> f = AsyncUtils.cancelledFuture();

        assertTrue(f.isDone());
        assertTrue(f.isCancelled());

        assertFalse(f.cancel(true));
        assertFalse(f.cancel(false));
        assertNull(f.get());
        assertNull(f.get(1, TimeUnit.SECONDS));
    }

    @Test
    public void testUtilityClass() throws Exception {
        assertValidUtilityClass(AsyncUtils.class);
    }
}
