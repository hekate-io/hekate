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

package io.hekate.messaging.internal;

import io.hekate.HekateTestBase;
import io.hekate.core.internal.util.ErrorUtils;
import io.hekate.messaging.MessageQueueOverflowException;
import io.hekate.messaging.MessageQueueTimeoutException;
import io.hekate.messaging.MessagingOverflowPolicy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.Test;

import static java.lang.Thread.currentThread;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SendPressureGuardTest extends HekateTestBase {
    @Test
    public void testBlockOneByOne() throws Exception {
        SendPressureGuard backPressure = new SendPressureGuard(0, 1, MessagingOverflowPolicy.BLOCK);

        // Lock queue.
        backPressure.onEnqueue();

        Future<?> future1 = runAsync(() -> {
            backPressure.onEnqueue();
            backPressure.onDequeue();

            return null;
        });

        Future<?> future2 = runAsync(() -> {
            backPressure.onEnqueue();
            backPressure.onDequeue();

            return null;
        });

        // Await for threads to be blocked.
        busyWait("treads blocked", () -> backPressure.queueSize() == 3);

        assertFalse(future1.isDone());
        assertFalse(future2.isDone());

        // Unlock queue.
        backPressure.onDequeue();

        // Eventually both should be unblocked.
        get(future1);
        get(future2);
    }

    @Test
    public void testBlock() throws Exception {
        SendPressureGuard backPressure = new SendPressureGuard(5, 10, MessagingOverflowPolicy.BLOCK);

        repeat(3, i -> {
            for (int j = 0; j < 10; j++) {
                backPressure.onEnqueue();

                assertEquals(j + 1, backPressure.queueSize());
            }

            assertEquals(10, backPressure.queueSize());

            Future<?> future = runAsync(() -> {
                backPressure.onEnqueue();

                return null;
            });

            // Await for thread to be blocked.
            busyWait("tread blocked", () -> backPressure.queueSize() == 11);

            assertFalse(future.isDone());

            for (int j = 0; j < 6; j++) {
                backPressure.onDequeue();
            }

            get(future);

            assertEquals(5, backPressure.queueSize());

            while (backPressure.queueSize() > 0) {
                backPressure.onDequeue();
            }
        });
    }

    @Test
    public void testBlockWithTimeout() throws Exception {
        doTestBlockWithTimeout(MessagingOverflowPolicy.BLOCK);
    }

    @Test
    public void testTerminateWhileBlocked() throws Exception {
        doTestTerminateWhileBlocked(MessagingOverflowPolicy.BLOCK);
    }

    @Test
    public void testTerminateWhileBlockedUninterruptedly() throws Exception {
        doTestTerminateWhileBlocked(MessagingOverflowPolicy.BLOCK_UNINTERRUPTEDLY);
    }

    @Test
    public void testBlockInterrupted() throws Exception {
        SendPressureGuard backPressure = new SendPressureGuard(5, 10, MessagingOverflowPolicy.BLOCK);

        repeat(3, i -> {
            for (int j = 0; j < 10; j++) {
                backPressure.onEnqueue();

                assertEquals(j + 1, backPressure.queueSize());
            }

            assertEquals(10, backPressure.queueSize());

            for (int j = 0; j < 10; j++) {
                try {
                    get(runAsync(() -> {
                        currentThread().interrupt();

                        backPressure.onEnqueue();

                        return null;
                    }));

                    fail("Error was expected.");
                } catch (ExecutionException err) {
                    assertTrue(err.toString(), err.getCause() instanceof InterruptedException);
                }

                assertEquals(10, backPressure.queueSize());
            }

            while (backPressure.queueSize() > 0) {
                backPressure.onDequeue();
            }
        });
    }

    @Test
    public void testBlockUninterruptedlyOneByOne() throws Exception {
        SendPressureGuard backPressure = new SendPressureGuard(0, 1, MessagingOverflowPolicy.BLOCK_UNINTERRUPTEDLY);

        // Lock queue.
        backPressure.onEnqueue();

        Future<?> future1 = runAsync(() -> {
            backPressure.onEnqueue();
            backPressure.onDequeue();

            return null;
        });

        Future<?> future2 = runAsync(() -> {
            backPressure.onEnqueue();
            backPressure.onDequeue();

            return null;
        });

        // Await for threads to be blocked.
        busyWait("treads blocked", () -> backPressure.queueSize() == 3);

        assertFalse(future1.isDone());
        assertFalse(future2.isDone());

        // Unlock queue.
        backPressure.onDequeue();

        // Eventually both should be unblocked.
        get(future1);
        get(future2);
    }

    @Test
    public void testBlockUninterruptedly() throws Exception {
        SendPressureGuard backPressure = new SendPressureGuard(5, 10, MessagingOverflowPolicy.BLOCK_UNINTERRUPTEDLY);

        repeat(3, i -> {
            for (int j = 0; j < 10; j++) {
                backPressure.onEnqueue();

                assertEquals(j + 1, backPressure.queueSize());
            }

            assertEquals(10, backPressure.queueSize());

            Future<?> future = runAsync(() -> {
                Thread.currentThread().interrupt();

                backPressure.onEnqueue();

                return null;
            });

            // Await for thread to be blocked.
            busyWait("tread blocked", () -> backPressure.queueSize() == 11);

            assertFalse(future.isDone());

            for (int j = 0; j < 6; j++) {
                backPressure.onDequeue();
            }

            get(future);

            assertEquals(5, backPressure.queueSize());

            while (backPressure.queueSize() > 0) {
                backPressure.onDequeue();
            }
        });
    }

    @Test
    public void testBlockUninterruptedlyWithTimeout() throws Exception {
        doTestBlockWithTimeout(MessagingOverflowPolicy.BLOCK_UNINTERRUPTEDLY);
    }

    @Test
    public void testFail() throws Exception {
        SendPressureGuard backPressure = new SendPressureGuard(5, 10, MessagingOverflowPolicy.FAIL);

        repeat(3, i -> {
            for (int j = 0; j < 10; j++) {
                backPressure.onEnqueue();

                assertEquals(j + 1, backPressure.queueSize());
            }

            assertEquals(10, backPressure.queueSize());

            for (int j = 0; j < 6; j++) {
                try {
                    backPressure.onEnqueue();

                    fail("Error was expected.");
                } catch (MessageQueueOverflowException e) {
                    assertEquals("Send queue overflow [queue-size=11, low-watermark=5, high-watermark=10]", e.getMessage());
                }

                assertEquals(10, backPressure.queueSize());
            }

            while (backPressure.queueSize() > 0) {
                backPressure.onDequeue();
            }
        });
    }

    @Test
    public void testToString() {
        SendPressureGuard backPressure = new SendPressureGuard(0, 1, MessagingOverflowPolicy.FAIL);

        assertTrue(backPressure.toString(), backPressure.toString().startsWith(SendPressureGuard.class.getSimpleName()));
    }

    private void doTestBlockWithTimeout(MessagingOverflowPolicy policy) throws Exception {
        SendPressureGuard backPressure = new SendPressureGuard(5, 10, policy);

        repeat(3, i -> {
            for (int j = 0; j < 10; j++) {
                backPressure.onEnqueue();

                assertEquals(j + 1, backPressure.queueSize());
            }

            assertEquals(10, backPressure.queueSize());

            try {
                get(runAsync(() ->
                    backPressure.onEnqueue(10, "test")
                ));

                fail("Error was expected");
            } catch (ExecutionException e) {
                assertTrue(getStacktrace(e), ErrorUtils.isCausedBy(MessageQueueTimeoutException.class, e));
            }

            assertEquals(10, backPressure.queueSize());

            CountDownLatch enqueueLatch = new CountDownLatch(1);

            Future<Long> future = runAsync(() -> {
                enqueueLatch.countDown();

                long remainingTime = backPressure.onEnqueue(300, "test");

                say("Enqueued [remaining=" + remainingTime + ']');

                return remainingTime;
            });

            await(enqueueLatch);

            sleep(50);

            assertFalse(future.isDone());

            while (backPressure.queueSize() > 1) {
                backPressure.onDequeue();
            }

            long remainingTime = get(future);

            assertTrue("remaining time:" + remainingTime, remainingTime > 0);
            assertTrue("remaining time:" + remainingTime, remainingTime < 300);
            assertEquals(1, backPressure.queueSize());

            backPressure.onDequeue();
        });
    }

    private void doTestTerminateWhileBlocked(MessagingOverflowPolicy policy) throws Exception {
        SendPressureGuard backPressure = new SendPressureGuard(0, 1, policy);

        backPressure.onEnqueue();

        Future<?> async = runAsync(() -> {
            backPressure.onEnqueue();

            return null;
        });

        busyWait("blocked", () -> backPressure.queueSize() == 2);

        assertFalse(async.isDone());

        backPressure.terminate();

        assertNull(get(async));
    }
}
