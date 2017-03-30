/*
 * Copyright 2017 The Hekate Project
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
import io.hekate.messaging.MessageQueueOverflowException;
import io.hekate.messaging.MessagingOverflowPolicy;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SendBackPressureTest extends HekateTestBase {
    @Test
    public void testBlock() throws Exception {
        SendBackPressure backPressure = new SendBackPressure(5, 10, MessagingOverflowPolicy.BLOCK);

        repeat(3, i -> {
            for (int j = 0; j < 10; j++) {
                backPressure.onEnqueue();

                assertEquals(j + 1, backPressure.getQueueSize());
            }

            assertEquals(10, backPressure.getQueueSize());

            Future<?> future = runAsync(() -> {
                backPressure.onEnqueue();

                return null;
            });

            expect(TimeoutException.class, () -> future.get(300, TimeUnit.MILLISECONDS));

            assertEquals(11, backPressure.getQueueSize());

            for (int j = 0; j < 6; j++) {
                backPressure.onDequeue();
            }

            assertNull(future.get(3, TimeUnit.SECONDS));

            assertEquals(5, backPressure.getQueueSize());

            while (backPressure.getQueueSize() > 0) {
                backPressure.onDequeue();
            }
        });
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
        SendBackPressure backPressure = new SendBackPressure(5, 10, MessagingOverflowPolicy.BLOCK);

        repeat(3, i -> {
            for (int j = 0; j < 10; j++) {
                backPressure.onEnqueue();

                assertEquals(j + 1, backPressure.getQueueSize());
            }

            assertEquals(10, backPressure.getQueueSize());

            for (int j = 0; j < 10; j++) {
                try {
                    runAsync(() -> {
                        Thread.currentThread().interrupt();

                        backPressure.onEnqueue();

                        return null;
                    }).get(3, TimeUnit.SECONDS);

                    fail("Error was expected.");
                } catch (ExecutionException err) {
                    assertTrue(err.toString(), err.getCause() instanceof InterruptedException);
                }

                backPressure.onDequeue();

                assertEquals(10, backPressure.getQueueSize());
            }

            while (backPressure.getQueueSize() > 0) {
                backPressure.onDequeue();
            }
        });
    }

    @Test
    public void testBlockUninterruptedly() throws Exception {
        SendBackPressure backPressure = new SendBackPressure(5, 10, MessagingOverflowPolicy.BLOCK_UNINTERRUPTEDLY);

        repeat(3, i -> {
            for (int j = 0; j < 10; j++) {
                backPressure.onEnqueue();

                assertEquals(j + 1, backPressure.getQueueSize());
            }

            assertEquals(10, backPressure.getQueueSize());

            Future<?> future = runAsync(() -> {
                Thread.currentThread().interrupt();

                backPressure.onEnqueue();

                return null;
            });

            expect(TimeoutException.class, () -> future.get(300, TimeUnit.MILLISECONDS));

            assertEquals(11, backPressure.getQueueSize());

            for (int j = 0; j < 6; j++) {
                backPressure.onDequeue();
            }

            assertNull(future.get(3, TimeUnit.SECONDS));

            assertEquals(5, backPressure.getQueueSize());

            while (backPressure.getQueueSize() > 0) {
                backPressure.onDequeue();
            }
        });
    }

    @Test
    public void testFail() throws Exception {
        SendBackPressure backPressure = new SendBackPressure(5, 10, MessagingOverflowPolicy.FAIL);

        repeat(3, i -> {
            for (int j = 0; j < 10; j++) {
                backPressure.onEnqueue();

                assertEquals(j + 1, backPressure.getQueueSize());
            }

            assertEquals(10, backPressure.getQueueSize());

            for (int j = 0; j < 6; j++) {
                try {
                    backPressure.onEnqueue();

                    fail("Error was expected.");
                } catch (MessageQueueOverflowException e) {
                    assertEquals("Send queue overflow [queue-size=11, low-watermark=5, high-watermark=10]", e.getMessage());
                }

                backPressure.onDequeue();

                assertEquals(10, backPressure.getQueueSize());
            }

            while (backPressure.getQueueSize() > 0) {
                backPressure.onDequeue();
            }
        });
    }

    @Test
    public void testToString() {
        SendBackPressure backPressure = new SendBackPressure(0, 1, MessagingOverflowPolicy.FAIL);

        assertTrue(backPressure.toString(), backPressure.toString().startsWith(SendBackPressure.class.getSimpleName()));
    }

    private void doTestTerminateWhileBlocked(MessagingOverflowPolicy policy) throws Exception {
        SendBackPressure backPressure = new SendBackPressure(0, 1, policy);

        backPressure.onEnqueue();

        Future<?> async = runAsync(() -> {
            backPressure.onEnqueue();

            return null;
        });

        busyWait("blocked", () -> backPressure.getQueueSize() == 2);

        expect(TimeoutException.class, () -> async.get(100, TimeUnit.MILLISECONDS));

        backPressure.terminate();

        assertNull(async.get(3, TimeUnit.SECONDS));
    }
}
