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
import io.hekate.network.NetworkEndpoint;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ReceiveBackPressureTest extends HekateTestBase {
    @Test
    public void testHiLo() throws Exception {
        ReceiveBackPressure backPressure = new ReceiveBackPressure(5, 10);

        repeat(5, (int i) -> {
            NetworkEndpoint<?> endpoint1 = mock(NetworkEndpoint.class);
            NetworkEndpoint<?> endpoint2 = mock(NetworkEndpoint.class);

            // Enqueue up to the high watermark minus one.
            for (int j = 0; j < 9; j++) {
                backPressure.onEnqueue(endpoint1);

                assertEquals(j + 1, backPressure.getQueueSize());
                assertEquals(0, backPressure.getPausedSize());

                verifyNoMoreInteractions(endpoint1);
            }

            // Check triggering of pause (since queue reaches its high watermark).
            backPressure.onEnqueue(endpoint1);

            verify(endpoint1).pauseReceiving(null);

            assertEquals(10, backPressure.getQueueSize());
            assertEquals(1, backPressure.getPausedSize());

            // Enqueue one more to make sure that pausing get immediately applied if queue size is >= high watermark.
            backPressure.onEnqueue(endpoint2);

            verify(endpoint2).pauseReceiving(null);

            assertEquals(11, backPressure.getQueueSize());
            assertEquals(2, backPressure.getPausedSize());

            // Dequeue down to low watermark minus one.
            for (int j = 0; j < 5; j++) {
                backPressure.onDequeue();

                assertEquals(11 - j - 1, backPressure.getQueueSize());

                verifyNoMoreInteractions(endpoint1);
                verifyNoMoreInteractions(endpoint2);
            }

            assertEquals(2, backPressure.getPausedSize());

            backPressure.onDequeue();

            verify(endpoint1).resumeReceiving(null);
            verify(endpoint2).resumeReceiving(null);

            assertEquals(5, backPressure.getQueueSize());
            assertEquals(0, backPressure.getPausedSize());

            while (backPressure.getQueueSize() > 0) {
                backPressure.onDequeue();
            }

            verifyNoMoreInteractions(endpoint1);
            verifyNoMoreInteractions(endpoint2);
        });
    }

    @Test
    public void testZeroOne() throws Exception {
        repeat(5, i -> {
            NetworkEndpoint<?> endpoint = mock(NetworkEndpoint.class);

            ReceiveBackPressure backPressure = new ReceiveBackPressure(0, 1);

            backPressure.onEnqueue(endpoint);

            verify(endpoint).pauseReceiving(null);
            verifyNoMoreInteractions(endpoint);
            assertEquals(1, backPressure.getQueueSize());

            backPressure.onDequeue();

            verify(endpoint).resumeReceiving(null);
            verifyNoMoreInteractions(endpoint);
            assertEquals(0, backPressure.getQueueSize());
        });
    }

    @Test
    public void testToString() {
        ReceiveBackPressure backPressure = new ReceiveBackPressure(0, 1);

        assertTrue(backPressure.toString(), backPressure.toString().startsWith(ReceiveBackPressure.class.getSimpleName()));
    }
}
