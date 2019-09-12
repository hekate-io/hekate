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
import io.hekate.network.NetworkEndpoint;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ReceivePressureGuardTest extends HekateTestBase {
    @Test
    public void testHiLo() throws Exception {
        ReceivePressureGuard backPressure = new ReceivePressureGuard(5, 10);

        repeat(5, (int i) -> {
            NetworkEndpoint<?> endpoint1 = mock(NetworkEndpoint.class);
            NetworkEndpoint<?> endpoint2 = mock(NetworkEndpoint.class);

            // Enqueue up to the high watermark minus one.
            for (int j = 0; j < 9; j++) {
                backPressure.onEnqueue(endpoint1);

                assertEquals(j + 1, backPressure.queueSize());
                assertEquals(0, backPressure.pausedSize());

                verifyNoMoreInteractions(endpoint1);
            }

            // Check triggering of pause (since queue reaches its high watermark).
            backPressure.onEnqueue(endpoint1);

            verify(endpoint1).pauseReceiving(null);

            assertEquals(10, backPressure.queueSize());
            assertEquals(1, backPressure.pausedSize());

            // Enqueue one more to make sure that pausing get immediately applied if queue size is >= high watermark.
            backPressure.onEnqueue(endpoint2);

            verify(endpoint2).pauseReceiving(null);

            assertEquals(11, backPressure.queueSize());
            assertEquals(2, backPressure.pausedSize());

            // Dequeue down to low watermark minus one.
            for (int j = 0; j < 5; j++) {
                backPressure.onDequeue();

                assertEquals(11 - j - 1, backPressure.queueSize());

                verifyNoMoreInteractions(endpoint1);
                verifyNoMoreInteractions(endpoint2);
            }

            assertEquals(2, backPressure.pausedSize());

            backPressure.onDequeue();

            verify(endpoint1).resumeReceiving(null);
            verify(endpoint2).resumeReceiving(null);

            assertEquals(5, backPressure.queueSize());
            assertEquals(0, backPressure.pausedSize());

            while (backPressure.queueSize() > 0) {
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

            ReceivePressureGuard backPressure = new ReceivePressureGuard(0, 1);

            backPressure.onEnqueue(endpoint);

            verify(endpoint).pauseReceiving(null);
            verifyNoMoreInteractions(endpoint);
            assertEquals(1, backPressure.queueSize());

            backPressure.onDequeue();

            verify(endpoint).resumeReceiving(null);
            verifyNoMoreInteractions(endpoint);
            assertEquals(0, backPressure.queueSize());
        });
    }

    @Test
    public void testToString() {
        ReceivePressureGuard backPressure = new ReceivePressureGuard(0, 1);

        assertTrue(backPressure.toString(), backPressure.toString().startsWith(ReceivePressureGuard.class.getSimpleName()));
    }
}
