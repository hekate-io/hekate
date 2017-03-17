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

import io.hekate.HekateInstanceTestBase;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingService;
import io.hekate.messaging.MessagingServiceFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MessagingServiceSingleNodeTest extends HekateInstanceTestBase {
    @Test
    public void testEmptyChannels() throws Exception {
        MessagingService messaging = createInstance(boot ->
            boot.withService(new MessagingServiceFactory())
        ).join().get(MessagingService.class);

        assertTrue(messaging.allChannels().isEmpty());

        assertFalse(messaging.hasChannel("no-such-channel"));

        expect(IllegalArgumentException.class, () -> messaging.channel("no-such-channel"));

        assertTrue(messaging.toString(), messaging.toString().startsWith(MessagingService.class.getSimpleName()));
    }

    @Test
    public void testMultipleChannels() throws Exception {
        MessagingService messaging = createInstance(boot ->
            boot.withService(new MessagingServiceFactory()
                .withChannel(new MessagingChannelConfig<>("channel1"))
                .withChannel(new MessagingChannelConfig<>("channel2"))
            )
        ).join().get(MessagingService.class);

        assertTrue(messaging.hasChannel("channel1"));
        assertTrue(messaging.hasChannel("channel2"));

        MessagingChannel<Object> channel1 = messaging.channel("channel1");
        MessagingChannel<Object> channel2 = messaging.channel("channel2");

        assertNotNull(channel1);
        assertNotNull(channel2);

        assertEquals(2, messaging.allChannels().size());
        assertTrue(messaging.allChannels().contains(channel1));
        assertTrue(messaging.allChannels().contains(channel2));

        assertTrue(messaging.toString(), messaging.toString().startsWith(MessagingService.class.getSimpleName()));
        assertTrue(messaging.toString(), messaging.toString().contains("channel1"));
        assertTrue(messaging.toString(), messaging.toString().contains("channel2"));
    }
}
