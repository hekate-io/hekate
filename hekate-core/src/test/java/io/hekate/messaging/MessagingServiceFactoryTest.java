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

package io.hekate.messaging;

import io.hekate.HekateTestBase;
import io.hekate.messaging.intercept.MessageInterceptor;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class MessagingServiceFactoryTest extends HekateTestBase {
    private final MessagingServiceFactory cfg = new MessagingServiceFactory();

    @Test
    public void testChannels() {
        assertNull(cfg.getChannels());

        MessagingChannelConfig<Object> c1 = MessagingChannelConfig.unchecked();
        MessagingChannelConfig<Object> c2 = MessagingChannelConfig.unchecked();

        cfg.setChannels(Collections.singletonList(c1));

        assertNotNull(cfg.getChannels());
        assertTrue(cfg.getChannels().contains(c1));

        cfg.setChannels(null);

        assertNull(cfg.getChannels());

        assertTrue(cfg.withChannel(c1).getChannels().contains(c1));

        cfg.withChannel(c2);

        assertTrue(cfg.getChannels().contains(c1));
        assertTrue(cfg.getChannels().contains(c2));
    }

    @Test
    public void testProviders() {
        MessagingConfigProvider p1 = Collections::emptyList;
        MessagingConfigProvider p2 = Collections::emptyList;

        assertNull(cfg.getConfigProviders());

        cfg.setConfigProviders(Arrays.asList(p1, p2));

        assertEquals(2, cfg.getConfigProviders().size());
        assertTrue(cfg.getConfigProviders().contains(p1));
        assertTrue(cfg.getConfigProviders().contains(p2));

        cfg.setConfigProviders(null);

        assertNull(cfg.getConfigProviders());

        assertSame(cfg, cfg.withConfigProvider(p1));

        assertEquals(1, cfg.getConfigProviders().size());
        assertTrue(cfg.getConfigProviders().contains(p1));
    }

    @Test
    public void testGlobalInterceptors() {
        MessageInterceptor p1 = mock(MessageInterceptor.class);
        MessageInterceptor p2 = mock(MessageInterceptor.class);

        assertNull(cfg.getGlobalInterceptors());

        cfg.setGlobalInterceptors(Arrays.asList(p1, p2));

        assertEquals(2, cfg.getGlobalInterceptors().size());
        assertTrue(cfg.getGlobalInterceptors().contains(p1));
        assertTrue(cfg.getGlobalInterceptors().contains(p2));

        cfg.setGlobalInterceptors(null);

        assertNull(cfg.getGlobalInterceptors());

        assertSame(cfg, cfg.withGlobalInterceptor(p1));

        assertEquals(1, cfg.getGlobalInterceptors().size());
        assertTrue(cfg.getGlobalInterceptors().contains(p1));
    }

    @Test
    public void testToString() {
        assertTrue(cfg.toString(), cfg.toString().startsWith(MessagingServiceFactory.class.getSimpleName()));
    }
}
