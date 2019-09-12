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

package io.hekate.network;

import io.hekate.HekateTestBase;
import io.hekate.codec.JdkCodecFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class NetworkConnectorConfigTest extends HekateTestBase {
    private final NetworkConnectorConfig<Object> cfg = new NetworkConnectorConfig<>();

    @Test
    public void testProtocol() {
        assertNull(cfg.getProtocol());

        cfg.setProtocol("test");

        assertEquals("test", cfg.getProtocol());

        assertSame(cfg, cfg.withProtocol("test2"));

        assertEquals("test2", cfg.getProtocol());
    }

    @Test
    public void testMessageCodec() throws Exception {
        assertNull(cfg.getMessageCodec());

        JdkCodecFactory<Object> factory1 = new JdkCodecFactory<>();
        JdkCodecFactory<Object> factory2 = new JdkCodecFactory<>();

        cfg.setMessageCodec(factory1);

        assertSame(factory1, cfg.getMessageCodec());

        assertSame(cfg, cfg.withMessageCodec(factory2));

        assertSame(factory2, cfg.getMessageCodec());
    }

    @Test
    public void testIdleSocketTimeout() throws Exception {
        assertEquals(0, cfg.getIdleSocketTimeout());

        cfg.setIdleSocketTimeout(1000);

        assertEquals(1000, cfg.getIdleSocketTimeout());

        assertSame(cfg, cfg.withIdleSocketTimeout(2000));

        assertEquals(2000, cfg.getIdleSocketTimeout());
    }

    @Test
    public void testNioThreads() throws Exception {
        assertEquals(0, cfg.getNioThreads());

        cfg.setNioThreads(10);

        assertEquals(10, cfg.getNioThreads());

        assertSame(cfg, cfg.withNioThreads(20));

        assertEquals(20, cfg.getNioThreads());
    }

    @Test
    public void testLogCategory() {
        assertNull(cfg.getLogCategory());

        cfg.setLogCategory("test");

        assertEquals("test", cfg.getLogCategory());

        assertSame(cfg, cfg.withLogCategory("test2"));

        assertEquals("test2", cfg.getLogCategory());
    }

    @Test
    public void testHandler() throws Exception {
        assertNull(cfg.getServerHandler());

        NetworkServerHandler<Object> handler1 = (message, from) -> {
            // No-op.
        };
        NetworkServerHandler<Object> handler2 = (message, from) -> {
            // No-op.
        };

        cfg.setServerHandler(handler1);

        assertSame(handler1, cfg.getServerHandler());

        assertSame(cfg, cfg.withServerHandler(handler2));

        assertSame(handler2, cfg.getServerHandler());
    }
}
