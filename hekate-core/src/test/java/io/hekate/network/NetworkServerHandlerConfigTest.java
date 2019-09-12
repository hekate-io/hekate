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

public class NetworkServerHandlerConfigTest extends HekateTestBase {
    private final NetworkServerHandlerConfig<Object> cfg = new NetworkServerHandlerConfig<>();

    @Test
    public void testProtocol() {
        assertNull(cfg.getProtocol());

        cfg.setProtocol("test");

        assertEquals("test", cfg.getProtocol());

        assertSame(cfg, cfg.withProtocol("test2"));

        assertEquals("test2", cfg.getProtocol());
    }

    @Test
    public void testCodecFactory() throws Exception {
        assertNull(cfg.getCodecFactory());

        JdkCodecFactory<Object> factory1 = new JdkCodecFactory<>();
        JdkCodecFactory<Object> factory2 = new JdkCodecFactory<>();

        cfg.setCodecFactory(factory1);

        assertSame(factory1, cfg.getCodecFactory());

        assertSame(cfg, cfg.withCodecFactory(factory2));

        assertSame(factory2, cfg.getCodecFactory());
    }

    @Test
    public void testHandler() throws Exception {
        assertNull(cfg.getHandler());

        NetworkServerHandler<Object> handler1 = (message, from) -> {
            // No-op.
        };
        NetworkServerHandler<Object> handler2 = (message, from) -> {
            // No-op.
        };

        cfg.setHandler(handler1);

        assertSame(handler1, cfg.getHandler());

        assertSame(cfg, cfg.withHandler(handler2));

        assertSame(handler2, cfg.getHandler());
    }

    @Test
    public void testLoggerCategory() {
        assertNull(cfg.getLoggerCategory());

        cfg.setLoggerCategory("test");

        assertEquals("test", cfg.getLoggerCategory());

        assertSame(cfg, cfg.withLoggerCategory("test2"));

        assertEquals("test2", cfg.getLoggerCategory());
    }
}
