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

package io.hekate.coordinate;

import io.hekate.HekateTestBase;
import io.hekate.codec.CodecFactory;
import io.hekate.util.format.ToString;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class CoordinationProcessConfigTest extends HekateTestBase {
    private final CoordinationProcessConfig cfg = new CoordinationProcessConfig();

    @Test
    public void testName() {
        assertNull(cfg.getName());

        cfg.setName("test");

        assertEquals("test", cfg.getName());

        cfg.setName(null);

        assertNull(cfg.getName());

        assertSame(cfg, cfg.withName("test"));

        assertEquals("test", cfg.getName());

        assertEquals("test3", new CoordinationProcessConfig("test3").getName());
    }

    @Test
    public void testHandler() {
        assertNull(cfg.getHandler());

        CoordinationHandler handler = mock(CoordinationHandler.class);

        cfg.setHandler(handler);

        assertSame(handler, cfg.getHandler());

        cfg.setHandler(null);

        assertNull(cfg.getHandler());

        assertSame(cfg, cfg.withHandler(handler));

        assertSame(handler, cfg.getHandler());
    }

    @Test
    public void testMessageCodec() {
        assertNull(cfg.getMessageCodec());

        CodecFactory<Object> factory = () -> null;

        cfg.setMessageCodec(factory);

        assertSame(factory, cfg.getMessageCodec());

        cfg.setMessageCodec(null);

        assertNull(cfg.getMessageCodec());

        assertSame(cfg, cfg.withMessageCodec(factory));

        assertSame(factory, cfg.getMessageCodec());
    }

    @Test
    public void testAsyncInit() {
        assertTrue(cfg.isAsyncInit());

        cfg.setAsyncInit(false);

        assertFalse(cfg.isAsyncInit());

        assertSame(cfg, cfg.withAsyncInit(true));

        assertTrue(cfg.isAsyncInit());
    }

    @Test
    public void testToString() {
        assertEquals(ToString.format(cfg), cfg.toString());
    }
}
