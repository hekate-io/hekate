/*
 * Copyright 2022 The Hekate Project
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

package io.hekate.network.netty;

import io.hekate.HekateTestBase;
import io.hekate.util.format.ToString;
import io.netty.channel.EventLoopGroup;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class NettyServerHandlerConfigTest extends HekateTestBase {
    private final NettyServerHandlerConfig<?> cfg = new NettyServerHandlerConfig<>();

    @Test
    public void testEventLoop() {
        EventLoopGroup eventLoop = mock(EventLoopGroup.class);

        assertNull(cfg.getEventLoop());

        cfg.setEventLoop(eventLoop);

        assertSame(eventLoop, cfg.getEventLoop());

        cfg.setEventLoop(null);

        assertNull(cfg.getEventLoop());

        assertSame(cfg, cfg.withEventLoop(eventLoop));
        assertSame(eventLoop, cfg.getEventLoop());
    }

    @Test
    public void testToString() {
        assertEquals(ToString.format(cfg), cfg.toString());
    }
}
