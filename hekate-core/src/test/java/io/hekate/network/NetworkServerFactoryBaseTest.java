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
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class NetworkServerFactoryBaseTest extends HekateTestBase {
    private final NetworkServerFactoryBase cfg = new NetworkServerFactoryBase() {
        @Override
        public NetworkServer createServer() {
            return null;
        }
    };

    @Test
    public void testAutoAccept() {
        assertTrue(cfg.isAutoAccept());

        cfg.setAutoAccept(false);

        assertFalse(cfg.isAutoAccept());

        assertSame(cfg, cfg.withAutoAccept(true));

        assertTrue(cfg.isAutoAccept());
    }

    @Test
    public void testHeartbeatInterval() {
        assertEquals(NetworkServiceFactory.DEFAULT_HB_INTERVAL, cfg.getHeartbeatInterval());

        cfg.setHeartbeatInterval(NetworkServiceFactory.DEFAULT_HB_INTERVAL + 1000);

        assertEquals(NetworkServiceFactory.DEFAULT_HB_INTERVAL + 1000, cfg.getHeartbeatInterval());

        assertSame(cfg, cfg.withHeartbeatInterval(10001));
        assertEquals(10001, cfg.getHeartbeatInterval());
    }

    @Test
    public void testHeartbeatLossThreshold() {
        assertEquals(NetworkServiceFactory.DEFAULT_HB_LOSS_THRESHOLD, cfg.getHeartbeatLossThreshold());

        cfg.setHeartbeatLossThreshold(NetworkServiceFactory.DEFAULT_HB_LOSS_THRESHOLD + 1000);

        assertEquals(NetworkServiceFactory.DEFAULT_HB_LOSS_THRESHOLD + 1000, cfg.getHeartbeatLossThreshold());

        assertSame(cfg, cfg.withHeartbeatLossThreshold(10002));
        assertEquals(10002, cfg.getHeartbeatLossThreshold());
    }

    @Test
    public void testTcpNoDelay() {
        assertTrue(cfg.isTcpNoDelay());

        cfg.setTcpNoDelay(true);

        assertTrue(cfg.isTcpNoDelay());

        cfg.setTcpNoDelay(false);

        assertFalse(cfg.isTcpNoDelay());

        assertSame(cfg, cfg.withTcpNoDelay(true));
        assertTrue(cfg.isTcpNoDelay());
    }

    @Test
    public void testSoReceiveBufferSize() {
        assertNull(cfg.getSoReceiveBufferSize());

        cfg.setSoReceiveBufferSize(1000);

        assertEquals(1000, cfg.getSoReceiveBufferSize().intValue());

        cfg.setSoReceiveBufferSize(null);

        assertNull(cfg.getSoReceiveBufferSize());

        assertSame(cfg, cfg.withSoReceiveBufferSize(100));
        assertEquals(100, cfg.getSoReceiveBufferSize().intValue());
    }

    @Test
    public void testSoSendBufferSize() {
        assertNull(cfg.getSoSendBufferSize());

        cfg.setSoSendBufferSize(1000);

        assertEquals(1000, cfg.getSoSendBufferSize().intValue());

        cfg.setSoSendBufferSize(null);

        assertNull(cfg.getSoSendBufferSize());

        assertSame(cfg, cfg.withSoSendBufferSize(1001));
        assertEquals(1001, cfg.getSoSendBufferSize().intValue());
    }

    @Test
    public void testSoReuseAddress() {
        assertNull(cfg.getSoReuseAddress());

        cfg.setSoReuseAddress(true);

        assertTrue(cfg.getSoReuseAddress());

        cfg.setSoReuseAddress(false);

        assertFalse(cfg.getSoReuseAddress());

        cfg.setSoReuseAddress(null);

        assertNull(cfg.getSoReuseAddress());

        assertSame(cfg, cfg.withSoReuseAddress(true));
        assertTrue(cfg.getSoReuseAddress());
    }

    @Test
    public void testSoBacklog() {
        assertNull(cfg.getSoBacklog());

        cfg.setSoBacklog(1000);

        assertEquals(1000, cfg.getSoBacklog().intValue());

        cfg.setSoBacklog(null);

        assertNull(cfg.getSoBacklog());

        assertSame(cfg, cfg.withSoBacklog(101));
        assertEquals(101, cfg.getSoBacklog().intValue());
    }
}
