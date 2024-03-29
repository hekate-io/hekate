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

package io.hekate.cluster.internal.gossip;

import io.hekate.HekateTestBase;
import io.hekate.network.NetworkConnectTimeoutException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class GossipSeedNodesSateTest extends HekateTestBase {
    @Test
    public void testEmpty() throws Exception {
        GossipSeedNodesSate s = new GossipSeedNodesSate(newSocketAddress(1), emptyList(), false);

        assertTrue(s.isSelfJoin());
        assertNull(s.nextSeed(false));
    }

    @Test
    public void testNextSeed() throws Exception {
        List<InetSocketAddress> seeds = new ArrayList<>();

        seeds.add(newSocketAddress(1));
        seeds.add(newSocketAddress(2));
        seeds.add(newSocketAddress(3));

        GossipSeedNodesSate s = new GossipSeedNodesSate(newSocketAddress(1000), seeds, false);

        assertFalse(s.isSelfJoin());

        repeat(3, i -> {
            for (InetSocketAddress addr : seeds) {
                assertEquals(addr, s.nextSeed(false));
            }
        });
    }

    @Test
    public void testOrderOnNetworkTimeout() throws Exception {
        List<InetSocketAddress> seeds = new ArrayList<>();

        seeds.add(newSocketAddress(100));
        seeds.add(newSocketAddress(200));
        seeds.add(newSocketAddress(300));

        for (int localPort = 0; localPort <= 303; localPort += 101) {
            say("Local port: " + localPort);

            GossipSeedNodesSate s = new GossipSeedNodesSate(newSocketAddress(localPort), seeds, false);

            repeat(3, i ->
                seeds.forEach(seed -> {
                    assertFalse(s.isSelfJoin());
                    assertEquals(seed, s.nextSeed(false));

                    s.onFailure(seed, new NetworkConnectTimeoutException(TEST_ERROR.getMessage()));
                })
            );
        }
    }

    @Test
    public void testNextSeedBusy() throws Exception {
        List<InetSocketAddress> seeds = new ArrayList<>();

        seeds.add(newSocketAddress(1));
        seeds.add(newSocketAddress(2));
        seeds.add(newSocketAddress(3));

        GossipSeedNodesSate s = new GossipSeedNodesSate(newSocketAddress(1000), seeds, false);

        assertFalse(s.isSelfJoin());

        repeat(3, i -> {
            for (InetSocketAddress addr : seeds) {
                assertEquals(addr, s.nextSeed(false));
            }
        });
    }

    @Test
    public void testNextSeedInflight() throws Exception {
        List<InetSocketAddress> seeds = new ArrayList<>();

        seeds.add(newSocketAddress(1));
        seeds.add(newSocketAddress(2));
        seeds.add(newSocketAddress(3));

        GossipSeedNodesSate s = new GossipSeedNodesSate(newSocketAddress(1000), seeds, false);

        assertFalse(s.isSelfJoin());

        repeat(3, i -> {
            for (InetSocketAddress addr : seeds) {
                assertEquals(addr, s.nextSeed());
            }

            assertNull(s.nextSeed());

            seeds.forEach(s::onSendComplete);
        });
    }

    @Test
    public void testNextSeedWithUpdateAdd() throws Exception {
        List<InetSocketAddress> seeds = new ArrayList<>();

        seeds.add(newSocketAddress(10));
        seeds.add(newSocketAddress(20));
        seeds.add(newSocketAddress(30));

        GossipSeedNodesSate s = new GossipSeedNodesSate(newSocketAddress(1000), seeds, false);

        assertFalse(s.isSelfJoin());

        repeat(3, i -> {
            for (InetSocketAddress addr : seeds) {
                assertEquals(addr, s.nextSeed(false));
            }
        });

        seeds.add(newSocketAddress(40));

        s.update(seeds);

        assertEquals(seeds.get(seeds.size() - 1), s.nextSeed(false));

        seeds.add(newSocketAddress(1));

        s.update(seeds);

        assertEquals(seeds.get(seeds.size() - 1), s.nextSeed(false));
    }

    @Test
    public void testNextSeedWithUpdateRemove() throws Exception {
        List<InetSocketAddress> seeds = new ArrayList<>();

        seeds.add(newSocketAddress(10));
        seeds.add(newSocketAddress(20));
        seeds.add(newSocketAddress(30));

        GossipSeedNodesSate s = new GossipSeedNodesSate(newSocketAddress(1000), seeds, false);

        assertFalse(s.isSelfJoin());

        repeat(3, i -> {
            for (InetSocketAddress addr : seeds) {
                assertEquals(addr, s.nextSeed(false));
            }
        });

        seeds.remove(0);

        s.update(seeds);

        assertEquals(seeds.get(0), s.nextSeed(false));

        seeds.remove(seeds.get(seeds.size() - 1));

        s.update(seeds);

        assertEquals(seeds.get(0), s.nextSeed(false));
    }

    @Test
    public void testNextSeedWithUpdateRemoveLastTried() throws Exception {
        List<InetSocketAddress> seeds = new ArrayList<>();

        seeds.add(newSocketAddress(10));
        seeds.add(newSocketAddress(20));
        seeds.add(newSocketAddress(30));

        GossipSeedNodesSate s = new GossipSeedNodesSate(newSocketAddress(1000), seeds, false);

        assertEquals(seeds.get(0), s.nextSeed(false));
        assertEquals(seeds.get(1), s.nextSeed(false));

        seeds.remove(1);

        s.update(seeds);

        assertEquals(seeds.get(0), s.nextSeed(false));
    }

    @Test
    public void testNextSeedWithAllReject() throws Exception {
        List<InetSocketAddress> seeds = new ArrayList<>();

        seeds.add(newSocketAddress(1));
        seeds.add(newSocketAddress(2));
        seeds.add(newSocketAddress(3));

        GossipSeedNodesSate s = new GossipSeedNodesSate(newSocketAddress(1000), seeds, false);

        assertFalse(s.isSelfJoin());

        repeat(3, i -> {
            for (InetSocketAddress addr : seeds) {
                assertEquals(addr, s.nextSeed(false));
            }

            seeds.forEach(s::onReject);

            for (InetSocketAddress addr : seeds) {
                assertEquals(addr, s.nextSeed(false));
            }
        });
    }

    @Test
    public void testNextSeedWithPartialReject() throws Exception {
        List<InetSocketAddress> seeds = new ArrayList<>();

        seeds.add(newSocketAddress(1));
        seeds.add(newSocketAddress(2));
        seeds.add(newSocketAddress(3));
        seeds.add(newSocketAddress(4));
        seeds.add(newSocketAddress(5));

        Set<InetSocketAddress> rejects = new HashSet<>();

        rejects.add(seeds.get(1));
        rejects.add(seeds.get(3));
        rejects.add(seeds.get(4));

        GossipSeedNodesSate s = new GossipSeedNodesSate(newSocketAddress(1000), seeds, false);

        assertFalse(s.isSelfJoin());

        repeat(3, i -> {
            for (InetSocketAddress addr : seeds) {
                assertEquals(addr, s.nextSeed(false));
            }

            seeds.stream()
                .filter(rejects::contains)
                .forEach(s::onReject);

            for (InetSocketAddress addr : seeds) {
                assertEquals(addr, s.nextSeed(false));
            }
        });
    }

    @Test
    public void testSelfJoinIfAllFailed() throws Exception {
        List<InetSocketAddress> seeds = new ArrayList<>();

        seeds.add(newSocketAddress(10));
        seeds.add(newSocketAddress(20));
        seeds.add(newSocketAddress(30));

        GossipSeedNodesSate s = new GossipSeedNodesSate(newSocketAddress(1000), seeds, false);

        for (InetSocketAddress addr : seeds) {
            assertEquals(addr, s.nextSeed(false));
        }

        seeds.forEach(seed -> s.onFailure(seed, TEST_ERROR));

        assertTrue(s.isSelfJoin());
    }

    @Test
    public void testSelfJoinIfAllBanned() throws Exception {
        List<InetSocketAddress> seeds = new ArrayList<>();

        seeds.add(newSocketAddress(10));
        seeds.add(newSocketAddress(20));
        seeds.add(newSocketAddress(30));

        GossipSeedNodesSate s = new GossipSeedNodesSate(newSocketAddress(1000), seeds, false);

        for (InetSocketAddress addr : seeds) {
            assertEquals(addr, s.nextSeed(false));
        }

        seeds.forEach(s::onBan);

        assertTrue(s.isSelfJoin());
    }

    @Test
    public void testSelfJoinIfHasHigherAddress() throws Exception {
        List<InetSocketAddress> seeds = new ArrayList<>();

        seeds.add(newSocketAddress(10));
        seeds.add(newSocketAddress(20));
        seeds.add(newSocketAddress(30));

        GossipSeedNodesSate s = new GossipSeedNodesSate(newSocketAddress(1), seeds, false);

        for (InetSocketAddress addr : seeds) {
            assertEquals(addr, s.nextSeed(false));
        }

        seeds.forEach(s::onReject);

        assertTrue(s.isSelfJoin());
    }
}
