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

package io.hekate.cluster.internal.gossip;

import io.hekate.HekateTestBase;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class GossipSeedNodesSateTest extends HekateTestBase {
    @Test
    public void testEmpty() throws Exception {
        GossipSeedNodesSate s = new GossipSeedNodesSate(newSocketAddress(1), Collections.emptyList());

        assertTrue(s.isSelfJoin());
        assertNull(s.nextSeed());
    }

    @Test
    public void testNextSeed() throws Exception {
        List<InetSocketAddress> seeds = new ArrayList<>();

        seeds.add(newSocketAddress(1));
        seeds.add(newSocketAddress(2));
        seeds.add(newSocketAddress(3));

        GossipSeedNodesSate s = new GossipSeedNodesSate(newSocketAddress(1000), seeds);

        assertFalse(s.isSelfJoin());

        repeat(3, i -> {
            for (InetSocketAddress addr : seeds) {
                assertEquals(addr, s.nextSeed());
            }
        });
    }

    @Test
    public void testNextSeedWithUpdateAdd() throws Exception {
        List<InetSocketAddress> seeds = new ArrayList<>();

        seeds.add(newSocketAddress(10));
        seeds.add(newSocketAddress(20));
        seeds.add(newSocketAddress(30));

        GossipSeedNodesSate s = new GossipSeedNodesSate(newSocketAddress(1000), seeds);

        assertFalse(s.isSelfJoin());

        repeat(3, i -> {
            for (InetSocketAddress addr : seeds) {
                assertEquals(addr, s.nextSeed());
            }
        });

        seeds.add(newSocketAddress(40));

        s.update(seeds);

        assertEquals(seeds.get(seeds.size() - 1), s.nextSeed());

        seeds.add(newSocketAddress(1));

        s.update(seeds);

        assertEquals(seeds.get(seeds.size() - 1), s.nextSeed());
    }

    @Test
    public void testNextSeedWithUpdateRemove() throws Exception {
        List<InetSocketAddress> seeds = new ArrayList<>();

        seeds.add(newSocketAddress(10));
        seeds.add(newSocketAddress(20));
        seeds.add(newSocketAddress(30));

        GossipSeedNodesSate s = new GossipSeedNodesSate(newSocketAddress(1000), seeds);

        assertFalse(s.isSelfJoin());

        repeat(3, i -> {
            for (InetSocketAddress addr : seeds) {
                assertEquals(addr, s.nextSeed());
            }
        });

        seeds.remove(0);

        s.update(seeds);

        assertEquals(seeds.get(0), s.nextSeed());

        seeds.remove(seeds.get(seeds.size() - 1));

        s.update(seeds);

        assertEquals(seeds.get(0), s.nextSeed());
    }

    @Test
    public void testNextSeedWithUpdateRemoveLastTried() throws Exception {
        List<InetSocketAddress> seeds = new ArrayList<>();

        seeds.add(newSocketAddress(10));
        seeds.add(newSocketAddress(20));
        seeds.add(newSocketAddress(30));

        GossipSeedNodesSate s = new GossipSeedNodesSate(newSocketAddress(1000), seeds);

        assertEquals(seeds.get(0), s.nextSeed());
        assertEquals(seeds.get(1), s.nextSeed());

        seeds.remove(1);

        s.update(seeds);

        assertEquals(seeds.get(0), s.nextSeed());
    }

    @Test
    public void testNextSeedWithAllReject() throws Exception {
        List<InetSocketAddress> seeds = new ArrayList<>();

        seeds.add(newSocketAddress(1));
        seeds.add(newSocketAddress(2));
        seeds.add(newSocketAddress(3));

        GossipSeedNodesSate s = new GossipSeedNodesSate(newSocketAddress(1000), seeds);

        assertFalse(s.isSelfJoin());

        repeat(3, i -> {
            for (InetSocketAddress addr : seeds) {
                assertEquals(addr, s.nextSeed());
            }

            seeds.forEach(s::onReject);

            for (InetSocketAddress addr : seeds) {
                assertEquals(addr, s.nextSeed());
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

        GossipSeedNodesSate s = new GossipSeedNodesSate(newSocketAddress(1000), seeds);

        assertFalse(s.isSelfJoin());

        repeat(3, i -> {
            for (InetSocketAddress addr : seeds) {
                assertEquals(addr, s.nextSeed());
            }

            seeds.stream()
                .filter(rejects::contains)
                .forEach(s::onReject);

            for (InetSocketAddress addr : seeds) {
                assertEquals(addr, s.nextSeed());
            }
        });
    }

    @Test
    public void testSelfJoinIfAllFailed() throws Exception {
        List<InetSocketAddress> seeds = new ArrayList<>();

        seeds.add(newSocketAddress(10));
        seeds.add(newSocketAddress(20));
        seeds.add(newSocketAddress(30));

        GossipSeedNodesSate s = new GossipSeedNodesSate(newSocketAddress(1000), seeds);

        for (InetSocketAddress addr : seeds) {
            assertEquals(addr, s.nextSeed());
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

        GossipSeedNodesSate s = new GossipSeedNodesSate(newSocketAddress(1000), seeds);

        for (InetSocketAddress addr : seeds) {
            assertEquals(addr, s.nextSeed());
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

        GossipSeedNodesSate s = new GossipSeedNodesSate(newSocketAddress(1), seeds);

        for (InetSocketAddress addr : seeds) {
            assertEquals(addr, s.nextSeed());
        }

        seeds.forEach(s::onReject);

        assertTrue(s.isSelfJoin());
    }
}
