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

package io.hekate.network.internal;

import io.hekate.HekateNodeTestBase;
import io.hekate.core.Hekate;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.network.NetworkPingCallback;
import io.hekate.network.NetworkPingResult;
import io.hekate.network.NetworkServiceFactory;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class NetworkPingTest extends HekateNodeTestBase {
    private static class TestNetworkPingCallback implements NetworkPingCallback {
        private final CountDownLatch latch = new CountDownLatch(1);

        private final AtomicReference<NetworkPingResult> resultRef = new AtomicReference<>();

        private final AtomicReference<AssertionError> errorRef = new AtomicReference<>();

        @Override
        public void onResult(InetSocketAddress address, NetworkPingResult result) {
            try {
                assertTrue(resultRef.compareAndSet(null, result));

            } catch (AssertionError e) {
                errorRef.compareAndSet(null, e);
            } finally {
                latch.countDown();
            }
        }

        public NetworkPingResult get() throws InterruptedException {
            await(latch);

            if (errorRef.get() != null) {
                throw errorRef.get();
            }

            return resultRef.get();
        }
    }

    @Test
    public void testSuccess() throws Exception {
        List<HekateTestNode> nodes = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            HekateTestNode node = createNode();

            nodes.add(node);

            node.join();
        }

        awaitForTopology(nodes);

        for (HekateTestNode source : nodes) {
            List<TestNetworkPingCallback> callbacks = new ArrayList<>();

            for (HekateTestNode target : nodes) {
                TestNetworkPingCallback callback = new TestNetworkPingCallback();

                callbacks.add(callback);

                source.network().ping(target.localNode().socket(), callback);
            }

            for (TestNetworkPingCallback callback : callbacks) {
                assertSame(NetworkPingResult.SUCCESS, callback.get());
            }
        }
    }

    @Test
    public void testConnectFailure() throws Exception {
        Hekate hekate = createNode(boot ->
            boot.withService(NetworkServiceFactory.class, net ->
                net.setConnectTimeout(3000)
            )
        ).join();

        TestNetworkPingCallback callback = new TestNetworkPingCallback();

        hekate.network().ping(new InetSocketAddress("127.0.0.1", 12765), callback);

        assertSame(NetworkPingResult.FAILURE, callback.get());
    }

    @Test
    public void testUnresolvedHostFailure() throws Exception {
        Hekate hekate = createNode().join();

        TestNetworkPingCallback callback = new TestNetworkPingCallback();

        hekate.network().ping(new InetSocketAddress("non-existing-host.com", 12765), callback);

        assertSame(NetworkPingResult.FAILURE, callback.get());
    }

    @Test
    public void testTimeout() throws Exception {
        Hekate hekate = createNode(c -> c.service(NetworkServiceFactory.class).get().setConnectTimeout(1)).join();

        TestNetworkPingCallback callback = new TestNetworkPingCallback();

        hekate.network().ping(new InetSocketAddress("hekate.io", 12765), callback);

        assertSame(NetworkPingResult.TIMEOUT, callback.get());
    }
}
