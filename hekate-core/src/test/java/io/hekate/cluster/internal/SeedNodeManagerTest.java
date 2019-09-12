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

package io.hekate.cluster.internal;

import io.hekate.HekateTestBase;
import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.cluster.seed.SeedNodeProviderAdaptor;
import io.hekate.core.HekateException;
import io.hekate.network.NetworkPingCallback;
import io.hekate.network.NetworkPingResult;
import io.hekate.network.NetworkService;
import io.hekate.test.HekateTestError;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SeedNodeManagerTest extends HekateTestBase {
    private SeedNodeProvider provider;

    private SeedNodeManager manager;

    @Before
    public void setUp() {
        provider = mock(SeedNodeProvider.class);

        manager = createManager(provider);

        // Ignore initialization methods that manager calls on the provider.
        reset(provider);
    }

    @Test
    public void testNeverReturnsNull() throws Exception {
        when(provider.findSeedNodes(any(String.class))).thenReturn(null);

        List<InetSocketAddress> nodes = manager.getSeedNodes();

        assertNotNull(nodes);
        assertTrue(nodes.isEmpty());

        verify(provider).findSeedNodes(any(String.class));
        verifyNoMoreInteractions(provider);
    }

    @Test
    public void testErrorOnGetSeedNodes() throws Exception {
        when(provider.findSeedNodes(any(String.class))).thenThrow(TEST_ERROR);

        try {
            manager.getSeedNodes();

            fail("Error was expected.");
        } catch (AssertionError e) {
            assertEquals(HekateTestError.MESSAGE, e.getMessage());
        }

        verify(provider).findSeedNodes(any(String.class));
        verifyNoMoreInteractions(provider);
    }

    @Test
    public void testErrorOnStartDiscovery() throws Exception {
        doThrow(TEST_ERROR).when(provider).startDiscovery(any(String.class), any(InetSocketAddress.class));

        try {
            manager.startDiscovery(newSocketAddress());

            fail("Error was expected.");
        } catch (AssertionError e) {
            assertEquals(HekateTestError.MESSAGE, e.getMessage());
        }

        verify(provider).startDiscovery(any(String.class), any(InetSocketAddress.class));
        verifyNoMoreInteractions(provider);
    }

    @Test
    public void testNoErrorOnSuspendDiscovery() throws Exception {
        doThrow(TEST_ERROR).when(provider).suspendDiscovery();

        manager.suspendDiscovery();

        verify(provider).suspendDiscovery();
        verifyNoMoreInteractions(provider);
    }

    @Test
    public void testStopDiscoveryWithoutStartDiscovery() throws Exception {
        manager.stopDiscovery(newSocketAddress());

        verifyNoMoreInteractions(provider);
    }

    @Test
    public void testNoErrorOnStopDiscovery() throws Exception {
        doThrow(TEST_ERROR).when(provider).stopDiscovery(any(String.class), any(InetSocketAddress.class));

        manager.startDiscovery(newSocketAddress());

        manager.stopDiscovery(newSocketAddress());

        verify(provider).startDiscovery(any(String.class), any(InetSocketAddress.class));
        verify(provider).stopDiscovery(any(String.class), any(InetSocketAddress.class));
        verifyNoMoreInteractions(provider);
    }

    @Test
    public void testStopCleaningWithoutStarting() throws Exception {
        manager.stopCleaning();
        manager.stopCleaning();
        manager.stopCleaning();
    }

    @Test
    public void testToString() throws Exception {
        assertEquals(provider.toString(), manager.toString());
    }

    @Test
    public void testCleaning() throws Exception {
        Map<InetSocketAddress, Boolean> addresses = new ConcurrentHashMap<>();

        InetSocketAddress address1 = newSocketAddress();
        InetSocketAddress address2 = newSocketAddress();
        InetSocketAddress address3 = newSocketAddress();

        addresses.put(address1, true);
        addresses.put(address2, true);
        addresses.put(address3, true);

        Map<InetSocketAddress, Boolean> canPing = new ConcurrentHashMap<>(addresses);
        Map<InetSocketAddress, Boolean> alive = new ConcurrentHashMap<>(addresses);

        Exchanger<String> latch = new Exchanger<>();

        SeedNodeManager manager = createManager(new SeedNodeProviderAdaptor() {
            @Override
            public List<InetSocketAddress> findSeedNodes(String cluster) throws HekateException {
                return new ArrayList<>(addresses.keySet());
            }

            @Override
            public long cleanupInterval() {
                return 1;
            }

            @Override
            public void registerRemote(String cluster, InetSocketAddress address) throws HekateException {
                addresses.put(address, true);

                try {
                    latch.exchange("register-" + address, AWAIT_TIMEOUT, TimeUnit.SECONDS);
                } catch (InterruptedException | TimeoutException e) {
                    throw new HekateException("Unexpected timing error.", e);
                }
            }

            @Override
            public void unregisterRemote(String cluster, InetSocketAddress address) throws HekateException {
                addresses.remove(address);

                try {
                    latch.exchange("unregister-" + address, AWAIT_TIMEOUT, TimeUnit.SECONDS);
                } catch (InterruptedException | TimeoutException e) {
                    throw new HekateException("Unexpected timing error.", e);
                }
            }
        });

        NetworkService netMock = mock(NetworkService.class);

        doAnswer(invocation -> {
            InetSocketAddress address = (InetSocketAddress)invocation.getArguments()[0];
            NetworkPingCallback callback = (NetworkPingCallback)invocation.getArguments()[1];

            if (canPing.containsKey(address)) {
                callback.onResult(address, NetworkPingResult.SUCCESS);
            } else {
                callback.onResult(address, NetworkPingResult.FAILURE);
            }

            return null;
        }).when(netMock).ping(any(InetSocketAddress.class), any(NetworkPingCallback.class));

        manager.startCleaning(netMock, alive::keySet);

        try {
            List<InetSocketAddress> nodes = manager.getSeedNodes();

            assertTrue(nodes.contains(address1));
            assertTrue(nodes.contains(address2));
            assertTrue(nodes.contains(address3));

            canPing.remove(address2);
            alive.remove(address2);

            assertEquals("unregister-" + address2, latch.exchange(null, AWAIT_TIMEOUT, TimeUnit.SECONDS));

            nodes = manager.getSeedNodes();

            assertTrue(nodes.contains(address1));
            assertFalse(nodes.contains(address2));
            assertTrue(nodes.contains(address3));

            alive.put(address2, true);

            assertEquals("register-" + address2, latch.exchange(null, AWAIT_TIMEOUT, TimeUnit.SECONDS));

            nodes = manager.getSeedNodes();

            assertTrue(nodes.contains(address1));
            assertTrue(nodes.contains(address2));
            assertTrue(nodes.contains(address3));
        } finally {
            manager.stopCleaning().await();
        }
    }

    private SeedNodeManager createManager(SeedNodeProvider provider) {
        return new SeedNodeManager("test", provider);
    }
}
