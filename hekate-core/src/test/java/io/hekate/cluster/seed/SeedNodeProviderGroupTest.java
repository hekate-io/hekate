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

package io.hekate.cluster.seed;

import io.hekate.HekateTestBase;
import io.hekate.core.HekateException;
import io.hekate.test.HekateTestError;
import io.hekate.util.format.ToString;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SeedNodeProviderGroupTest extends HekateTestBase {
    private static final String CLUSTER = "whatever";

    private InetSocketAddress selfAddr;

    private SeedNodeProvider p1;

    private SeedNodeProvider p2;

    @Before
    public void setUp() throws Exception {
        selfAddr = newSocketAddress();

        p1 = mock(SeedNodeProvider.class);
        p2 = mock(SeedNodeProvider.class);
    }

    @Test
    public void testFindWithFailOnFirstErrorWhenFirstProviderFails() throws Exception {
        SeedNodeProviderGroup group = new SeedNodeProviderGroup(new SeedNodeProviderGroupConfig()
            .withPolicy(SeedNodeProviderGroupPolicy.FAIL_ON_FIRST_ERROR)
            .withProvider(p1)
            .withProvider(p2)
        );

        group.startDiscovery(CLUSTER, selfAddr);
        verify(p1).startDiscovery(CLUSTER, selfAddr);
        verify(p2).startDiscovery(CLUSTER, selfAddr);

        when(p1.findSeedNodes(anyString())).thenThrow(new HekateException(HekateTestError.MESSAGE));

        expectExactMessage(HekateException.class, HekateTestError.MESSAGE, () ->
            group.findSeedNodes(CLUSTER)
        );

        verify(p1).findSeedNodes(CLUSTER);
        verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void testFindWithFailOnFirstErrorWhenSecondProviderFails() throws Exception {
        SeedNodeProviderGroup group = new SeedNodeProviderGroup(new SeedNodeProviderGroupConfig()
            .withPolicy(SeedNodeProviderGroupPolicy.FAIL_ON_FIRST_ERROR)
            .withProvider(p1)
            .withProvider(p2)
        );

        group.startDiscovery(CLUSTER, selfAddr);
        verify(p1).startDiscovery(CLUSTER, selfAddr);
        verify(p2).startDiscovery(CLUSTER, selfAddr);

        when(p2.findSeedNodes(anyString())).thenThrow(new HekateException(HekateTestError.MESSAGE));

        expectExactMessage(HekateException.class, HekateTestError.MESSAGE, () ->
            group.findSeedNodes(CLUSTER)
        );

        verify(p1).findSeedNodes(CLUSTER);
        verify(p2).findSeedNodes(CLUSTER);
        verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void testFindWithIgnorePartialErrorsWhenFirstProviderFails() throws Exception {
        SeedNodeProviderGroup group = new SeedNodeProviderGroup(new SeedNodeProviderGroupConfig()
            .withPolicy(SeedNodeProviderGroupPolicy.IGNORE_PARTIAL_ERRORS)
            .withProvider(p1)
            .withProvider(p2)
        );

        group.startDiscovery(CLUSTER, selfAddr);

        InetSocketAddress addr = newSocketAddress();

        when(p1.findSeedNodes(anyString())).thenThrow(new HekateException(HekateTestError.MESSAGE));
        when(p2.findSeedNodes(anyString())).thenReturn(Collections.singletonList(addr));

        assertEquals(Collections.singletonList(addr), group.findSeedNodes(CLUSTER));

        verify(p1).startDiscovery(CLUSTER, selfAddr);
        verify(p2).startDiscovery(CLUSTER, selfAddr);
        verify(p1).findSeedNodes(CLUSTER);
        verify(p2).findSeedNodes(CLUSTER);
        verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void testFindWithIgnorePartialErrorsWhenSecondProviderFails() throws Exception {
        SeedNodeProviderGroup group = new SeedNodeProviderGroup(new SeedNodeProviderGroupConfig()
            .withPolicy(SeedNodeProviderGroupPolicy.IGNORE_PARTIAL_ERRORS)
            .withProvider(p1)
            .withProvider(p2)
        );

        group.startDiscovery(CLUSTER, selfAddr);

        InetSocketAddress addr = newSocketAddress();

        when(p1.findSeedNodes(anyString())).thenReturn(Collections.singletonList(addr));
        when(p2.findSeedNodes(anyString())).thenThrow(new HekateException(HekateTestError.MESSAGE));

        assertEquals(Collections.singletonList(addr), group.findSeedNodes(CLUSTER));

        verify(p1).startDiscovery(CLUSTER, selfAddr);
        verify(p2).startDiscovery(CLUSTER, selfAddr);
        verify(p1).findSeedNodes(CLUSTER);
        verify(p2).findSeedNodes(CLUSTER);
        verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void testFindWithIgnorePartialErrorsWhenBothProvidersFail() throws Exception {
        SeedNodeProviderGroup group = new SeedNodeProviderGroup(new SeedNodeProviderGroupConfig()
            .withPolicy(SeedNodeProviderGroupPolicy.IGNORE_PARTIAL_ERRORS)
            .withProvider(p1)
            .withProvider(p2)
        );

        group.startDiscovery(CLUSTER, selfAddr);

        when(p1.findSeedNodes(anyString())).thenThrow(new HekateException(HekateTestError.MESSAGE));
        when(p2.findSeedNodes(anyString())).thenThrow(new HekateException(HekateTestError.MESSAGE));

        expectExactMessage(HekateException.class, "All seed node providers failed to find seed nodes.", () ->
            group.findSeedNodes(CLUSTER)
        );

        verify(p1).startDiscovery(CLUSTER, selfAddr);
        verify(p2).startDiscovery(CLUSTER, selfAddr);
        verify(p1).findSeedNodes(CLUSTER);
        verify(p2).findSeedNodes(CLUSTER);
        verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void testCleanupInterval() {
        SeedNodeProviderGroup group = new SeedNodeProviderGroup(new SeedNodeProviderGroupConfig()
            .withPolicy(SeedNodeProviderGroupPolicy.IGNORE_PARTIAL_ERRORS)
            .withProvider(p1)
            .withProvider(p2)
        );

        // Both don't have an interval.
        when(p1.cleanupInterval()).thenReturn(0L);
        when(p2.cleanupInterval()).thenReturn(0L);

        assertEquals(0, group.cleanupInterval());

        verify(p1).cleanupInterval();
        verify(p2).cleanupInterval();
        verifyNoMoreInteractions(p1, p2);

        // First has an interval.
        reset(p1, p2);

        when(p1.cleanupInterval()).thenReturn(100500L);
        when(p2.cleanupInterval()).thenReturn(0L);

        assertEquals(100500, group.cleanupInterval());

        verify(p1).cleanupInterval();
        verify(p2).cleanupInterval();
        verifyNoMoreInteractions(p1, p2);

        // Second has an interval.
        reset(p1, p2);

        when(p1.cleanupInterval()).thenReturn(0L);
        when(p2.cleanupInterval()).thenReturn(100500L);

        assertEquals(100500, group.cleanupInterval());

        verify(p1).cleanupInterval();
        verify(p2).cleanupInterval();
        verifyNoMoreInteractions(p1, p2);

        // Both have the same interval.
        reset(p1, p2);

        when(p1.cleanupInterval()).thenReturn(100500L);
        when(p2.cleanupInterval()).thenReturn(100500L);

        assertEquals(100500, group.cleanupInterval());

        verify(p1).cleanupInterval();
        verify(p2).cleanupInterval();
        verifyNoMoreInteractions(p1, p2);

        // First has a smaller interval.
        reset(p1, p2);

        when(p1.cleanupInterval()).thenReturn(1050L);
        when(p2.cleanupInterval()).thenReturn(100500L);

        assertEquals(1050, group.cleanupInterval());

        verify(p1).cleanupInterval();
        verify(p2).cleanupInterval();
        verifyNoMoreInteractions(p1, p2);

        // Second has a smaller interval.
        reset(p1, p2);

        when(p1.cleanupInterval()).thenReturn(100500L);
        when(p2.cleanupInterval()).thenReturn(1050L);

        assertEquals(1050, group.cleanupInterval());

        verify(p1).cleanupInterval();
        verify(p2).cleanupInterval();
        verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void testFindSeedNodes() throws Exception {
        InetSocketAddress addr1 = newSocketAddress();
        InetSocketAddress addr2 = newSocketAddress();

        when(p1.findSeedNodes(anyString())).thenReturn(Collections.singletonList(addr1));
        when(p2.findSeedNodes(anyString())).thenReturn(Collections.singletonList(addr2));

        SeedNodeProviderGroup group = new SeedNodeProviderGroup(new SeedNodeProviderGroupConfig()
            .withProvider(p1)
            .withProvider(p2)
        );

        group.startDiscovery(CLUSTER, selfAddr);

        List<InetSocketAddress> found = group.findSeedNodes(CLUSTER);

        assertEquals(Arrays.asList(addr1, addr2), found);

        verify(p1).startDiscovery(CLUSTER, selfAddr);
        verify(p2).startDiscovery(CLUSTER, selfAddr);
        verify(p1).findSeedNodes(CLUSTER);
        verify(p2).findSeedNodes(CLUSTER);
        verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void testStartDiscovery() throws Exception {
        SeedNodeProviderGroup group = new SeedNodeProviderGroup(new SeedNodeProviderGroupConfig()
            .withProvider(p1)
            .withProvider(p2)
        );

        InetSocketAddress addr = newSocketAddress();

        group.startDiscovery(CLUSTER, addr);

        assertEquals(2, group.liveProviders().size());
        assertTrue(group.liveProviders().contains(p1));
        assertTrue(group.liveProviders().contains(p2));

        verify(p1).startDiscovery(CLUSTER, addr);
        verify(p2).startDiscovery(CLUSTER, addr);
        verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void testStartDiscoveryWithFailOnFirstErrorWhenFirstProviderFails() throws Exception {
        SeedNodeProviderGroup group = new SeedNodeProviderGroup(new SeedNodeProviderGroupConfig()
            .withPolicy(SeedNodeProviderGroupPolicy.FAIL_ON_FIRST_ERROR)
            .withProvider(p1)
            .withProvider(p2)
        );

        doThrow(new HekateException(HekateTestError.MESSAGE)).when(p1).startDiscovery(CLUSTER, selfAddr);

        expectExactMessage(HekateException.class, HekateTestError.MESSAGE, () ->
            group.startDiscovery(CLUSTER, selfAddr)
        );

        assertTrue(group.liveProviders().isEmpty());

        verify(p1).startDiscovery(CLUSTER, selfAddr);
        verify(p1).stopDiscovery(CLUSTER, selfAddr);
        verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void testStartDiscoveryWithFailOnFirstErrorWhenSecondProviderFails() throws Exception {
        SeedNodeProviderGroup group = new SeedNodeProviderGroup(new SeedNodeProviderGroupConfig()
            .withPolicy(SeedNodeProviderGroupPolicy.FAIL_ON_FIRST_ERROR)
            .withProvider(p1)
            .withProvider(p2)
        );

        doThrow(new HekateException(HekateTestError.MESSAGE)).when(p2).startDiscovery(CLUSTER, selfAddr);

        expectExactMessage(HekateException.class, HekateTestError.MESSAGE, () ->
            group.startDiscovery(CLUSTER, selfAddr)
        );

        assertTrue(group.liveProviders().isEmpty());

        verify(p1).startDiscovery(CLUSTER, selfAddr);
        verify(p2).startDiscovery(CLUSTER, selfAddr);
        verify(p1).stopDiscovery(CLUSTER, selfAddr);
        verify(p2).stopDiscovery(CLUSTER, selfAddr);
        verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void testStartDiscoveryWithIgnorePartialErrorsWhenFirstProviderFails() throws Exception {
        SeedNodeProviderGroup group = new SeedNodeProviderGroup(new SeedNodeProviderGroupConfig()
            .withPolicy(SeedNodeProviderGroupPolicy.IGNORE_PARTIAL_ERRORS)
            .withProvider(p1)
            .withProvider(p2)
        );

        doThrow(new HekateException(HekateTestError.MESSAGE)).when(p1).startDiscovery(CLUSTER, selfAddr);

        group.startDiscovery(CLUSTER, selfAddr);

        assertEquals(1, group.liveProviders().size());
        assertTrue(group.liveProviders().contains(p2));

        verify(p1).startDiscovery(CLUSTER, selfAddr);
        verify(p1).stopDiscovery(CLUSTER, selfAddr);
        verify(p2).startDiscovery(CLUSTER, selfAddr);
        verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void testStartDiscoveryWithIgnorePartialErrorsWhenSecondProviderFails() throws Exception {
        SeedNodeProviderGroup group = new SeedNodeProviderGroup(new SeedNodeProviderGroupConfig()
            .withPolicy(SeedNodeProviderGroupPolicy.IGNORE_PARTIAL_ERRORS)
            .withProvider(p1)
            .withProvider(p2)
        );

        doThrow(new HekateException(HekateTestError.MESSAGE)).when(p2).startDiscovery(CLUSTER, selfAddr);

        group.startDiscovery(CLUSTER, selfAddr);

        assertEquals(1, group.liveProviders().size());
        assertTrue(group.liveProviders().contains(p1));

        verify(p1).startDiscovery(CLUSTER, selfAddr);
        verify(p2).startDiscovery(CLUSTER, selfAddr);
        verify(p2).stopDiscovery(CLUSTER, selfAddr);
        verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void testStartDiscoveryWithIgnorePartialErrorsWhenBothProvidersFail() throws Exception {
        SeedNodeProviderGroup group = new SeedNodeProviderGroup(new SeedNodeProviderGroupConfig()
            .withPolicy(SeedNodeProviderGroupPolicy.IGNORE_PARTIAL_ERRORS)
            .withProvider(p1)
            .withProvider(p2)
        );

        doThrow(new HekateException(HekateTestError.MESSAGE)).when(p1).startDiscovery(CLUSTER, selfAddr);
        doThrow(new HekateException(HekateTestError.MESSAGE)).when(p2).startDiscovery(CLUSTER, selfAddr);

        expectExactMessage(HekateException.class, "All seed node providers failed to start discovery.", () ->
            group.startDiscovery(CLUSTER, selfAddr)
        );

        assertTrue(group.liveProviders().isEmpty());

        verify(p1).startDiscovery(CLUSTER, selfAddr);
        verify(p1).stopDiscovery(CLUSTER, selfAddr);
        verify(p2).startDiscovery(CLUSTER, selfAddr);
        verify(p2).stopDiscovery(CLUSTER, selfAddr);
        verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void testStopDiscovery() throws Exception {
        SeedNodeProviderGroup group = new SeedNodeProviderGroup(new SeedNodeProviderGroupConfig()
            .withProvider(p1)
            .withProvider(p2)
        );

        group.startDiscovery(CLUSTER, selfAddr);

        assertEquals(2, group.liveProviders().size());

        group.stopDiscovery(CLUSTER, selfAddr);

        assertTrue(group.liveProviders().isEmpty());

        verify(p1).startDiscovery(CLUSTER, selfAddr);
        verify(p2).startDiscovery(CLUSTER, selfAddr);
        verify(p1).stopDiscovery(CLUSTER, selfAddr);
        verify(p2).stopDiscovery(CLUSTER, selfAddr);
        verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void testStopDiscoveryWithError() throws Exception {
        SeedNodeProviderGroup group = new SeedNodeProviderGroup(new SeedNodeProviderGroupConfig()
            .withProvider(p1)
            .withProvider(p2)
        );

        group.startDiscovery(CLUSTER, selfAddr);

        assertEquals(2, group.liveProviders().size());

        doThrow(new HekateException(HekateTestError.MESSAGE)).when(p1).stopDiscovery(CLUSTER, selfAddr);

        group.stopDiscovery(CLUSTER, selfAddr);

        assertTrue(group.liveProviders().isEmpty());

        verify(p1).startDiscovery(CLUSTER, selfAddr);
        verify(p2).startDiscovery(CLUSTER, selfAddr);
        verify(p1).stopDiscovery(CLUSTER, selfAddr);
        verify(p2).stopDiscovery(CLUSTER, selfAddr);
        verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void testSuspendDiscovery() throws Exception {
        SeedNodeProviderGroup group = new SeedNodeProviderGroup(new SeedNodeProviderGroupConfig()
            .withProvider(p1)
            .withProvider(p2)
        );

        group.startDiscovery(CLUSTER, selfAddr);

        group.suspendDiscovery();

        verify(p1).startDiscovery(CLUSTER, selfAddr);
        verify(p2).startDiscovery(CLUSTER, selfAddr);
        verify(p1).suspendDiscovery();
        verify(p2).suspendDiscovery();
        verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void testSuspendDiscoveryWithFailure() throws Exception {
        SeedNodeProviderGroup group = new SeedNodeProviderGroup(new SeedNodeProviderGroupConfig()
            .withPolicy(SeedNodeProviderGroupPolicy.FAIL_ON_FIRST_ERROR)
            .withProvider(p1)
            .withProvider(p2)
        );

        group.startDiscovery(CLUSTER, selfAddr);

        doThrow(new HekateException(HekateTestError.MESSAGE)).when(p1).suspendDiscovery();
        doThrow(new HekateException(HekateTestError.MESSAGE)).when(p2).suspendDiscovery();

        group.suspendDiscovery();

        verify(p1).startDiscovery(CLUSTER, selfAddr);
        verify(p2).startDiscovery(CLUSTER, selfAddr);
        verify(p1).suspendDiscovery();
        verify(p2).suspendDiscovery();
        verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void testRegisterRemote() throws Exception {
        SeedNodeProviderGroup group = new SeedNodeProviderGroup(new SeedNodeProviderGroupConfig()
            .withProvider(p1)
            .withProvider(p2)
        );

        group.startDiscovery(CLUSTER, selfAddr);

        InetSocketAddress addr = newSocketAddress();

        group.registerRemote(CLUSTER, addr);

        verify(p1).startDiscovery(CLUSTER, selfAddr);
        verify(p2).startDiscovery(CLUSTER, selfAddr);
        verify(p1).registerRemote(CLUSTER, addr);
        verify(p2).registerRemote(CLUSTER, addr);
        verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void testUnregisterRemote() throws Exception {
        SeedNodeProviderGroup group = new SeedNodeProviderGroup(new SeedNodeProviderGroupConfig()
            .withProvider(p1)
            .withProvider(p2)
        );

        group.startDiscovery(CLUSTER, selfAddr);

        InetSocketAddress addr = newSocketAddress();

        group.unregisterRemote(CLUSTER, addr);

        verify(p1).startDiscovery(CLUSTER, selfAddr);
        verify(p2).startDiscovery(CLUSTER, selfAddr);
        verify(p1).unregisterRemote(CLUSTER, addr);
        verify(p2).unregisterRemote(CLUSTER, addr);
        verifyNoMoreInteractions(p1, p2);
    }

    @Test
    public void testToString() {
        SeedNodeProvider provider = new SeedNodeProviderGroup(new SeedNodeProviderGroupConfig()
            .withPolicy(SeedNodeProviderGroupPolicy.IGNORE_PARTIAL_ERRORS)
            .withProvider(p1)
            .withProvider(p2)
        );

        assertEquals(ToString.format(provider), provider.toString());
    }
}
