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

package io.hekate.network.address;

import io.hekate.HekateTestBase;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.AddressUtils;
import io.hekate.util.format.ToString;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AddressPatternTest extends HekateTestBase {
    private static class SupportedInterface {
        private final NetworkInterface networkInterface;

        private final List<InetAddress> addresses;

        public SupportedInterface(NetworkInterface networkInterface, List<InetAddress> addresses) {
            this.networkInterface = networkInterface;
            this.addresses = addresses;
        }

        public NetworkInterface getNetworkInterface() {
            return networkInterface;
        }

        public List<InetAddress> getAddresses() {
            return addresses;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[name=" + networkInterface.getName() + ", addresses=" + addresses + ']';
        }
    }

    private static List<NetworkInterface> networkInterfaces;

    private static List<SupportedInterface> interfaces;

    private static boolean ipV6Supported;

    @BeforeClass
    public static void loadInterfaces() throws Exception {
        networkInterfaces = AddressUtils.activeNetworks();

        interfaces = Collections.unmodifiableList(getInterfaces(networkInterfaces));

        Assume.assumeTrue(interfaces.size() > 1);

        ipV6Supported = interfaces.stream()
            .flatMap(i -> i.getAddresses().stream())
            .anyMatch(a -> a instanceof Inet6Address);
    }

    @Test
    public void testPattern() throws Exception {
        assertEquals("any-ip4", createSelector(null).pattern());
        assertEquals("127.0.0.1", createSelector("127.0.0.1").pattern());
    }

    @Test
    public void testThrowsError() throws Exception {
        assertNull(createSelector("net~NO_SUCH_INTERFACE").select());
    }

    @Test
    public void testNonWildcard() throws Exception {
        InetSocketAddress address = new InetSocketAddress("192.192.192.192", 0);

        assertEquals(address.getAddress(), createSelector(address.getHostName()).select());
    }

    @Test
    public void testUnknownHost() throws Exception {
        InetSocketAddress address = new InetSocketAddress("some-unknown-host.unknown", 0);

        AddressPattern selector = createSelector(address.getHostName());

        try {
            selector.select();

            fail("Error was expected.");
        } catch (HekateException e) {
            assertTrue(e.isCausedBy(UnknownHostException.class));
        }
    }

    @Test
    public void testInterfaceNotMatch() throws Exception {
        for (SupportedInterface si : interfaces) {
            String pattern = getAllButOneInterfacePattern(si, interfaces);

            AddressPattern selector = createSelector("!net~" + pattern);

            assertEquals(pattern, selector.opts().interfaceNotMatch());

            InetAddress resolved = selector.select();

            assertEquals(si.getAddresses().get(0), resolved);
        }
    }

    @Test
    public void testInterfaceMatch() throws Exception {
        for (SupportedInterface si : interfaces) {
            String pattern = si.getNetworkInterface().getName();

            AddressPattern selector = createSelector("net~" + pattern);

            assertEquals(pattern, selector.opts().interfaceMatch());

            assertEquals(si.getAddresses().get(0), selector.select());
        }
    }

    @Test
    public void testIpNotMatch() throws Exception {
        for (SupportedInterface si : interfaces) {
            for (InetAddress address : si.getAddresses()) {
                String excludes = getAllButOneAddressPattern(address, interfaces);

                AddressPattern selector = createSelector("!ip~" + excludes);

                assertEquals(excludes, selector.opts().ipNotMatch());

                assertEquals(address, selector.select());
            }
        }
    }

    @Test
    public void testIpMatch() throws Exception {
        for (SupportedInterface si : interfaces) {
            for (InetAddress address : si.getAddresses()) {
                String includes = address.getHostAddress().replaceAll("\\.", "\\\\.");

                AddressPattern selector = createSelector("ip~" + includes);

                assertEquals(includes, selector.opts().ipMatch());

                assertEquals(address, selector.select());
            }
        }
    }

    @Test
    public void testIpVersion() throws Exception {
        AddressPattern selectorIp4 = createSelector("ip4~.*");

        assertTrue(selectorIp4.select() instanceof Inet4Address);

        if (ipV6Supported) {
            AddressPattern selectorIp6 = createSelector("ip6~.*");

            assertSame(AddressPatternOpts.IpVersion.V6, selectorIp6.opts().ipVersion());

            assertTrue(selectorIp6.select() instanceof Inet6Address);
        }
    }

    @Test
    public void testToString() {
        AddressPattern selector = new AddressPattern();

        assertEquals(ToString.format(selector), selector.toString());
    }

    private AddressPattern createSelector(String pattern) {
        // Override getNetworkInterfaces() in order to use cached list and speedup tests.
        return new AddressPattern(pattern) {
            @Override
            List<NetworkInterface> networkInterfaces() throws SocketException {
                return networkInterfaces;
            }
        };
    }

    private String getAllButOneAddressPattern(InetAddress include, List<SupportedInterface> all) {
        return all.stream()
            .flatMap(i -> i.getAddresses().stream())
            .filter(a -> !a.getHostAddress().equals(include.getHostAddress()))
            .map(n -> n.getHostAddress().replaceAll("\\.", "\\\\."))
            .collect(joining("|"));
    }

    private String getAllButOneInterfacePattern(SupportedInterface include, List<SupportedInterface> all) {
        return all.stream()
            .filter(n -> !n.getNetworkInterface().getName().equals(include.getNetworkInterface().getName()))
            .map(n -> n.getNetworkInterface().getName())
            .collect(joining("|"));
    }

    private static List<SupportedInterface> getInterfaces(List<NetworkInterface> interfaces) throws SocketException {
        List<SupportedInterface> supported = new ArrayList<>();

        for (NetworkInterface ni : interfaces) {
            if (ni.isUp() && !ni.isLoopback()) {
                List<InetAddress> addresses = Collections.list(ni.getInetAddresses()).stream()
                    .filter(a -> !a.isLinkLocalAddress())
                    .collect(toList());

                if (!addresses.isEmpty()) {
                    supported.add(new SupportedInterface(ni, addresses));
                }
            }
        }

        return supported;
    }
}
