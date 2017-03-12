/*
 * Copyright 2017 The Hekate Project
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
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class DefaultAddressSelectorTest extends HekateTestBase {
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

    private final InetAddress anyAddress = new InetSocketAddress(0).getAddress();

    private final DefaultAddressSelectorConfig selectorCfg = new DefaultAddressSelectorConfig();

    @BeforeClass
    public static void loadInterfaces() throws Exception {
        networkInterfaces = Collections.unmodifiableList(getNetworkInterfaces());

        interfaces = Collections.unmodifiableList(getInterfaces(networkInterfaces));

        ipV6Supported = interfaces.stream()
            .flatMap(i -> i.getAddresses().stream())
            .anyMatch(a -> a instanceof Inet6Address);
    }

    @Test
    public void testThrowsError() throws Exception {
        selectorCfg.setInterfaceMatch("NO_SUCH_INTERFACE");

        assertNull(createSelector().select(anyAddress));
    }

    @Test
    public void testNonWildcard() throws Exception {
        InetSocketAddress address = new InetSocketAddress("192.192.192.192", 0);

        assertEquals(address.getAddress(), createSelector().select(address.getAddress()));
    }

    @Test
    public void testExcludeLoopback() throws Exception {
        assertTrue(selectorCfg.isExcludeLoopback());

        selectorCfg.setExcludeLoopback(false);

        assertFalse(selectorCfg.isExcludeLoopback());

        selectorCfg.setExcludeLoopback(true);

        selectorCfg.setIpMatch(InetAddress.getLoopbackAddress().getHostAddress().replaceAll("\\.", "\\\\."));

        assertNull(createSelector().select(anyAddress));
    }

    @Test
    public void testInterfaceNotMatch() throws Exception {
        selectorCfg.setExcludeLoopback(false);
        selectorCfg.setIpVersion(IpVersion.ANY);

        for (SupportedInterface si : interfaces) {
            String pattern = getAllButOneInterfacePattern(si, interfaces);

            selectorCfg.setInterfaceNotMatch(pattern);

            assertEquals(pattern, selectorCfg.getInterfaceNotMatch());

            InetAddress resolved = createSelector().select(anyAddress);

            assertEquals(si.getAddresses().get(0), resolved);
        }
    }

    @Test
    public void testInterfaceMatch() throws Exception {
        selectorCfg.setExcludeLoopback(false);
        selectorCfg.setIpVersion(IpVersion.ANY);

        for (SupportedInterface si : interfaces) {
            String pattern = si.getNetworkInterface().getName();

            selectorCfg.setInterfaceMatch(pattern);

            assertEquals(pattern, selectorCfg.getInterfaceMatch());

            InetAddress resolved = createSelector().select(anyAddress);

            assertEquals(si.getAddresses().get(0), resolved);
        }
    }

    @Test
    public void testInterfaceMatchAndNotMatch() throws Exception {
        selectorCfg.setExcludeLoopback(false);
        selectorCfg.setIpVersion(IpVersion.ANY);

        for (SupportedInterface si : interfaces) {
            String excludes = getAllButOneInterfacePattern(si, interfaces);

            String includes = si.getNetworkInterface().getName();

            selectorCfg.setInterfaceMatch(includes);
            selectorCfg.setInterfaceNotMatch(excludes);

            assertEquals(includes, selectorCfg.getInterfaceMatch());
            assertEquals(excludes, selectorCfg.getInterfaceNotMatch());

            InetAddress resolved = createSelector().select(anyAddress);

            assertEquals(si.getAddresses().get(0), resolved);
        }
    }

    @Test
    public void testIpNotMatch() throws Exception {
        selectorCfg.setExcludeLoopback(false);
        selectorCfg.setIpVersion(IpVersion.ANY);

        for (SupportedInterface si : interfaces) {
            for (InetAddress address : si.getAddresses()) {
                String excludes = getAllButOneAddressPattern(address, interfaces);

                say(address.getHostAddress() + " ~ " + excludes);

                selectorCfg.setIpNotMatch(excludes);

                assertEquals(excludes, selectorCfg.getIpNotMatch());

                assertEquals(address, createSelector().select(anyAddress));
            }
        }
    }

    @Test
    public void testIpMatch() throws Exception {
        selectorCfg.setExcludeLoopback(false);
        selectorCfg.setIpVersion(IpVersion.ANY);

        for (SupportedInterface si : interfaces) {
            for (InetAddress address : si.getAddresses()) {
                String includes = address.getHostAddress().replaceAll("\\.", "\\\\.");

                selectorCfg.setIpMatch(includes);

                assertEquals(includes, selectorCfg.getIpMatch());

                assertEquals(address, createSelector().select(anyAddress));
            }
        }
    }

    @Test
    public void testIpMatchAndNotMatch() throws Exception {
        selectorCfg.setExcludeLoopback(false);
        selectorCfg.setIpVersion(IpVersion.ANY);

        for (SupportedInterface si : interfaces) {
            for (InetAddress address : si.getAddresses()) {
                String excludes = getAllButOneAddressPattern(address, interfaces);
                String includes = address.getHostAddress().replaceAll("\\.", "\\\\.");

                selectorCfg.setIpNotMatch(excludes);
                selectorCfg.setIpMatch(includes);

                assertEquals(excludes, selectorCfg.getIpNotMatch());
                assertEquals(includes, selectorCfg.getIpMatch());

                assertEquals(address, createSelector().select(anyAddress));
            }
        }
    }

    @Test
    public void testIpVersion() throws Exception {
        selectorCfg.setExcludeLoopback(false);

        selectorCfg.setIpVersion(IpVersion.V4);

        assertSame(IpVersion.V4, selectorCfg.getIpVersion());

        assertTrue(createSelector().select(anyAddress) instanceof Inet4Address);

        if (ipV6Supported) {
            selectorCfg.setIpVersion(IpVersion.V6);

            assertSame(IpVersion.V6, selectorCfg.getIpVersion());

            assertTrue(createSelector().select(anyAddress) instanceof Inet6Address);
        }
    }

    private DefaultAddressSelector createSelector() {
        // Override getNetworkInterfaces() to use cached list and speedup tests.
        return new DefaultAddressSelector(selectorCfg) {
            @Override
            List<NetworkInterface> getNetworkInterfaces() throws SocketException {
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
            if (ni.isUp()) {
                List<InetAddress> addresses = Collections.list(ni.getInetAddresses()).stream()
                    .filter(a -> !a.isLinkLocalAddress())
                    .collect(toList());

                if (!addresses.isEmpty()) {
                    supported.add(new SupportedInterface(ni, addresses));
                }
            }
        }

        assertTrue("Expected more than two network interfaces:" + supported, supported.size() >= 2);

        return supported;
    }
}
