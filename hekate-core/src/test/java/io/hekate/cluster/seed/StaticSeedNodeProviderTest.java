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
import io.hekate.core.HekateConfigurationException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class StaticSeedNodeProviderTest extends HekateTestBase {
    public static final String ADDRESS_V6 = "fe80::f4d2:5b65:8569:2945%4";

    public static final String ADDRESS_V4 = "127.0.0.1";

    @Test
    public void testValidIAddressV4() throws Exception {
        StaticSeedNodeProvider provider = get(ADDRESS_V4 + ":10001");

        List<InetSocketAddress> nodes = provider.findSeedNodes("test");

        assertFalse(nodes.isEmpty());
        assertEquals(new InetSocketAddress(ADDRESS_V4, 10001), nodes.get(0));
    }

    @Test
    public void testValidAddressV6() throws Exception {
        StaticSeedNodeProvider provider = get("[" + ADDRESS_V6 + "]:10001");

        List<InetSocketAddress> nodes = provider.findSeedNodes("test");

        assertFalse(nodes.isEmpty());
        assertEquals(new InetSocketAddress(ADDRESS_V6, 10001), nodes.get(0));
    }

    @Test
    public void testEmptyHostV4() throws Exception {
        checkInvalid(":10001");
    }

    @Test
    public void testEmptyPortV4() throws Exception {
        checkInvalid(ADDRESS_V4 + ":");
    }

    @Test
    public void testEmptyHostV6() throws Exception {
        checkInvalid("[]:10001");
    }

    @Test
    public void testEmptyPortV6() throws Exception {
        checkInvalid("[" + ADDRESS_V6 + "]:");
    }

    @Test
    public void testPartialV6() throws Exception {
        checkInvalid("[" + ADDRESS_V6);
    }

    @Test
    public void testNoSeparator() throws Exception {
        checkInvalid("10001");
    }

    @Test
    public void testEmptyHostAndPort() throws Exception {
        checkInvalid(":");
    }

    @Test
    public void testMultipleSeparators() throws Exception {
        checkInvalid(":::");
    }

    private StaticSeedNodeProvider get(String address) throws UnknownHostException {
        StaticSeedNodeProviderConfig cfg = new StaticSeedNodeProviderConfig();

        cfg.withAddress(address);

        return new StaticSeedNodeProvider(cfg);
    }

    private void checkInvalid(String address) throws UnknownHostException {
        try {
            get(address);

            fail("Failure was expected for address '" + address + "'.");
        } catch (HekateConfigurationException e) {
            say("Expected: " + e);
        }
    }
}
