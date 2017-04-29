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
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class DefaultAddressSelectorConfigTest extends HekateTestBase {
    private final DefaultAddressSelectorConfig cfg = new DefaultAddressSelectorConfig();

    @Test
    public void testIpVersion() {
        assertSame(IpVersion.V4, cfg.getIpVersion());
        assertSame(IpVersion.V4, createSelector().ipVersion());

        cfg.setIpVersion(IpVersion.ANY);

        assertSame(IpVersion.ANY, cfg.getIpVersion());
        assertSame(IpVersion.ANY, createSelector().ipVersion());

        assertSame(cfg, cfg.withIpVersion(IpVersion.V6));

        assertSame(IpVersion.V6, cfg.getIpVersion());
        assertSame(IpVersion.V6, createSelector().ipVersion());
    }

    @Test
    public void testExcludeLoopback() {
        assertTrue(cfg.isExcludeLoopback());
        assertTrue(createSelector().excludeLoopback());

        cfg.setExcludeLoopback(false);

        assertFalse(cfg.isExcludeLoopback());
        assertFalse(createSelector().excludeLoopback());

        assertSame(cfg, cfg.withExcludeLoopback(true));
        assertTrue(createSelector().excludeLoopback());
    }

    @Test
    public void testInterfaceNotMatch() {
        assertNull(cfg.getInterfaceNotMatch());
        assertNull(createSelector().interfaceNotMatch());

        cfg.setInterfaceNotMatch("test");

        assertEquals("test", cfg.getInterfaceNotMatch());
        assertEquals("test", createSelector().interfaceNotMatch());

        assertSame(cfg, cfg.withInterfaceNotMatch("test2"));

        assertEquals("test2", cfg.getInterfaceNotMatch());
        assertEquals("test2", createSelector().interfaceNotMatch());
    }

    @Test
    public void testInterfaceMatch() {
        assertNull(cfg.getInterfaceMatch());
        assertNull(createSelector().interfaceMatch());

        cfg.setInterfaceMatch("test");

        assertEquals("test", cfg.getInterfaceMatch());
        assertEquals("test", createSelector().interfaceMatch());

        assertSame(cfg, cfg.withInterfaceMatch("test2"));

        assertEquals("test2", cfg.getInterfaceMatch());
        assertEquals("test2", createSelector().interfaceMatch());
    }

    @Test
    public void testIpNotMatch() {
        assertNull(cfg.getIpNotMatch());
        assertNull(createSelector().ipNotMatch());

        cfg.setIpNotMatch("test");

        assertEquals("test", cfg.getIpNotMatch());
        assertEquals("test", createSelector().ipNotMatch());

        assertSame(cfg, cfg.withIpNotMatch("test2"));

        assertEquals("test2", cfg.getIpNotMatch());
        assertEquals("test2", createSelector().ipNotMatch());
    }

    @Test
    public void testIpMatch() {
        assertNull(cfg.getIpMatch());
        assertNull(createSelector().ipMatch());

        cfg.setIpMatch("test");

        assertEquals("test", cfg.getIpMatch());
        assertEquals("test", createSelector().ipMatch());

        assertSame(cfg, cfg.withIpMatch("test2"));

        assertEquals("test2", cfg.getIpMatch());
        assertEquals("test2", createSelector().ipMatch());
    }

    private DefaultAddressSelector createSelector() {
        return new DefaultAddressSelector(cfg);
    }
}
