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
import org.junit.Test;

import static io.hekate.network.address.AddressPatternOpts.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class AddressPatternOptsTest extends HekateTestBase {
    @Test
    public void testAny() {
        AddressPatternOpts opts = parse("any");

        assertNull(opts.ipVersion());
        assertNull(opts.exactAddress());
        assertAllPatternsAreNull(opts);
    }

    @Test
    public void testAnyIp4() {
        AddressPatternOpts opts = parse("any-ip4");

        assertSame(AddressPatternOpts.IpVersion.V4, opts.ipVersion());
        assertNull(opts.exactAddress());
        assertAllPatternsAreNull(opts);

        opts = parse(" any-ip4 ");

        assertSame(AddressPatternOpts.IpVersion.V4, opts.ipVersion());
        assertNull(opts.exactAddress());
        assertAllPatternsAreNull(opts);
    }

    @Test
    public void testAnyIp6() {
        AddressPatternOpts opts = parse("any-ip6");

        assertSame(AddressPatternOpts.IpVersion.V6, opts.ipVersion());
        assertNull(opts.exactAddress());
        assertAllPatternsAreNull(opts);

        opts = parse(" any-ip6 ");

        assertSame(AddressPatternOpts.IpVersion.V6, opts.ipVersion());
        assertNull(opts.exactAddress());
        assertAllPatternsAreNull(opts);
    }

    @Test
    public void testIpMatchPrefix() {
        AddressPatternOpts opts = parse("ip~.*");

        assertEquals(".*", opts.ipMatch());
        assertNull(opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.ipNotMatch());
        assertInterfacePattersAreNull(opts);

        opts = parse(" ip~ .*");

        assertEquals(".*", opts.ipMatch());
        assertNull(opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.ipNotMatch());
        assertInterfacePattersAreNull(opts);
    }

    @Test
    public void testIpNotMatchPrefix() {
        AddressPatternOpts opts = parse("!ip~.*");

        assertEquals(".*", opts.ipNotMatch());
        assertNull(opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.ipMatch());
        assertInterfacePattersAreNull(opts);

        opts = parse(" !ip~ .* ");

        assertEquals(".*", opts.ipNotMatch());
        assertNull(opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.ipMatch());
        assertInterfacePattersAreNull(opts);
    }

    @Test
    public void testIp4MatchPrefix() {
        AddressPatternOpts opts = parse("ip4~.*");

        assertEquals(".*", opts.ipMatch());
        assertSame(AddressPatternOpts.IpVersion.V4, opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.ipNotMatch());
        assertInterfacePattersAreNull(opts);

        opts = parse(" ip4~ .* ");

        assertEquals(".*", opts.ipMatch());
        assertSame(AddressPatternOpts.IpVersion.V4, opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.ipNotMatch());
        assertInterfacePattersAreNull(opts);
    }

    @Test
    public void testIp4NotMatchPrefix() {
        AddressPatternOpts opts = parse("!ip4~.*");

        assertEquals(".*", opts.ipNotMatch());
        assertSame(AddressPatternOpts.IpVersion.V4, opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.ipMatch());
        assertInterfacePattersAreNull(opts);

        opts = parse(" !ip4~ .* ");

        assertEquals(".*", opts.ipNotMatch());
        assertSame(AddressPatternOpts.IpVersion.V4, opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.ipMatch());
        assertInterfacePattersAreNull(opts);
    }

    @Test
    public void testIp6MatchPrefix() {
        AddressPatternOpts opts = parse("ip6~.*");

        assertEquals(".*", opts.ipMatch());
        assertSame(AddressPatternOpts.IpVersion.V6, opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.ipNotMatch());
        assertInterfacePattersAreNull(opts);

        opts = parse(" ip6~ .* ");

        assertEquals(".*", opts.ipMatch());
        assertSame(AddressPatternOpts.IpVersion.V6, opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.ipNotMatch());
        assertInterfacePattersAreNull(opts);
    }

    @Test
    public void testIp6NotMatchPrefix() {
        AddressPatternOpts opts = parse("!ip6~.*");

        assertEquals(".*", opts.ipNotMatch());
        assertSame(AddressPatternOpts.IpVersion.V6, opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.ipMatch());
        assertInterfacePattersAreNull(opts);

        opts = parse(" !ip6~ .* ");

        assertEquals(".*", opts.ipNotMatch());
        assertSame(AddressPatternOpts.IpVersion.V6, opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.ipMatch());
        assertInterfacePattersAreNull(opts);
    }

    @Test
    public void testInterfaceMatchPrefix() {
        AddressPatternOpts opts = parse("net~.*");

        assertEquals(".*", opts.interfaceMatch());
        assertNull(opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.interfaceNotMatch());
        assertIpPatternsAreNull(opts);

        opts = parse(" net~ .* ");

        assertEquals(".*", opts.interfaceMatch());
        assertNull(opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.interfaceNotMatch());
        assertIpPatternsAreNull(opts);
    }

    @Test
    public void testInterface4MatchPrefix() {
        AddressPatternOpts opts = parse("net4~.*");

        assertEquals(".*", opts.interfaceMatch());
        assertSame(AddressPatternOpts.IpVersion.V4, opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.interfaceNotMatch());
        assertIpPatternsAreNull(opts);

        opts = parse(" net4~ .* ");

        assertEquals(".*", opts.interfaceMatch());
        assertSame(AddressPatternOpts.IpVersion.V4, opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.interfaceNotMatch());
        assertIpPatternsAreNull(opts);
    }

    @Test
    public void testInterface6MatchPrefix() {
        AddressPatternOpts opts = parse("net6~.*");

        assertEquals(".*", opts.interfaceMatch());
        assertSame(AddressPatternOpts.IpVersion.V6, opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.interfaceNotMatch());
        assertIpPatternsAreNull(opts);

        opts = parse(" net6~ .* ");

        assertEquals(".*", opts.interfaceMatch());
        assertSame(AddressPatternOpts.IpVersion.V6, opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.interfaceNotMatch());
        assertIpPatternsAreNull(opts);
    }

    @Test
    public void testInterfaceNotMatchPrefix() {
        AddressPatternOpts opts = parse("!net~.*");

        assertEquals(".*", opts.interfaceNotMatch());
        assertNull(opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.interfaceMatch());
        assertIpPatternsAreNull(opts);

        opts = parse(" !net~ .* ");

        assertEquals(".*", opts.interfaceNotMatch());
        assertNull(opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.interfaceMatch());
        assertIpPatternsAreNull(opts);
    }

    @Test
    public void testInterface4NotMatchPrefix() {
        AddressPatternOpts opts = parse("!net4~.*");

        assertEquals(".*", opts.interfaceNotMatch());
        assertSame(AddressPatternOpts.IpVersion.V4, opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.interfaceMatch());
        assertIpPatternsAreNull(opts);

        opts = parse(" !net4~ .* ");

        assertEquals(".*", opts.interfaceNotMatch());
        assertSame(AddressPatternOpts.IpVersion.V4, opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.interfaceMatch());
        assertIpPatternsAreNull(opts);
    }

    @Test
    public void testInterface6NotMatchPrefix() {
        AddressPatternOpts opts = parse("!net6~.*");

        assertEquals(".*", opts.interfaceNotMatch());
        assertSame(AddressPatternOpts.IpVersion.V6, opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.interfaceMatch());
        assertIpPatternsAreNull(opts);

        opts = parse(" !net6~ .* ");

        assertEquals(".*", opts.interfaceNotMatch());
        assertSame(AddressPatternOpts.IpVersion.V6, opts.ipVersion());
        assertNull(opts.exactAddress());
        assertNull(opts.interfaceMatch());
        assertIpPatternsAreNull(opts);
    }

    @Test
    public void testExactAddress() {
        AddressPatternOpts opts = parse("127.0.0.1");

        assertEquals("127.0.0.1", opts.exactAddress());
        assertNull(opts.ipVersion());
        assertInterfacePattersAreNull(opts);
        assertIpPatternsAreNull(opts);

        opts = parse(" 127.0.0.1 ");

        assertEquals("127.0.0.1", opts.exactAddress());
        assertNull(opts.ipVersion());
        assertInterfacePattersAreNull(opts);
        assertIpPatternsAreNull(opts);
    }

    @Test
    public void testEmpty() {
        AddressPatternOpts opts = parse("");

        assertSame(AddressPatternOpts.IpVersion.V4, opts.ipVersion());
        assertNull(opts.exactAddress());

        assertAllPatternsAreNull(opts);
    }

    @Test
    public void testNull() {
        AddressPatternOpts opts = parse(null);

        assertSame(AddressPatternOpts.IpVersion.V4, opts.ipVersion());
        assertNull(opts.exactAddress());

        assertAllPatternsAreNull(opts);
    }

    @Test
    public void testToString() {
        assertEquals("any-ip4", parse(null).toString());
        assertEquals("any", parse("any").toString());
        assertEquals("ip~.*", parse("ip~.*").toString());
    }

    private void assertAllPatternsAreNull(AddressPatternOpts opts) {
        assertInterfacePattersAreNull(opts);
        assertIpPatternsAreNull(opts);
    }

    private void assertInterfacePattersAreNull(AddressPatternOpts opts) {
        assertNull(opts.interfaceMatch());
        assertNull(opts.interfaceNotMatch());
    }

    private void assertIpPatternsAreNull(AddressPatternOpts opts) {
        assertNull(opts.ipMatch());
        assertNull(opts.ipNotMatch());
    }
}
