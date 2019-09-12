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

import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.Utils;
import java.net.NetworkInterface;

/**
 * Pattern for {@link AddressPattern}.
 */
final class AddressPatternOpts {
    enum IpVersion {
        V4,
        V6
    }

    private static final ConfigCheck CHECK = ConfigCheck.get(AddressPattern.class);

    private static final String ANY = "any";

    private static final String ANY_IP_4 = "any-ip4";

    private static final String ANY_IP_6 = "any-ip6";

    private static final String IP_PREFIX = "ip~";

    private static final String IP_4_PREFIX = "ip4~";

    private static final String IP_6_PREFIX = "ip6~";

    private static final String NET_PREFIX = "net~";

    private static final String NET_4_PREFIX = "net4~";

    private static final String NET_6_PREFIX = "net6~";

    private static final String IP_NOT_PREFIX = "!ip~";

    private static final String IP_4_NOT_PREFIX = "!ip4~";

    private static final String IP_6_NOT_PREFIX = "!ip6~";

    private static final String NET_NOT_PREFIX = "!net~";

    private static final String NET_4_NOT_PREFIX = "!net4~";

    private static final String NET_6_NOT_PREFIX = "!net6~";

    private final String source;

    private final String exactAddress;

    private final IpVersion ipVersion;

    private final String interfaceMatch;

    private final String interfaceNotMatch;

    private final String ipMatch;

    private final String ipNotMatch;

    private AddressPatternOpts(String source, String exactAddress, IpVersion ipVersion, String netNotMatch, String netMatch,
        String ipNotMatch, String ipMatch) {
        this.source = source;
        this.ipVersion = ipVersion;
        this.interfaceNotMatch = netNotMatch;
        this.interfaceMatch = netMatch;
        this.ipNotMatch = ipNotMatch;
        this.ipMatch = ipMatch;
        this.exactAddress = exactAddress;
    }

    public static AddressPatternOpts parse(String pattern) {
        pattern = Utils.nullOrTrim(pattern, ANY_IP_4);

        IpVersion ipVer = null;
        String netMatch = null;
        String netNotMatch = null;
        String ipMatch = null;
        String ipNotMatch = null;
        String exactAddress = null;

        if (!pattern.equals(ANY)) {
            //////////////////////////////////////////////////////////////////
            // 1) Any address.
            //////////////////////////////////////////////////////////////////
            switch (pattern) {
                case ANY_IP_4: {
                    ipVer = IpVersion.V4;

                    break;
                }
                case ANY_IP_6: {
                    ipVer = IpVersion.V6;

                    break;
                }
                default: {
                    //////////////////////////////////////////////////////////////////
                    // 2) IP pattern.
                    //////////////////////////////////////////////////////////////////
                    if (pattern.startsWith(IP_4_PREFIX)) {
                        ipVer = IpVersion.V4;

                        ipMatch = extractMatcher(IP_4_PREFIX, pattern);
                    } else if (pattern.startsWith(IP_4_NOT_PREFIX)) {
                        ipVer = IpVersion.V4;

                        ipNotMatch = extractMatcher(IP_4_NOT_PREFIX, pattern);
                    } else if (pattern.startsWith(IP_6_PREFIX)) {
                        ipVer = IpVersion.V6;

                        ipMatch = extractMatcher(IP_6_PREFIX, pattern);
                    } else if (pattern.startsWith(IP_6_NOT_PREFIX)) {
                        ipVer = IpVersion.V6;

                        ipNotMatch = extractMatcher(IP_6_NOT_PREFIX, pattern);
                    } else if (pattern.startsWith(IP_PREFIX)) {
                        ipMatch = extractMatcher(IP_PREFIX, pattern);
                    } else if (pattern.startsWith(IP_NOT_PREFIX)) {
                        ipNotMatch = extractMatcher(IP_NOT_PREFIX, pattern);
                    } else {
                        //////////////////////////////////////////////////////////////////
                        // 3) Interface name pattern.
                        //////////////////////////////////////////////////////////////////
                        if (pattern.startsWith(NET_4_PREFIX)) {
                            ipVer = IpVersion.V4;

                            netMatch = extractMatcher(NET_4_PREFIX, pattern);
                        } else if (pattern.startsWith(NET_4_NOT_PREFIX)) {
                            ipVer = IpVersion.V4;

                            netNotMatch = extractMatcher(NET_4_NOT_PREFIX, pattern);
                        } else if (pattern.startsWith(NET_6_PREFIX)) {
                            ipVer = IpVersion.V6;

                            netMatch = extractMatcher(NET_6_PREFIX, pattern);
                        } else if (pattern.startsWith(NET_6_NOT_PREFIX)) {
                            ipVer = IpVersion.V6;

                            netNotMatch = extractMatcher(NET_6_NOT_PREFIX, pattern);
                        } else if (pattern.startsWith(NET_PREFIX)) {
                            netMatch = extractMatcher(NET_PREFIX, pattern);
                        } else if (pattern.startsWith(NET_NOT_PREFIX)) {
                            netNotMatch = extractMatcher(NET_NOT_PREFIX, pattern);
                        } else {
                            //////////////////////////////////////////////////////////////////
                            // 4) Address as is.
                            //////////////////////////////////////////////////////////////////
                            exactAddress = pattern;
                        }
                    }

                    break;
                }
            }
        }

        return new AddressPatternOpts(pattern, exactAddress, ipVer, netNotMatch, netMatch, ipNotMatch, ipMatch);
    }

    /**
     * Returns the exact address if it could be directly extracted from the pattern.
     *
     * @return Address.
     */
    public String exactAddress() {
        return exactAddress;
    }

    /**
     * Returns the regular expression for {@link NetworkInterface#getName() interface name} exclusion. Only addresses of network interfaces
     * that do NOT match this pattern will be selected.
     *
     * @return Regular expression.
     */
    public String interfaceNotMatch() {
        return interfaceNotMatch;
    }

    /**
     * Returns the regular expression for {@link NetworkInterface#getName() interface name} inclusion. Only addresses of network interfaces
     * that do match this pattern will be selected.
     *
     * @return Regular expression.
     */
    public String interfaceMatch() {
        return interfaceMatch;
    }

    /**
     * Returns the regular expression for IP addresses exclusion. Only IP addresses that DO NOT match this pattern will be selected.
     *
     * @return Regular expression.
     */
    public String ipNotMatch() {
        return ipNotMatch;
    }

    /**
     * Returns the regular expression for IP addresses inclusion. Only IP addresses that do match this pattern will be selected.
     *
     * @return Regular expression.
     */
    public String ipMatch() {
        return ipMatch;
    }

    /**
     * Returns IP protocol version for IP addresses filtering. Only addresses of the specified protocol version will be selected.
     *
     * @return IP protocol version.
     */
    public IpVersion ipVersion() {
        return ipVersion;
    }

    private static String extractMatcher(String prefix, String pattern) {
        assert prefix != null : "Prefix is null.";
        assert !prefix.isEmpty() : "Prefix is empty.";
        assert pattern != null : "Pattern is null.";
        assert !pattern.isEmpty() : "Pattern is empty.";
        assert pattern.startsWith(prefix) : "Pattern doesn't start with the prefix [prefix=" + prefix + ", pattern=" + pattern + ']';

        String matcher = pattern.substring(prefix.length()).trim();

        CHECK.that(!matcher.isEmpty(), "invalid host pattern [prefix=" + prefix + ", pattern=" + pattern + ']');

        return matcher;
    }

    @Override
    public String toString() {
        return source;
    }
}
