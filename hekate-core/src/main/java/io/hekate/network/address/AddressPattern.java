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

import io.hekate.core.HekateException;
import io.hekate.core.internal.util.AddressUtils;
import io.hekate.network.NetworkServiceFactory;
import io.hekate.util.format.ToString;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.List;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pattern-based implementation of {@link AddressSelector} interface.
 *
 * <p>
 * This implementation of {@link AddressSelector} interface scans through all available network interfaces of the local host and uses
 * the pattern matching logic in order to decide which address should be selected.
 * </p>
 *
 * <p>
 * The following patterns can be specified:
 * </p>
 *
 * <ul>
 * <li><b>any</b> - any non-loopback address</li>
 * <li><b>any-ip4</b> - any IPv4 non-loopback address</li>
 * <li><b>any-ip6</b> - any IPv6 non-loopback address</li>
 *
 * <li><b>ip~<i>regex</i></b> - any IP address that matches the specified regular expression</li>
 * <li><b>ip4~<i>regex</i></b> - any IPv4 address that matches the specified regular expression</li>
 * <li><b>ip6~<i>regex</i></b> - any IPv6 address that matches the specified regular expression</li>
 *
 * <li><b>!ip~<i>regex</i></b> - any IP address that does NOT match the specified regular expression</li>
 * <li><b>!ip4~<i>regex</i></b> - any IPv4 address that does NOT match the specified regular expression</li>
 * <li><b>!ip6~<i>regex</i></b> - any IPv6 address that does NOT match the specified regular expression</li>
 *
 * <li><b>net~<i>regex</i></b> - any IP address of a network interface who's {@link NetworkInterface#getName() name} matches the specified
 * regular expression</li>
 * <li><b>net4~<i>regex</i></b> - IPv4 address of a network interface who's {@link NetworkInterface#getName() name} matches the specified
 * regular expression</li>
 * <li><b>net6~<i>regex</i></b> - IPv6 address of a network interface who's {@link NetworkInterface#getName() name} matches the specified
 * regular expression</li>
 *
 * <li><b>!net~<i>regex</i></b> - any IP address of a network interface who's {@link NetworkInterface#getName() name} does NOT match the
 * specified regular expression</li>
 * <li><b>!net4~<i>regex</i></b> - IPv4 address of a network interface who's {@link NetworkInterface#getName() name} does NOT match the
 * specified regular expression</li>
 * <li><b>!net6~<i>regex</i></b> - IPv6 address of a network interface who's {@link NetworkInterface#getName() name} does NOT match the
 * specified regular expression</li>
 *
 * <li>...all other values will be treated as a directly specified address (see {@link InetAddress#getByName(String)})</li>
 * </ul>
 *
 * <p>
 * If multiple addresses match the specified pattern then the first one will be selected (order is not guaranteed).
 * </p>
 *
 * @see AddressSelector
 * @see NetworkServiceFactory#setHostSelector(AddressSelector)
 */
public class AddressPattern implements AddressSelector {
    private static final Logger log = LoggerFactory.getLogger(AddressPattern.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final AddressPatternOpts opts;

    /**
     * Constructs a new instance with {@code 'any'} pattern.
     */
    public AddressPattern() {
        this(null);
    }

    /**
     * Constructs a new instance.
     *
     * @param pattern Pattern (see the descriptions of this class for the list of supported patterns).
     */
    public AddressPattern(String pattern) {
        this.opts = AddressPatternOpts.parse(pattern);
    }

    /**
     * Returns the host pattern as string.
     *
     * @return Host pattern.
     */
    public String pattern() {
        return opts.toString();
    }

    @Override
    public InetAddress select() throws HekateException {
        try {
            if (opts.exactAddress() != null) {
                if (DEBUG) {
                    log.debug("Using the exact address [{}]", opts);
                }

                return InetAddress.getByName(opts.exactAddress());
            }

            if (DEBUG) {
                log.debug("Trying to resolve address [{}]", opts);
            }

            Pattern niIncludes = regex(opts.interfaceMatch());
            Pattern niExcludes = regex(opts.interfaceNotMatch());
            Pattern addrIncludes = regex(opts.ipMatch());
            Pattern addrExcludes = regex(opts.ipNotMatch());

            List<NetworkInterface> nis = networkInterfaces();

            for (NetworkInterface ni : nis) {
                if (!ni.isUp() || ni.isLoopback()) {
                    continue;
                }

                String niName = ni.getName();

                if (matches(true, niName, niIncludes) && matches(false, niName, niExcludes)) {
                    Enumeration<InetAddress> addresses = ni.getInetAddresses();

                    while (addresses.hasMoreElements()) {
                        InetAddress address = addresses.nextElement();

                        if (DEBUG) {
                            log.debug("Trying address {}", address);
                        }

                        if (checkAddress(addrIncludes, addrExcludes, niName, address)) {
                            if (DEBUG) {
                                log.debug("Resolved address [interface={}, address={}]", niName, address);
                            }

                            return address;
                        }
                    }
                } else {
                    if (DEBUG) {
                        log.debug("Skipped network interface that doesn't match name pattern [name={}]", niName);
                    }
                }
            }
        } catch (IOException e) {
            throw new HekateException("Failed to resolve node address [" + opts + ']', e);
        }

        return null;
    }

    // Package level for testing purposes.
    AddressPatternOpts opts() {
        return opts;
    }

    // Package level for testing purposes.
    List<NetworkInterface> networkInterfaces() throws SocketException {
        return AddressUtils.activeNetworks();
    }

    private boolean checkAddress(Pattern addrIncludes, Pattern addrExcludes, String niName, InetAddress address) {
        if (address.isLinkLocalAddress()) {
            if (DEBUG) {
                log.debug("Skipped link local address [interface={}, address={}]", niName, address);
            }
        } else if (!ipVersionMatch(address)) {
            if (DEBUG) {
                log.debug("Skipped address that doesn't match IP protocol version [interface={}, address={}]", niName, address);
            }
        } else {
            String host = address.getHostAddress();

            if (matches(true, host, addrIncludes) && matches(false, host, addrExcludes)) {
                return true;
            } else {
                if (DEBUG) {
                    log.debug("Skipped address that doesn't match host pattern [interface={}, address={}]", niName, host);
                }
            }
        }

        return false;
    }

    private boolean ipVersionMatch(InetAddress addr) {
        if (opts.ipVersion() != null) {
            switch (opts.ipVersion()) {
                case V4: {
                    return addr instanceof Inet4Address;
                }
                case V6: {
                    return addr instanceof Inet6Address;
                }
                default: {
                    throw new IllegalStateException("Unexpected IP version type: " + opts.ipVersion());
                }
            }
        }

        return true;
    }

    private boolean matches(boolean shouldMatch, String str, Pattern pattern) {
        return pattern == null || pattern.matcher(str).matches() == shouldMatch;
    }

    private Pattern regex(String pattern) {
        if (pattern != null) {
            pattern = pattern.trim();

            if (!pattern.isEmpty()) {
                return Pattern.compile(pattern);
            }
        }

        return null;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
