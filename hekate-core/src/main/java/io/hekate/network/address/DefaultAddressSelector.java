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

import io.hekate.core.Hekate;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.network.NetworkServiceFactory;
import io.hekate.util.format.ToString;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link AddressSelector} interface.
 *
 * <p>
 * This implementation of {@link AddressSelector} interface scans through all available network interfaces of the local host and uses
 * filtering rules to decide on which address should be selected. Note that filtering rules will be applied only if {@link Hekate} instance
 * is configured with a wildcard {@link NetworkServiceFactory#setHost(String) host address} (or if {@link
 * NetworkServiceFactory#setHost(String) host address} is not specified). If non wildcard {@link NetworkServiceFactory#setHost(String) host
 * address} is configured then it will be used as is by this implementation.
 * </p>
 *
 * <p>
 * The following filtering rules can be applied:
 * </p>
 * <ul>
 *
 * <li>
 * {@link DefaultAddressSelectorConfig#setIpVersion(IpVersion) IP protocol version} - only addresses of the specified IP protocol version
 * will be selected
 * </li>
 *
 * <li>
 * {@link DefaultAddressSelectorConfig#setExcludeLoopback(boolean) Loopback address exclusion} - controls whether the loopback address can
 * be selected
 * </li>
 *
 * <li>
 * Network interface {@link DefaultAddressSelectorConfig#setInterfaceMatch(String) inclusion} / {@link
 * DefaultAddressSelectorConfig#setInterfaceNotMatch(String) exclusion} - only network interfaces with {@link NetworkInterface#getName()
 * names} that match the specified regular expression will be analyzed
 * </li>
 *
 * <li>
 * IP address {@link DefaultAddressSelectorConfig#setIpMatch(String) inclusion} / {@link DefaultAddressSelectorConfig#setIpNotMatch(String)
 * exclusion} - only IP addresses that match the specified regular expression will be selected
 * </li>
 * </ul>
 *
 * <p>
 * If multiple addresses match the specified rules then the first one will be selected (order is not guaranteed).
 * </p>
 *
 * @see AddressSelector
 * @see NetworkServiceFactory#setAddressSelector(AddressSelector)
 * @see DefaultAddressSelectorConfig
 */
public class DefaultAddressSelector implements AddressSelector {
    private static final Logger log = LoggerFactory.getLogger(DefaultAddressSelector.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final IpVersion ipVersion;

    private final boolean excludeLoopback;

    private final String interfaceNotMatch;

    private final String interfaceMatch;

    private final String ipNotMatch;

    private final String ipMatch;

    /**
     * Constructs new instance with all configuration options set to their defaults (see {@link DefaultAddressSelectorConfig}).
     */
    public DefaultAddressSelector() {
        this(new DefaultAddressSelectorConfig());
    }

    /**
     * Constructs new instance with the specified configuration.
     *
     * @param cfg Configuration.
     */
    public DefaultAddressSelector(DefaultAddressSelectorConfig cfg) {
        ArgAssert.check(cfg != null, "Configuration is null.");

        ipVersion = cfg.getIpVersion();
        excludeLoopback = cfg.isExcludeLoopback();
        interfaceNotMatch = cfg.getInterfaceNotMatch();
        interfaceMatch = cfg.getInterfaceMatch();
        ipNotMatch = cfg.getIpNotMatch();
        ipMatch = cfg.getIpMatch();
    }

    @Override
    public InetAddress select(InetAddress bindAddress) throws HekateException {
        if (!bindAddress.isAnyLocalAddress()) {
            if (DEBUG) {
                log.debug("Skipped non-wildcard address resolution [address={}]", bindAddress);
            }

            return bindAddress;
        }

        try {
            if (DEBUG) {
                log.debug("Trying to resolve address [{}]", ToString.formatProperties(this));
            }

            Pattern niIncludes = getRegex(interfaceMatch);
            Pattern niExcludes = getRegex(interfaceNotMatch);
            Pattern addrIncludes = getRegex(ipMatch);
            Pattern addrExcludes = getRegex(ipNotMatch);

            List<NetworkInterface> nis = getNetworkInterfaces();

            for (NetworkInterface ni : nis) {
                if (!ni.isUp()) {
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
            throw new HekateException("Failed to resolve node address [address=" + bindAddress + ']', e);
        }

        return null;
    }

    /**
     * See {@link DefaultAddressSelectorConfig#setIpVersion(IpVersion)}.
     *
     * @return IP protocol version.
     */
    public IpVersion getIpVersion() {
        return ipVersion;
    }

    /**
     * See {@link DefaultAddressSelectorConfig#setExcludeLoopback(boolean)}.
     *
     * @return {@code true} if the loopback address must be excluded.
     */
    public boolean isExcludeLoopback() {
        return excludeLoopback;
    }

    /**
     * See {@link DefaultAddressSelectorConfig#setInterfaceNotMatch(String)}.
     *
     * @return Regular expression.
     */
    public String getInterfaceNotMatch() {
        return interfaceNotMatch;
    }

    /**
     * See {@link DefaultAddressSelectorConfig#setInterfaceMatch(String)}.
     *
     * @return Regular expression.
     */
    public String getInterfaceMatch() {
        return interfaceMatch;
    }

    /**
     * See {@link DefaultAddressSelectorConfig#setIpNotMatch(String)}.
     *
     * @return Regular expression.
     */
    public String getIpNotMatch() {
        return ipNotMatch;
    }

    /**
     * See {@link DefaultAddressSelectorConfig#setIpMatch(String)}.
     *
     * @return Regular expression.
     */
    public String getIpMatch() {
        return ipMatch;
    }

    // Package level for testing purposes.
    List<NetworkInterface> getNetworkInterfaces() throws SocketException {
        return Collections.list(NetworkInterface.getNetworkInterfaces());
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
        } else if (excludeLoopback && address.isLoopbackAddress()) {
            if (DEBUG) {
                log.debug("Skipped loopback address [interface={}, address={}]", niName, address);
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
        switch (ipVersion) {
            case V4: {
                return addr instanceof Inet4Address;
            }
            case V6: {
                return addr instanceof Inet6Address;
            }
            case ANY: {
                return true;
            }
            default: {
                throw new IllegalStateException("Unexpected IP version type: " + ipVersion);
            }
        }
    }

    private boolean matches(boolean shouldMatch, String str, Pattern pattern) {
        return pattern == null || pattern.matcher(str).matches() == shouldMatch;
    }

    private Pattern getRegex(String pattern) {
        if (pattern != null && !pattern.trim().isEmpty()) {
            return Pattern.compile(pattern.trim());
        }

        return null;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
