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

import io.hekate.util.format.ToString;
import java.net.NetworkInterface;

/**
 * Configuration for {@link DefaultAddressSelector}.
 *
 * @see DefaultAddressSelector#DefaultAddressSelector(DefaultAddressSelectorConfig)
 */
public class DefaultAddressSelectorConfig {
    private IpVersion ipVersion = IpVersion.V4;

    private boolean excludeLoopback = true;

    private String interfaceNotMatch;

    private String interfaceMatch;

    private String ipNotMatch;

    private String ipMatch;

    /**
     * Returns the regular expression for {@link NetworkInterface#getName() interface name} exclusion (see {@link
     * #setInterfaceNotMatch(String)}).
     *
     * @return Regular expression.
     */
    public String getInterfaceNotMatch() {
        return interfaceNotMatch;
    }

    /**
     * Sets the regular expression for {@link NetworkInterface#getName() interface name} exclusion. Only addresses of network interfaces
     * that do NOT match this pattern will be selected.
     *
     * @param interfaceNotMatch Regular expression.
     */
    public void setInterfaceNotMatch(String interfaceNotMatch) {
        this.interfaceNotMatch = interfaceNotMatch;
    }

    /**
     * Fluent-style version of {@link #setInterfaceNotMatch(String)}.
     *
     * @param interfaceNotMatch Regular expression.
     *
     * @return This instance.
     */
    public DefaultAddressSelectorConfig withInterfaceNotMatch(String interfaceNotMatch) {
        setInterfaceNotMatch(interfaceNotMatch);

        return this;
    }

    /**
     * Returns the regular expression for {@link NetworkInterface#getName() interface name} inclusion (see {@link
     * #setInterfaceMatch(String)}).
     *
     * @return Regular expression.
     */
    public String getInterfaceMatch() {
        return interfaceMatch;
    }

    /**
     * Sets the regular expression for {@link NetworkInterface#getName() interface name} inclusion. Only addresses of network interfaces
     * that do match this pattern will be selected.
     *
     * @param interfaceMatch Regular expression.
     */
    public void setInterfaceMatch(String interfaceMatch) {
        this.interfaceMatch = interfaceMatch;
    }

    /**
     * Fluent-style version of {@link #setInterfaceMatch(String)}.
     *
     * @param interfaceMatch Regular expression.
     *
     * @return This instance.
     */
    public DefaultAddressSelectorConfig withInterfaceMatch(String interfaceMatch) {
        setInterfaceMatch(interfaceMatch);

        return this;
    }

    /**
     * Returns the regular expression for IP addresses exclusion (see {@link #setInterfaceNotMatch(String)}).
     *
     * @return Regular expression.
     */
    public String getIpNotMatch() {
        return ipNotMatch;
    }

    /**
     * Sets the regular expression for IP addresses exclusion. Only IP addresses that DO NOT match this pattern will be selected.
     *
     * @param ipNotMatch Regular expression.
     */
    public void setIpNotMatch(String ipNotMatch) {
        this.ipNotMatch = ipNotMatch;
    }

    /**
     * Fluent-style version of {@link #setIpNotMatch(String)}.
     *
     * @param ipNotMatch Regular expression.
     *
     * @return This instance.
     */
    public DefaultAddressSelectorConfig withIpNotMatch(String ipNotMatch) {
        setIpNotMatch(ipNotMatch);

        return this;
    }

    /**
     * Returns the regular expression for IP addresses inclusion (see {@link #setInterfaceMatch(String)}).
     *
     * @return Regular expression.
     */
    public String getIpMatch() {
        return ipMatch;
    }

    /**
     * Sets the regular expression for IP addresses inclusion. Only IP addresses that do match this pattern will be selected.
     *
     * @param ipMatch Regular expression.
     */
    public void setIpMatch(String ipMatch) {
        this.ipMatch = ipMatch;
    }

    /**
     * Fluent-style version of {@link #setIpMatch(String)}.
     *
     * @param ipMatch Regular expression.
     *
     * @return This instance.
     */
    public DefaultAddressSelectorConfig withIpMatch(String ipMatch) {
        setIpMatch(ipMatch);

        return this;
    }

    /**
     * Returns IP protocol version for IP addresses filtering (see {@link #setIpVersion(IpVersion)}).
     *
     * @return IP protocol version.
     */
    public IpVersion getIpVersion() {
        return ipVersion;
    }

    /**
     * Sets IP protocol version for IP addresses filtering. Only addresses of the specified protocol version will be selected.
     *
     * <p>
     * Default value of this parameter is {@link IpVersion#V4}.
     * </p>
     *
     * @param ipVersion IP protocol version.
     */
    public void setIpVersion(IpVersion ipVersion) {
        this.ipVersion = ipVersion == null ? IpVersion.V4 : ipVersion;
    }

    /**
     * Fluent-style version of {@link #setIpVersion(IpVersion)}.
     *
     * @param ipVersion IP protocol version.
     *
     * @return This instance.
     */
    public DefaultAddressSelectorConfig withIpVersion(IpVersion ipVersion) {
        setIpVersion(ipVersion);

        return this;
    }

    /**
     * Returns {@code true} if the loopback address must be excluded from selection (see {@link #setExcludeLoopback(boolean)}).
     *
     * @return {@code true} if the loopback address must be excluded.
     */
    public boolean isExcludeLoopback() {
        return excludeLoopback;
    }

    /**
     * Sets the flag indicating whether the loopback address must be excluded from selection.
     *
     * <p>
     * Default value of this property is {@code true}.
     * </p>
     *
     * @param excludeLoopback {@code true} if the loopback address must be excluded.
     */
    public void setExcludeLoopback(boolean excludeLoopback) {
        this.excludeLoopback = excludeLoopback;
    }

    /**
     * Fluent-style version of {@link #setExcludeLoopback(boolean)}.
     *
     * @param excludeLoopback {@code true} if the loopback address must be excluded.
     *
     * @return This instance.
     */
    public DefaultAddressSelectorConfig withExcludeLoopback(boolean excludeLoopback) {
        setExcludeLoopback(excludeLoopback);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
