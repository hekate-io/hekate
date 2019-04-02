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

import io.hekate.util.format.ToString;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for {@link StaticSeedNodeProvider}.
 *
 * @see StaticSeedNodeProvider
 */
public class StaticSeedNodeProviderConfig {
    private List<String> addresses;

    /**
     * Returns the list of configured addresses (see {@link #setAddresses(List)}).
     *
     * @return List of configured addresses.
     */
    public List<String> getAddresses() {
        return addresses;
    }

    /**
     * Sets the list of addresses.
     *
     * <p>
     * Each address can be specified in a form of {@code <host>:<port>} where {@code <host>} can be an IP address or a host name (in case
     * of IPv6 addresses consider enclosing the host string with square brackets) and {@code <port>} is a numeric port number.
     * </p>
     *
     * @param addresses List of addresses.
     */
    public void setAddresses(List<String> addresses) {
        this.addresses = addresses;
    }

    /**
     * Fluent-style version of {@link #setAddresses(List)}.
     *
     * @param address Address.
     *
     * @return This instance.
     */
    public StaticSeedNodeProviderConfig withAddress(String address) {
        if (addresses == null) {
            addresses = new ArrayList<>();
        }

        addresses.add(address);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
