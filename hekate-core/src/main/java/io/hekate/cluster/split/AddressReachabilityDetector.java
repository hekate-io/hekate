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

package io.hekate.cluster.split;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.core.internal.util.AddressUtils;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.util.format.ToString;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Socket address connectivity-based detector.
 *
 * <p>
 * This implementation of {@link SplitBrainDetector} tries to establish a socket connection to check if some pre-configured
 * address is reachable. Assumption is that if local node can connect to that address then it is not cut off the network and can
 * communicate with other nodes.
 * </p>
 *
 * <p>
 * Note that it is possible to combine multiple detectors with the help of {@link SplitBrainDetectorGroup}.
 * </p>
 *
 * @see ClusterServiceFactory#setSplitBrainDetector(SplitBrainDetector)
 */
public class AddressReachabilityDetector implements SplitBrainDetector {
    /**
     * Default value (={@value}) in milliseconds for the {@code timeout} parameter of {@link #AddressReachabilityDetector(String, int)}.
     */
    public static final int DEFAULT_TIMEOUT = 3000;

    private static final Logger log = LoggerFactory.getLogger(AddressReachabilityDetector.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final InetSocketAddress address;

    private final int timeout;

    /**
     * Constructs new instance with the default timeout value (={@value DEFAULT_TIMEOUT}).
     *
     * @param address Address in a form of {@code <host>:<port>} (f.e. {@code 192.168.39.41:10012}).
     */
    public AddressReachabilityDetector(String address) {
        this(address, DEFAULT_TIMEOUT);
    }

    /**
     * Constructs new instance.
     *
     * @param address Address in a form of {@code <host>:<port>} (f.e. {@code 192.168.39.41:10012}).
     * @param timeout Connect timeout in milliseconds (see {@link Socket#connect(SocketAddress, int)}).
     */
    public AddressReachabilityDetector(String address, int timeout) {
        this(AddressUtils.parseUnresolved(address, ConfigCheck.get(AddressReachabilityDetector.class)), timeout);
    }

    /**
     * Constructs new instance.
     *
     * @param address Address.
     * @param timeout Connect timeout in milliseconds (see {@link Socket#connect(SocketAddress, int)}).
     */
    public AddressReachabilityDetector(InetSocketAddress address, int timeout) {
        ConfigCheck check = ConfigCheck.get(AddressReachabilityDetector.class);

        check.notNull(address, "address");
        check.positive(timeout, "timeout");

        this.address = address;
        this.timeout = timeout;
    }

    @Override
    public boolean isValid(ClusterNode localNode) {
        try (Socket socket = new Socket()) {
            InetAddress resolvedHost = InetAddress.getByName(address.getHostString());

            InetSocketAddress resolvedAddress = new InetSocketAddress(resolvedHost, address.getPort());

            socket.connect(resolvedAddress, timeout);

            if (DEBUG) {
                log.debug("Address reachability check success [host={}, timeout={}]", address, timeout);
            }

            return true;
        } catch (IOException e) {
            if (log.isWarnEnabled()) {
                log.warn("Address reachability check failed [host={}, timeout={}, error={}]", address, timeout, e.toString());
            }
        }

        return false;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
