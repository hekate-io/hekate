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
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.util.format.ToString;
import java.io.IOException;
import java.net.InetAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Host reachability-based detector.
 *
 * <p>
 * This implementation of {@link SplitBrainDetector} utilizes {@link InetAddress#isReachable(int)} method to check if some pre-configured
 * host address is reachable. Assumption is that if local node can reach that host then it is not cut off the network and can communicate
 * with other nodes.
 * </p>
 *
 * <p>
 * Note that it is possible to combine multiple detectors with the help of {@link SplitBrainDetectorGroup}.
 * </p>
 *
 * @see ClusterServiceFactory#setSplitBrainDetector(SplitBrainDetector)
 */
public class HostReachabilityDetector implements SplitBrainDetector {
    /** Default value (={@value}) in milliseconds for the {@code timeout} parameter of {@link #HostReachabilityDetector(String, int)}. */
    public static final int DEFAULT_TIMEOUT = 3000;

    private static final Logger log = LoggerFactory.getLogger(HostReachabilityDetector.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final String host;

    private final int timeout;

    /**
     * Constructs new instance with the default timeout value (={@value DEFAULT_TIMEOUT}).
     *
     * @param host Host to be checked for reachability.
     */
    public HostReachabilityDetector(String host) {
        this(host, DEFAULT_TIMEOUT);
    }

    /**
     * Constructs new instance.
     *
     * @param host Host to be checked for reachability.
     * @param timeout Timeout in milliseconds for host reachability checking (see {@link InetAddress#isReachable(int)}).
     */
    public HostReachabilityDetector(String host, int timeout) {
        ConfigCheck check = ConfigCheck.get(getClass());

        check.notEmpty(host, "host");
        check.positive(timeout, "timeout");

        this.host = host.trim();
        this.timeout = timeout;
    }

    @Override
    public boolean isValid(ClusterNode localNode) {
        try {
            InetAddress address = InetAddress.getByName(host);

            boolean reachable = address.isReachable(timeout);

            if (reachable) {
                if (DEBUG) {
                    log.debug("Address reachability check success [host={}, timeout={}]", host, timeout);
                }
            } else {
                if (log.isWarnEnabled()) {
                    log.warn("Address reachability check failed [host={}, timeout={}]", host, timeout);
                }
            }

            return reachable;
        } catch (IOException e) {
            if (log.isWarnEnabled()) {
                log.warn("Address reachability check failed with an error [host={}, timeout={}, cause={}]", host, timeout, e.toString());
            }
        }

        return false;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
