/*
 * Copyright 2021 The Hekate Project
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

import io.hekate.HekateTestBase;
import io.hekate.core.report.DefaultConfigReporter;
import io.hekate.test.HekateTestError;
import io.hekate.util.format.ToString;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.junit.Test;

import static io.hekate.core.internal.util.Utils.NL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HostReachabilityDetectorTest extends HekateTestBase {
    private final String localhost;

    public HostReachabilityDetectorTest() throws UnknownHostException {
        localhost = localhost().getHostAddress();
    }

    @Test
    public void testValid() throws Exception {
        HostReachabilityDetector detector = new HostReachabilityDetector(localhost, 2000) {
            @Override
            boolean isReachable(InetAddress address, int timeout) throws IOException {
                return true;
            }
        };

        assertTrue(detector.isValid(newNode()));
    }

    @Test
    public void testNotReachable() throws Exception {
        HostReachabilityDetector detector = new HostReachabilityDetector(localhost, 2123) {
            @Override
            boolean isReachable(InetAddress address, int timeout) throws IOException {
                return false;
            }
        };

        assertFalse(detector.isValid(newNode()));
    }

    @Test
    public void testReachabilityCheckFailure() throws Exception {
        HostReachabilityDetector detector = new HostReachabilityDetector(localhost, 2123) {
            @Override
            boolean isReachable(InetAddress address, int timeout) throws IOException {
                throw new IOException(HekateTestError.MESSAGE);
            }
        };

        assertFalse(detector.isValid(newNode()));
    }

    @Test
    public void testConfigReport() throws Exception {
        HostReachabilityDetector detector = new HostReachabilityDetector(localhost);

        assertEquals(
            NL
                + "  host-reachability:" + NL
                + "    host: " + detector.host() + NL
                + "    timeout: " + detector.timeout() + NL,
            DefaultConfigReporter.report(detector)
        );
    }

    @Test
    public void testToString() {
        HostReachabilityDetector detector = new HostReachabilityDetector(localhost);

        assertEquals(ToString.format(detector), detector.toString());
    }
}
