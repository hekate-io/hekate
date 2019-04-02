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

import io.hekate.HekateTestBase;
import java.net.InetAddress;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HostReachabilityDetectorTest extends HekateTestBase {
    @Test
    public void testValid() throws Exception {
        HostReachabilityDetector detector = new HostReachabilityDetector(InetAddress.getLocalHost().getHostAddress(), 2000);

        assertTrue(detector.isValid(newNode()));
    }

    @Test
    public void testInvalid() throws Exception {
        HostReachabilityDetector detector = new HostReachabilityDetector("some.invalid.localhost", 2000);

        assertFalse(detector.isValid(newNode()));
    }
}
