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

package io.hekate.cluster.health;

import io.hekate.HekateTestBase;
import org.junit.Test;

import static io.hekate.cluster.health.DefaultFailureDetectorConfig.DEFAULT_FAILURE_DETECTION_QUORUM;
import static io.hekate.cluster.health.DefaultFailureDetectorConfig.DEFAULT_HEARTBEAT_INTERVAL;
import static io.hekate.cluster.health.DefaultFailureDetectorConfig.DEFAULT_HEARTBEAT_LOSS_THRESHOLD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class DefaultFailureDetectorConfigTest extends HekateTestBase {
    private final DefaultFailureDetectorConfig cfg = new DefaultFailureDetectorConfig();

    @Test
    public void testHeartbeatInterval() {
        assertEquals(DEFAULT_HEARTBEAT_INTERVAL, cfg.getHeartbeatInterval());

        cfg.setHeartbeatInterval(DEFAULT_HEARTBEAT_INTERVAL * 2);

        assertEquals(DEFAULT_HEARTBEAT_INTERVAL * 2, cfg.getHeartbeatInterval());

        assertSame(cfg, cfg.withHeartbeatInterval(DEFAULT_HEARTBEAT_INTERVAL * 3));

        assertEquals(DEFAULT_HEARTBEAT_INTERVAL * 3, cfg.getHeartbeatInterval());
    }

    @Test
    public void testHeartbeatLossThreshold() {
        assertEquals(DEFAULT_HEARTBEAT_LOSS_THRESHOLD, cfg.getHeartbeatLossThreshold());

        cfg.setHeartbeatLossThreshold(DEFAULT_HEARTBEAT_LOSS_THRESHOLD * 2);

        assertEquals(DEFAULT_HEARTBEAT_LOSS_THRESHOLD * 2, cfg.getHeartbeatLossThreshold());

        assertSame(cfg, cfg.withHeartbeatLossThreshold(DEFAULT_HEARTBEAT_LOSS_THRESHOLD * 3));

        assertEquals(DEFAULT_HEARTBEAT_LOSS_THRESHOLD * 3, cfg.getHeartbeatLossThreshold());
    }

    @Test
    public void testFailureDetectionQuorum() {
        assertEquals(DEFAULT_FAILURE_DETECTION_QUORUM, cfg.getFailureDetectionQuorum());

        cfg.setFailureDetectionQuorum(DEFAULT_FAILURE_DETECTION_QUORUM * 2);

        assertEquals(DEFAULT_FAILURE_DETECTION_QUORUM * 2, cfg.getFailureDetectionQuorum());

        assertSame(cfg, cfg.withFailureDetectionQuorum(DEFAULT_FAILURE_DETECTION_QUORUM * 3));

        assertEquals(DEFAULT_FAILURE_DETECTION_QUORUM * 3, cfg.getFailureDetectionQuorum());
    }

    @Test
    public void testFailFast() {
        assertTrue(cfg.isFailFast());

        cfg.setFailFast(false);

        assertFalse(cfg.isFailFast());

        assertSame(cfg, cfg.withFailFast(true));

        assertTrue(cfg.isFailFast());
    }
}
