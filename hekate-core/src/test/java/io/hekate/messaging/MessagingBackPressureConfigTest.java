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

package io.hekate.messaging;

import io.hekate.HekateTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class MessagingBackPressureConfigTest extends HekateTestBase {
    private final MessagingBackPressureConfig cfg = new MessagingBackPressureConfig();

    @Test
    public void testInLowWatermark() {
        assertEquals(0, cfg.getInLowWatermark());

        cfg.setInLowWatermark(10001);

        assertEquals(10001, cfg.getInLowWatermark());

        assertSame(cfg, cfg.withInLowWatermark(10002));

        assertEquals(10002, cfg.getInLowWatermark());
    }

    @Test
    public void testInHighWatermark() {
        assertEquals(0, cfg.getInHighWatermark());

        cfg.setInHighWatermark(10001);

        assertEquals(10001, cfg.getInHighWatermark());

        assertSame(cfg, cfg.withInHighWatermark(10002));

        assertEquals(10002, cfg.getInHighWatermark());
    }

    @Test
    public void testOutLowWatermark() {
        assertEquals(0, cfg.getOutLowWatermark());

        cfg.setOutLowWatermark(10001);

        assertEquals(10001, cfg.getOutLowWatermark());

        assertSame(cfg, cfg.withOutLowWatermark(10002));

        assertEquals(10002, cfg.getOutLowWatermark());
    }

    @Test
    public void testOutHighWatermark() {
        assertEquals(0, cfg.getOutHighWatermark());

        cfg.setOutHighWatermark(10001);

        assertEquals(10001, cfg.getOutHighWatermark());

        assertSame(cfg, cfg.withOutHighWatermark(10002));

        assertEquals(10002, cfg.getOutHighWatermark());
    }

    @Test
    public void testOutOverflowPolicy() {
        assertSame(MessagingOverflowPolicy.IGNORE, cfg.getOutOverflowPolicy());

        cfg.setOutOverflowPolicy(MessagingOverflowPolicy.BLOCK);

        assertSame(MessagingOverflowPolicy.BLOCK, cfg.getOutOverflowPolicy());

        assertSame(cfg, cfg.withOutOverflowPolicy(MessagingOverflowPolicy.FAIL));

        assertSame(MessagingOverflowPolicy.FAIL, cfg.getOutOverflowPolicy());
    }

    @Test
    public void testToString() {
        assertTrue(cfg.toString(), cfg.toString().startsWith(MessagingBackPressureConfig.class.getSimpleName()));
    }
}
