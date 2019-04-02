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

package io.hekate.election;

import io.hekate.HekateTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class CandidateConfigTest extends HekateTestBase {
    private final CandidateConfig cfg = new CandidateConfig();

    @Test
    public void testGroup() {
        assertNull(cfg.getGroup());

        cfg.setGroup("test");

        assertEquals("test", cfg.getGroup());

        assertSame(cfg, cfg.withGroup("test2"));

        assertEquals("test2", cfg.getGroup());

        cfg.setGroup(null);

        assertNull(cfg.getGroup());

        assertEquals("test3", new CandidateConfig("test3").getGroup());
    }

    @Test
    public void testCandidate() {
        Candidate candidate = mock(Candidate.class);

        assertNull(cfg.getCandidate());

        cfg.setCandidate(candidate);

        assertSame(candidate, cfg.getCandidate());

        cfg.setCandidate(null);

        assertNull(cfg.getCandidate());

        assertSame(cfg, cfg.withCandidate(candidate));
    }

    @Test
    public void testToString() {
        assertTrue(cfg.toString(), cfg.toString().startsWith(CandidateConfig.class.getSimpleName()));
    }
}
