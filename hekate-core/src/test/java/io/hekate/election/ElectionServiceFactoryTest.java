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
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ElectionServiceFactoryTest extends HekateTestBase {
    private final ElectionServiceFactory factory = new ElectionServiceFactory();

    @Test
    public void testCandidates() {
        CandidateConfig cfg1 = new CandidateConfig();
        CandidateConfig cfg2 = new CandidateConfig();

        assertNull(factory.getCandidates());

        factory.setCandidates(Arrays.asList(cfg1, cfg2));

        assertEquals(2, factory.getCandidates().size());
        assertTrue(factory.getCandidates().contains(cfg1));
        assertTrue(factory.getCandidates().contains(cfg2));

        factory.setCandidates(null);

        assertNull(factory.getCandidates());

        assertSame(factory, factory.withCandidate(cfg1));

        assertEquals(1, factory.getCandidates().size());
        assertTrue(factory.getCandidates().contains(cfg1));

        factory.setCandidates(null);

        CandidateConfig election = factory.withCandidate("test");

        assertNotNull(election);
        assertEquals("test", election.getGroup());
        assertTrue(factory.getCandidates().contains(election));
    }

    @Test
    public void testConfigProviders() {
        CandidateConfigProvider p1 = () -> null;
        CandidateConfigProvider p2 = () -> null;

        assertNull(factory.getConfigProviders());

        factory.setConfigProviders(Arrays.asList(p1, p2));

        assertEquals(2, factory.getConfigProviders().size());
        assertTrue(factory.getConfigProviders().contains(p1));
        assertTrue(factory.getConfigProviders().contains(p2));

        factory.setConfigProviders(null);

        assertNull(factory.getConfigProviders());

        assertSame(factory, factory.withConfigProvider(p1));

        assertEquals(1, factory.getConfigProviders().size());
        assertTrue(factory.getConfigProviders().contains(p1));
    }

    @Test
    public void testToString() {
        assertTrue(factory.toString(), factory.toString().startsWith(ElectionServiceFactory.class.getSimpleName()));
    }
}
