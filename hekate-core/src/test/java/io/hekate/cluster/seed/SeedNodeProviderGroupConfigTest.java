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

import io.hekate.HekateTestBase;
import io.hekate.util.format.ToString;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class SeedNodeProviderGroupConfigTest extends HekateTestBase {
    private final SeedNodeProviderGroupConfig cfg = new SeedNodeProviderGroupConfig();

    @Test
    public void testPolicy() {
        assertSame(SeedNodeProviderGroupPolicy.FAIL_ON_FIRST_ERROR, cfg.getPolicy());

        cfg.setPolicy(SeedNodeProviderGroupPolicy.IGNORE_PARTIAL_ERRORS);

        assertSame(SeedNodeProviderGroupPolicy.IGNORE_PARTIAL_ERRORS, cfg.getPolicy());

        assertSame(cfg, cfg.withPolicy(SeedNodeProviderGroupPolicy.FAIL_ON_FIRST_ERROR));

        assertSame(SeedNodeProviderGroupPolicy.FAIL_ON_FIRST_ERROR, cfg.getPolicy());

        cfg.setPolicy(null);

        assertNull(cfg.getPolicy());
    }

    @Test
    public void testProviders() throws Exception {
        assertNull(cfg.getProviders());

        assertFalse(cfg.hasProviders());

        SeedNodeProvider p1 = mock(SeedNodeProvider.class);
        SeedNodeProvider p2 = mock(SeedNodeProvider.class);
        SeedNodeProvider p3 = mock(SeedNodeProvider.class);

        cfg.setProviders(new ArrayList<>(Arrays.asList(p1, p2)));

        assertTrue(cfg.hasProviders());
        assertEquals(Arrays.asList(p1, p2), cfg.getProviders());

        assertSame(cfg, cfg.withProvider(p3));

        assertEquals(Arrays.asList(p1, p2, p3), cfg.getProviders());
        assertTrue(cfg.hasProviders());

        cfg.setProviders(null);

        assertNull(cfg.getProviders());
        assertFalse(cfg.hasProviders());

        assertSame(cfg, cfg.withProvider(p1));

        assertEquals(singletonList(p1), cfg.getProviders());
        assertTrue(cfg.hasProviders());

        cfg.setProviders(null);

        assertNull(cfg.getProviders());
        assertFalse(cfg.hasProviders());

        assertSame(cfg, cfg.withProviders(Arrays.asList(p1, p2, p3)));

        assertEquals(Arrays.asList(p1, p2, p3), cfg.getProviders());
        assertTrue(cfg.hasProviders());
    }

    @Test
    public void testToString() {
        assertEquals(ToString.format(cfg), cfg.toString());
    }
}
