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

package io.hekate.rpc;

import io.hekate.HekateTestBase;
import io.hekate.util.format.ToString;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class RpcServerConfigTest extends HekateTestBase {
    public interface TestRpc {
        // No-op.
    }

    private final RpcServerConfig cfg = new RpcServerConfig();

    @Test
    public void testHandler() {
        assertNull(cfg.getHandler());

        TestRpc handler = mock(TestRpc.class);

        cfg.setHandler(handler);

        assertSame(handler, cfg.getHandler());

        cfg.setHandler(null);

        assertNull(cfg.getHandler());

        assertSame(cfg, cfg.withHandler(handler));
        assertSame(handler, cfg.getHandler());
    }

    @Test
    public void testTags() {
        assertNull(cfg.getTags());

        Set<String> tags = new HashSet<>(Arrays.asList("one", "two"));

        cfg.setTags(tags);

        assertEquals(tags, cfg.getTags());

        cfg.setTags(null);

        assertNull(cfg.getTags());

        assertSame(cfg, cfg.withTag("three"));
        assertEquals(Collections.singleton("three"), cfg.getTags());
    }

    @Test
    public void testToString() {
        assertEquals(ToString.format(cfg), cfg.toString());
    }
}
