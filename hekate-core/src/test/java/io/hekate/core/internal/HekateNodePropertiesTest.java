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

package io.hekate.core.internal;

import io.hekate.HekateNodeTestBase;
import java.util.Collections;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HekateNodePropertiesTest extends HekateNodeTestBase {
    @Test
    public void testRolesAndProperties() throws Exception {
        HekateTestNode node = createNode(boot -> boot
            .withRole("role1")
            .withRole("role2")
            .withProperty("prop1", "val1")
            .withProperty("prop2", "val2")
            .withPropertyProvider(() ->
                Collections.singletonMap("prop3", "val3")
            )
        ).join();

        assertTrue(node.localNode().hasRole("role1"));
        assertTrue(node.localNode().hasRole("role2"));
        assertEquals("val1", node.localNode().property("prop1"));
        assertEquals("val2", node.localNode().property("prop2"));
        assertEquals("val3", node.localNode().property("prop3"));
    }
}
