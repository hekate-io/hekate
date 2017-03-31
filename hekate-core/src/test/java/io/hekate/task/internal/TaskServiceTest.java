/*
 * Copyright 2017 The Hekate Project
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

package io.hekate.task.internal;

import io.hekate.HekateTestContext;
import io.hekate.core.HekateTestInstance;
import io.hekate.failover.FailoverContext;
import io.hekate.task.TaskService;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TaskServiceTest extends TaskServiceTestBase {
    public TaskServiceTest(HekateTestContext params) {
        super(params);
    }

    @Test
    public void testFiltering() throws Exception {
        List<HekateTestInstance> nodes = createAndJoin(2);

        HekateTestInstance first = nodes.get(0);
        HekateTestInstance second = nodes.get(1);

        TaskService tasks = first.get(TaskService.class);

        assertTrue(tasks.toString(), tasks.toString().startsWith(TaskService.class.getSimpleName()));

        assertEquals(second.getNode(), get(tasks.forRemotes().call(() -> {
            say(first.getNode());

            return first.getNode();
        })));

        assertEquals(second.getNode(), get(tasks.forRemotes().withFailover(FailoverContext::fail).call(() -> {
            say(first.getNode());

            return first.getNode();
        })));

        assertEquals(second.getNode(), get(tasks.forNode(second.getNode()).call(() -> {
            say(second.getNode());

            return second.getNode();
        })));

        assertEquals(second.getNode(), get(tasks.forNode(second.getNode().getId()).call(() -> {
            say(second.getNode());

            return second.getNode();
        })));

        assertEquals(second.getNode(), get(tasks.filter(n -> n.equals(second.getNode())).call(() -> {
            say(second.getNode());

            return second.getNode();
        })));

        TaskService filtered = tasks.forRemotes();

        assertTrue(filtered.toString(), filtered.toString().startsWith(TaskService.class.getSimpleName()));
        assertTrue(filtered.hasFilter());
    }
}
