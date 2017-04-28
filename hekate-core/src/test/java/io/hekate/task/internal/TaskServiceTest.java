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
import io.hekate.core.internal.HekateTestNode;
import io.hekate.core.service.Service;
import io.hekate.failover.FailoverPolicy;
import io.hekate.task.TaskService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TaskServiceTest extends TaskServiceTestBase {
    private interface DummyService extends Service {
        // No-op.
    }

    private HekateTestNode local;

    private HekateTestNode first;

    private HekateTestNode second;

    private TaskService tasks;

    public TaskServiceTest(HekateTestContext params) {
        super(params);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        local = createTaskNode(c -> c.withNodeRole("all"));

        first = createTaskNode(c -> c.withNodeRole("all").withNodeRole("role1")
            .withNodeProperty("prop", "val1")
            .withNodeProperty("unique-prop", "val")
            .withService(() -> {
                return new DummyService() {
                    // No-op.
                };
            })
        );

        second = createTaskNode(c -> c.withNodeRole("all").withNodeRole("role2")
            .withNodeProperty("prop", "val2")
        );

        get(CompletableFuture.allOf(local.joinAsync(), first.joinAsync(), second.joinAsync()));

        awaitForTopology(local, first, second);

        tasks = local.tasks();
    }

    @Test
    public void testToString() throws Exception {
        assertTrue(tasks.toString(), tasks.toString().startsWith(TaskService.class.getSimpleName()));

        TaskService filtered = tasks.forRemotes();

        assertTrue(filtered.toString(), filtered.toString().startsWith(TaskService.class.getSimpleName()));
    }

    @Test
    public void testFilter() throws Exception {
        assertEquals(first.getLocalNode(), get(tasks.filter(n -> n.equals(first.getLocalNode())).call(local::getLocalNode)));
        assertEquals(second.getLocalNode(), get(tasks.filter(n -> n.equals(second.getLocalNode())).call(local::getLocalNode)));
    }

    @Test
    public void testFilterAll() throws Exception {
        assertEquals(first.getLocalNode(), get(tasks.filterAll(n -> singletonList(first.getLocalNode())).call(local::getLocalNode)));
        assertEquals(second.getLocalNode(), get(tasks.filterAll(n -> singletonList(second.getLocalNode())).call(local::getLocalNode)));
    }

    @Test
    public void testForNode() throws Exception {
        assertEquals(first.getLocalNode(), get(tasks.forNode(first.getLocalNode()).call(local::getLocalNode)));
        assertEquals(second.getLocalNode(), get(tasks.forNode(second.getLocalNode()).call(local::getLocalNode)));
    }

    @Test
    public void testForNodeId() throws Exception {
        assertEquals(first.getLocalNode(), get(tasks.forNode(first.getLocalNode().getId()).call(local::getLocalNode)));
        assertEquals(second.getLocalNode(), get(tasks.forNode(second.getLocalNode().getId()).call(local::getLocalNode)));
    }

    @Test
    public void testForProperty() throws Exception {
        assertThat(get(tasks.forProperty("unique-prop").aggregate(local::getLocalNode)).results(),
            allOf(
                hasItems(first.getLocalNode()),
                not(hasItems(local.getLocalNode(), second.getLocalNode()))
            )
        );

        assertThat(get(tasks.forProperty("prop").aggregate(local::getLocalNode)).results(),
            allOf(
                hasItems(first.getLocalNode(), second.getLocalNode()),
                not(hasItems(local.getLocalNode()))
            )
        );
    }

    @Test
    public void testForPropertyValue() throws Exception {
        assertThat(get(tasks.forProperty("prop", "val1").aggregate(local::getLocalNode)).results(),
            allOf(
                hasItems(first.getLocalNode()),
                not(hasItems(local.getLocalNode(), second.getLocalNode()))
            )
        );

        assertThat(get(tasks.forProperty("prop", "val2").aggregate(local::getLocalNode)).results(),
            allOf(
                hasItems(second.getLocalNode()),
                not(hasItems(local.getLocalNode(), first.getLocalNode()))
            )
        );
    }

    @Test
    public void testForService() throws Exception {
        assertThat(get(tasks.forRemotes().forService(TaskService.class).aggregate(local::getLocalNode)).results(),
            allOf(
                hasItems(first.getLocalNode(), second.getLocalNode()),
                not(hasItems(local.getLocalNode()))
            )
        );

        assertThat(get(tasks.forService(DummyService.class).aggregate(local::getLocalNode)).results(),
            allOf(
                hasItems(first.getLocalNode()),
                not(hasItems(local.getLocalNode(), second.getLocalNode()))
            )
        );
    }

    @Test
    public void testForRemotes() throws Exception {
        assertThat(get(tasks.forRemotes().aggregate(local::getLocalNode)).results(),
            allOf(
                hasItems(first.getLocalNode(), second.getLocalNode()),
                not(hasItems(local.getLocalNode()))
            )
        );
    }

    @Test
    public void testCombo() throws Exception {
        assertThat(get(tasks.forRemotes().forProperty("prop").aggregate(local::getLocalNode)).results(),
            allOf(
                hasItems(first.getLocalNode(), second.getLocalNode()),
                not(hasItems(local.getLocalNode()))
            )
        );

        assertThat(get(tasks.forRole("all").forProperty("prop").aggregate(local::getLocalNode)).results(),
            allOf(
                hasItems(first.getLocalNode(), second.getLocalNode()),
                not(hasItems(local.getLocalNode()))
            )
        );

        assertThat(get(tasks.forRemotes().forRole("all").aggregate(local::getLocalNode)).results(),
            allOf(
                hasItems(first.getLocalNode(), second.getLocalNode()),
                not(hasItems(local.getLocalNode()))
            )
        );
    }

    @Test
    public void testGetAffinity() throws Exception {
        assertNull(tasks.getAffinity());
        assertNull(tasks.forRemotes().getAffinity());

        assertEquals("affinity1", tasks.withAffinity("affinity1").getAffinity());
        assertNull(tasks.getAffinity());
        assertNull(tasks.forRemotes().getAffinity());

        assertEquals("affinity2", tasks.forRemotes().withAffinity("affinity2").getAffinity());
        assertNull(tasks.getAffinity());
        assertNull(tasks.forRemotes().getAffinity());

        assertEquals("affinity3", tasks.forRemotes().withAffinity("affinity3").forRemotes().getAffinity());
        assertNull(tasks.getAffinity());
        assertNull(tasks.forRemotes().getAffinity());
    }

    @Test
    public void testGetFailover() throws Exception {
        assertNull(tasks.getFailover());
        assertNull(tasks.forRemotes().getFailover());

        FailoverPolicy p1 = context -> null;
        FailoverPolicy p2 = context -> null;
        FailoverPolicy p3 = context -> null;

        assertSame(p1, tasks.withFailover(p1).getFailover());
        assertNull(tasks.getFailover());
        assertNull(tasks.forRemotes().getFailover());

        assertSame(p2, tasks.forRemotes().withFailover(p2).getFailover());
        assertNull(tasks.getFailover());
        assertNull(tasks.forRemotes().getFailover());

        assertSame(p3, tasks.forRemotes().withFailover(p3).forRemotes().getFailover());
        assertNull(tasks.getFailover());
        assertNull(tasks.forRemotes().getFailover());
    }

    @Test
    public void testGetTimeout() throws Exception {
        assertEquals(0, tasks.getTimeout());

        assertEquals(1000, tasks.withTimeout(1, TimeUnit.SECONDS).getTimeout());
        assertEquals(0, tasks.getTimeout());

        assertEquals(2000, tasks.forRemotes().withTimeout(2, TimeUnit.SECONDS).getTimeout());
        assertEquals(0, tasks.getTimeout());

        assertEquals(3000, tasks.forRemotes().withTimeout(3, TimeUnit.SECONDS).forRemotes().getTimeout());
        assertEquals(0, tasks.getTimeout());
    }
}
