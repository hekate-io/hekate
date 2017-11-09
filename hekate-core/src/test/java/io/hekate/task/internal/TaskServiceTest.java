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

    public TaskServiceTest(MultiCodecTestContext params) {
        super(params);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        local = createTaskNode(c -> c.withRole("all"));

        first = createTaskNode(c -> c.withRole("all").withRole("role1")
            .withProperty("prop", "val1")
            .withProperty("unique-prop", "val")
            .withService(() -> {
                return new DummyService() {
                    // No-op.
                };
            })
        );

        second = createTaskNode(c -> c.withRole("all").withRole("role2")
            .withProperty("prop", "val2")
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
        assertEquals(first.localNode(), get(tasks.filter(n -> n.equals(first.localNode())).call(local::localNode)));
        assertEquals(second.localNode(), get(tasks.filter(n -> n.equals(second.localNode())).call(local::localNode)));
    }

    @Test
    public void testFilterAll() throws Exception {
        assertEquals(first.localNode(), get(tasks.filterAll(n -> singletonList(first.localNode())).call(local::localNode)));
        assertEquals(second.localNode(), get(tasks.filterAll(n -> singletonList(second.localNode())).call(local::localNode)));
    }

    @Test
    public void testForNode() throws Exception {
        assertEquals(first.localNode(), get(tasks.forNode(first.localNode()).call(local::localNode)));
        assertEquals(second.localNode(), get(tasks.forNode(second.localNode()).call(local::localNode)));
    }

    @Test
    public void testForNodeId() throws Exception {
        assertEquals(first.localNode(), get(tasks.forNode(first.localNode().id()).call(local::localNode)));
        assertEquals(second.localNode(), get(tasks.forNode(second.localNode().id()).call(local::localNode)));
    }

    @Test
    public void testForProperty() throws Exception {
        assertThat(get(tasks.forProperty("unique-prop").aggregate(local::localNode)).results(),
            allOf(
                hasItems(first.localNode()),
                not(hasItems(local.localNode(), second.localNode()))
            )
        );

        assertThat(get(tasks.forProperty("prop").aggregate(local::localNode)).results(),
            allOf(
                hasItems(first.localNode(), second.localNode()),
                not(hasItems(local.localNode()))
            )
        );
    }

    @Test
    public void testForPropertyValue() throws Exception {
        assertThat(get(tasks.forProperty("prop", "val1").aggregate(local::localNode)).results(),
            allOf(
                hasItems(first.localNode()),
                not(hasItems(local.localNode(), second.localNode()))
            )
        );

        assertThat(get(tasks.forProperty("prop", "val2").aggregate(local::localNode)).results(),
            allOf(
                hasItems(second.localNode()),
                not(hasItems(local.localNode(), first.localNode()))
            )
        );
    }

    @Test
    public void testForService() throws Exception {
        assertThat(get(tasks.forRemotes().forService(TaskService.class).aggregate(local::localNode)).results(),
            allOf(
                hasItems(first.localNode(), second.localNode()),
                not(hasItems(local.localNode()))
            )
        );

        assertThat(get(tasks.forService(DummyService.class).aggregate(local::localNode)).results(),
            allOf(
                hasItems(first.localNode()),
                not(hasItems(local.localNode(), second.localNode()))
            )
        );
    }

    @Test
    public void testForRemotes() throws Exception {
        assertThat(get(tasks.forRemotes().aggregate(local::localNode)).results(),
            allOf(
                hasItems(first.localNode(), second.localNode()),
                not(hasItems(local.localNode()))
            )
        );
    }

    @Test
    public void testCombo() throws Exception {
        assertThat(get(tasks.forRemotes().forProperty("prop").aggregate(local::localNode)).results(),
            allOf(
                hasItems(first.localNode(), second.localNode()),
                not(hasItems(local.localNode()))
            )
        );

        assertThat(get(tasks.forRole("all").forProperty("prop").aggregate(local::localNode)).results(),
            allOf(
                hasItems(first.localNode(), second.localNode()),
                not(hasItems(local.localNode()))
            )
        );

        assertThat(get(tasks.forRemotes().forRole("all").aggregate(local::localNode)).results(),
            allOf(
                hasItems(first.localNode(), second.localNode()),
                not(hasItems(local.localNode()))
            )
        );
    }

    @Test
    public void testGetAffinity() throws Exception {
        assertNull(tasks.affinity());
        assertNull(tasks.forRemotes().affinity());

        assertEquals("affinity1", tasks.withAffinity("affinity1").affinity());
        assertNull(tasks.affinity());
        assertNull(tasks.forRemotes().affinity());

        assertEquals("affinity2", tasks.forRemotes().withAffinity("affinity2").affinity());
        assertNull(tasks.affinity());
        assertNull(tasks.forRemotes().affinity());

        assertEquals("affinity3", tasks.forRemotes().withAffinity("affinity3").forRemotes().affinity());
        assertNull(tasks.affinity());
        assertNull(tasks.forRemotes().affinity());
    }

    @Test
    public void testGetFailover() throws Exception {
        assertNull(tasks.failover());
        assertNull(tasks.forRemotes().failover());

        FailoverPolicy p1 = context -> null;
        FailoverPolicy p2 = context -> null;
        FailoverPolicy p3 = context -> null;

        assertSame(p1, tasks.withFailover(p1).failover());
        assertNull(tasks.failover());
        assertNull(tasks.forRemotes().failover());

        assertSame(p2, tasks.forRemotes().withFailover(p2).failover());
        assertNull(tasks.failover());
        assertNull(tasks.forRemotes().failover());

        assertSame(p3, tasks.forRemotes().withFailover(p3).forRemotes().failover());
        assertNull(tasks.failover());
        assertNull(tasks.forRemotes().failover());
    }

    @Test
    public void testGetTimeout() throws Exception {
        assertEquals(0, tasks.timeout());

        assertEquals(1000, tasks.withTimeout(1, TimeUnit.SECONDS).timeout());
        assertEquals(0, tasks.timeout());

        assertEquals(2000, tasks.forRemotes().withTimeout(2, TimeUnit.SECONDS).timeout());
        assertEquals(0, tasks.timeout());

        assertEquals(3000, tasks.forRemotes().withTimeout(3, TimeUnit.SECONDS).forRemotes().timeout());
        assertEquals(0, tasks.timeout());
    }
}
