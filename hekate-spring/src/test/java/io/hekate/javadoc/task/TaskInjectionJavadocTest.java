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

package io.hekate.javadoc.task;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.task.TaskService;
import io.hekate.task.TaskServiceFactory;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:javadoc/task/task-injection.xml")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class TaskInjectionJavadocTest extends HekateTestBase {
    @Autowired
    private SeedNodeProvider seedNodeProvider;

    @Test
    public void example() throws Exception {
        Hekate hekate = new HekateBootstrap()
            .withService(new ClusterServiceFactory()
                .withSeedNodeProvider(seedNodeProvider)
            )
            .withService(new TaskServiceFactory())
            .join();

        try {
            // Start:execute
            TaskService tasks = hekate.get(TaskService.class);

            CompletableFuture<?> future = tasks.forRemotes().run(new ExampleTask());

            future.get();
            // End:execute
        } finally {
            hekate.leave();
        }
    }

    @Override
    protected void assertAllThreadsStopped() throws InterruptedException {
        // Do not check threads since Spring context gets terminated after all tests have been run.
    }
}
