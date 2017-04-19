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

import io.hekate.HekateNodeTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.task.MultiNodeResult;
import io.hekate.task.TaskFuture;
import io.hekate.task.TaskService;
import io.hekate.task.TaskServiceFactory;
import java.util.Collection;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class TaskServiceJavadocTest extends HekateNodeTestBase {
    @Test
    public void exampleAccessService() throws Exception {
        // Start:configure
        // Prepare task service factory.
        TaskServiceFactory factory = new TaskServiceFactory()
            // Configure some settings (optional).
            .withWorkerThreads(16);

        // Start node.
        Hekate hekate = new HekateBootstrap()
            .withService(factory)
            .join();
        // End:configure

        // Start:access
        TaskService tasks = hekate.tasks();
        // End:access

        assertNotNull(tasks);

        runExample(hekate);
        broadcastExample(hekate);
        callExample(hekate);
        aggregateExample(hekate);
        applyExample(hekate);
        filterNodesExample(hekate);

        hekate.leave();
    }

    private void runExample(Hekate hekate) throws Exception {
        // Start:run_task
        // Submit runnable task for asynchronous execution.
        TaskFuture<?> run = hekate.tasks().run(() ->
            System.out.println("Running on node " + hekate.getLocalNode())
        );

        // Await for task completion.
        run.get();
        // End:run_task
    }

    private void broadcastExample(Hekate hekate) throws Exception {
        // Start:broadcast_task
        // Submit runnable task for asynchronous execution on all cluster nodes.
        TaskFuture<MultiNodeResult<Void>> broadcast = hekate.tasks().broadcast(() ->
            System.out.println("Running on node " + hekate.getLocalNode())
        );

        // Await for task completion on all nodes.
        System.out.println("Broadcast results: " + broadcast.get());
        // End:broadcast_task
    }

    private void callExample(Hekate hekate) throws Exception {
        // Start:call_task
        // Submit callable task and obtain a task execution future.
        TaskFuture<ClusterNode> call = hekate.tasks().call(() -> {
            System.out.println("Running on node " + hekate.getLocalNode());

            return hekate.getLocalNode();
        });

        // Await for task execution and print out its result.
        System.out.println("Called on node " + call.get());
        // End:call_task
    }

    private void aggregateExample(Hekate hekate) throws Exception {
        // Start:aggregate_task
        // Submit runnable task for asynchronous execution on all cluster nodes.
        TaskFuture<MultiNodeResult<ClusterNode>> broadcast = hekate.tasks().aggregate(() -> {
                System.out.println("Running on node " + hekate.getLocalNode());

                return hekate.getLocalNode();
            }
        );

        // Await for task completion on all nodes.
        System.out.println("Aggregated on nodes: " + broadcast.get().results());
        // End:aggregate_task
    }

    private void applyExample(Hekate hekate) throws Exception {
        // Start:apply
        String text = "some long text";

        // Split text into words and count the length of each word.
        // Words counting is distributed among the cluster nodes and is processed in parallel.
        TaskFuture<Collection<Integer>> apply = hekate.tasks().applyAll(text.split(" "), word -> {
            System.out.println("Processing word: " + word);

            return word.length();
        });

        System.out.println("Total: " + apply.get().stream().mapToInt(Integer::intValue).sum());
        // End:apply
    }

    private void filterNodesExample(Hekate hekate) throws Exception {
        // Start:filter_nodes
        // Submit task to all remote nodes.
        hekate.tasks().forRemotes().broadcast(() ->
            System.out.println("Running on node " + hekate.getLocalNode())
        );

        // Submit task to all nodes with 'example_role'.
        hekate.tasks().forRole("example_role").broadcast(() ->
            System.out.println("Running on node " + hekate.getLocalNode())
        );

        // Multiple filters can be stacked: submit task to all remote nodes with 'example_role'.
        hekate.tasks().forRemotes().forRole("example_role").broadcast(() ->
            System.out.println("Running on node " + hekate.getLocalNode())
        );

        // Custom filter: submit task to all nodes that have more than 2 CPUs.
        hekate.tasks().filter(node -> node.getSysInfo().getCpus() > 2).broadcast(() ->
            System.out.println("Running on node " + hekate.getLocalNode())
        );
        // End:filter_nodes
    }
}
