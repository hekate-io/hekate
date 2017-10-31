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

package io.hekate.spring.boot.task;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.cluster.seed.SeedNodeProviderMock;
import io.hekate.coordinate.CoordinationService;
import io.hekate.core.Hekate;
import io.hekate.core.inject.HekateInject;
import io.hekate.core.inject.InjectionService;
import io.hekate.election.ElectionService;
import io.hekate.lock.LockService;
import io.hekate.messaging.MessagingService;
import io.hekate.metrics.cluster.ClusterMetricsService;
import io.hekate.metrics.local.LocalMetricsService;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import io.hekate.task.CallableTask;
import io.hekate.task.TaskService;
import java.net.UnknownHostException;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HekateTaskServiceConfigurerTest extends HekateAutoConfigurerTestBase {
    @HekateInject
    public static class InjectionCall implements CallableTask<Void> {
        private static final long serialVersionUID = 1;

        @Autowired
        private Hekate hekate;

        @Autowired
        private TaskService tasks;

        @Autowired
        private ClusterService cluster;

        @Autowired
        private MessagingService messaging;

        @Autowired
        private LocalMetricsService localMetrics;

        @Autowired
        private ClusterMetricsService clusterMetrics;

        @Autowired
        private LockService locks;

        @Autowired
        private ElectionService election;

        @Autowired
        private CoordinationService coordination;

        @Autowired
        private InjectionService injection;

        @Override
        public Void call() throws Exception {
            assertNotNull(hekate);
            assertNotNull(tasks);
            assertNotNull(cluster);
            assertNotNull(messaging);
            assertNotNull(localMetrics);
            assertNotNull(clusterMetrics);
            assertNotNull(locks);
            assertNotNull(election);
            assertNotNull(coordination);
            assertNotNull(injection);

            say("Injection was successful.");

            return null;
        }
    }

    @EnableAutoConfiguration
    public static class TaskConfig extends HekateTestConfigBase {
        private static final SeedNodeProviderMock SEED_NODE_PROVIDER_MOCK = new SeedNodeProviderMock();

        @Autowired(required = false)
        private TaskService taskService;

        @Override
        public SeedNodeProvider seedNodeProvider() throws UnknownHostException {
            return SEED_NODE_PROVIDER_MOCK;
        }
    }

    @Test
    public void testService() throws Exception {
        registerAndRefresh(new String[]{"hekate.task.enable=true"}, TaskConfig.class);

        assertNotNull(get("taskService", TaskService.class));
        assertNotNull(get(TaskConfig.class).taskService);

        assertEquals(100500, getNode().tasks().call((CallableTask<Integer>)() -> 100500).get().intValue());
    }

    @Test
    public void testInjection() throws Exception {
        registerAndRefresh(new String[]{"hekate.task.enable=true", "endpoints.jmx.enabled=false"}, TaskConfig.class);
        registerAndRefresh(new String[]{"hekate.task.enable=true", "endpoints.jmx.enabled=false"}, TaskConfig.class);

        Hekate node = getNode();

        get(node.cluster().futureOf(topology -> topology.size() == 2));

        get(node.tasks().forRemotes().call(new InjectionCall()));
        get(node.tasks().filter(ClusterNode::isLocal).call(new InjectionCall()));
    }

}
