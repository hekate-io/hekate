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

package io.hekate.spring.bean.internal;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterService;
import io.hekate.coordinate.CoordinationService;
import io.hekate.core.Hekate;
import io.hekate.election.ElectionService;
import io.hekate.lock.DistributedLock;
import io.hekate.lock.LockRegion;
import io.hekate.lock.LockService;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingService;
import io.hekate.metrics.Metric;
import io.hekate.metrics.cluster.ClusterMetricsService;
import io.hekate.metrics.local.CounterMetric;
import io.hekate.metrics.local.LocalMetricsService;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkService;
import io.hekate.task.TaskService;
import io.hekate.util.format.ToString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ContextConfiguration("classpath*:xsd-test/test-single-node.xml")
public class XsdSingleNodeTest extends HekateTestBase {
    @Autowired
    @Qualifier("hekateBean")
    private Hekate hekate;

    @Autowired
    @Qualifier("cluster")
    private ClusterService cluster;

    @Autowired
    @Qualifier("locks")
    private LockService locks;

    @Autowired
    @Qualifier("messaging")
    private MessagingService messaging;

    @Autowired
    @Qualifier("tasks")
    private TaskService tasks;

    @Autowired
    @Qualifier("metrics")
    private LocalMetricsService metrics;

    @Autowired
    @Qualifier("clusterMetrics")
    private ClusterMetricsService clusterMetrics;

    @Autowired
    @Qualifier("coordination")
    private CoordinationService coordination;

    @Autowired
    @Qualifier("election")
    private ElectionService election;

    @Autowired
    @Qualifier("network")
    private NetworkService network;

    @Autowired
    @Qualifier("test.protocol.1")
    private NetworkConnector<Object> networkConnector;

    @Autowired
    @Qualifier("some.channel")
    private MessagingChannel<Object> messagingChannel;

    @Autowired
    @Qualifier("region1")
    private LockRegion lockRegion;

    @Autowired
    @Qualifier("someLock1")
    private DistributedLock lock;

    @Autowired
    @Qualifier("counter1")
    private CounterMetric counter;

    @Autowired
    @Qualifier("probe1")
    private Metric probe;

    @Test
    public void test() {
        assertNotNull(hekate);
        assertNotNull(cluster);
        assertNotNull(locks);
        assertNotNull(messaging);
        assertNotNull(tasks);
        assertNotNull(metrics);
        assertNotNull(clusterMetrics);
        assertNotNull(coordination);
        assertNotNull(election);
        assertNotNull(network);
        assertNotNull(networkConnector);
        assertNotNull(messagingChannel);
        assertNotNull(lockRegion);
        assertNotNull(lock);
        assertNotNull(counter);
        assertNotNull(probe);

        assertEquals("some-value", hekate.localNode().property("some-property"));
        assertEquals(String.class, hekate.messaging().channel("some.channel", String.class).baseType());

        say("Done: " + ToString.format(this));
    }

    @Override
    protected void assertAllThreadsStopped() throws InterruptedException {
        // Do not check threads since Spring context gets terminated after all tests have been run.
    }
}
