/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.cluster.internal;

import io.hekate.HekateNodeParamTestBase;
import io.hekate.HekateTestContext;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.split.SplitBrainAction;
import io.hekate.core.Hekate;
import io.hekate.core.HekateFutureException;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.core.internal.util.Jvm;
import io.hekate.test.SplitBrainDetectorMock;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ClusterServiceSplitBrainTest extends HekateNodeParamTestBase {
    public ClusterServiceSplitBrainTest(HekateTestContext params) {
        super(params);
    }

    @Test
    public void testRejoinOnJoin() throws Exception {
        SplitBrainDetectorMock detector = new SplitBrainDetectorMock(false);

        HekateTestNode node = createNode(c -> {
            ClusterServiceFactory cluster = c.service(ClusterServiceFactory.class).get();

            cluster.setSplitBrainAction(SplitBrainAction.REJOIN);
            cluster.setSplitBrainDetector(detector);
        });

        Future<HekateTestNode> future = runAsync(node::join);

        detector.awaitForChecks(5);

        assertSame(Hekate.State.JOINING, node.state());

        detector.setValid(true);

        get(future);

        assertEquals(1, detector.getChecks());
    }

    @Test
    public void testRejoinWhenOtherNodeLeaves() throws Exception {
        SplitBrainDetectorMock detector = new SplitBrainDetectorMock(true);

        HekateTestNode node = createNode(c -> {
            ClusterServiceFactory cluster = c.service(ClusterServiceFactory.class).get();

            cluster.setSplitBrainAction(SplitBrainAction.REJOIN);
            cluster.setSplitBrainDetector(detector);
        }).join();

        HekateTestNode leaving = createNode().join();

        awaitForTopology(leaving, node);

        detector.setValid(false);

        leaving.leave();

        node.awaitForStatus(Hekate.State.JOINING);

        detector.awaitForChecks(5);

        detector.setValid(true);

        node.awaitForStatus(Hekate.State.UP);
    }

    @Test
    public void testTerminateWhenOtherNodeLeaves() throws Exception {
        SplitBrainDetectorMock detector = new SplitBrainDetectorMock(true);

        HekateTestNode node = createNode(c -> {
            ClusterServiceFactory cluster = c.service(ClusterServiceFactory.class).get();

            cluster.setSplitBrainAction(SplitBrainAction.TERMINATE);
            cluster.setSplitBrainDetector(detector);
        }).join();

        HekateTestNode leaving = createNode().join();

        awaitForTopology(leaving, node);

        detector.setValid(false);

        leaving.leave();

        node.awaitForStatus(Hekate.State.DOWN);

        assertEquals(1, detector.getChecks());
    }

    @Test
    public void testKillJvmWhenOtherNodeLeaves() throws Exception {
        try {
            SplitBrainDetectorMock detector = new SplitBrainDetectorMock(true);

            HekateTestNode node = createNode(c -> {
                ClusterServiceFactory cluster = c.service(ClusterServiceFactory.class).get();

                cluster.setSplitBrainAction(SplitBrainAction.KILL_JVM);
                cluster.setSplitBrainDetector(detector);
            });

            Jvm.ExitHandler jvmExitHandler = mock(Jvm.ExitHandler.class);

            doAnswer(invocation -> node.leaveAsync()).when(jvmExitHandler).exit(anyInt());

            Jvm.setExitHandler(jvmExitHandler);

            node.join();

            HekateTestNode leaving = createNode().join();

            awaitForTopology(leaving, node);

            detector.setValid(false);

            leaving.leave();

            node.awaitForStatus(Hekate.State.DOWN);

            assertEquals(1, detector.getChecks());

            verify(jvmExitHandler).exit(250);
            verifyNoMoreInteractions(jvmExitHandler);
        } finally {
            Jvm.setExitHandler(null);
        }
    }

    @Test
    public void testTerminateOnDetectorError() throws Exception {
        HekateTestNode node = createNode(c -> {
            ClusterServiceFactory cluster = c.service(ClusterServiceFactory.class).get();

            cluster.setSplitBrainAction(SplitBrainAction.REJOIN);
            cluster.setSplitBrainDetector(localNode -> {
                throw TEST_ERROR;
            });
        });

        try {
            node.join();

            fail("Error was expected.");
        } catch (HekateFutureException e) {
            assertSame(TEST_ERROR, e.getCause());
        }
    }

    @Test
    public void testTerminateOnDetectorErrorWhenOtherNodeLeaves() throws Exception {
        CountDownLatch errorLatch = new CountDownLatch(1);

        SplitBrainDetectorMock detector = new SplitBrainDetectorMock(true) {
            @Override
            public synchronized boolean isValid(ClusterNode localNode) {
                if (getChecks() == 5) {
                    errorLatch.countDown();

                    throw TEST_ERROR;
                }

                return super.isValid(localNode);
            }
        };

        HekateTestNode node = createNode(c -> {
            ClusterServiceFactory cluster = c.service(ClusterServiceFactory.class).get();

            cluster.setSplitBrainAction(SplitBrainAction.REJOIN);
            cluster.setSplitBrainDetector(detector);
        }).join();

        HekateTestNode leaving = createNode().join();

        awaitForTopology(leaving, node);

        detector.setValid(false);

        leaving.leave();

        await(errorLatch);

        node.awaitForStatus(Hekate.State.DOWN);
    }
}
