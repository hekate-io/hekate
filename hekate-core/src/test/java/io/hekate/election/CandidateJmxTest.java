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

package io.hekate.election;

import io.hekate.HekateNodeTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.core.jmx.JmxService;
import io.hekate.core.jmx.JmxServiceFactory;
import io.hekate.core.jmx.JmxTestUtils;
import java.util.ArrayList;
import java.util.List;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import org.junit.Test;

import static io.hekate.core.jmx.JmxTestUtils.jmxAttribute;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class CandidateJmxTest extends HekateNodeTestBase {
    @Test
    public void test() throws Exception {
        List<HekateTestNode> nodes = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            String jmxDomain = "test-node-" + i;

            createNode(boot -> {
                boot.withService(JmxServiceFactory.class, jmx -> jmx.withDomain(jmxDomain));
                boot.withService(ElectionServiceFactory.class, election -> {
                    election.withCandidate(new CandidateConfig()
                        .withGroup("test-group-1")
                        .withCandidate(mock(Candidate.class))
                    );
                    election.withCandidate(new CandidateConfig()
                        .withGroup("test-group-2")
                        .withCandidate(mock(Candidate.class))
                    );
                });
            }).join();
        }

        awaitForTopology(nodes);

        for (HekateTestNode node : nodes) {
            ClusterNode leader1 = get(node.election().leader("test-group-1"));
            ClusterNode leader2 = get(node.election().leader("test-group-2"));

            ObjectName name1 = node.get(JmxService.class).nameFor(CandidateJmx.class, "test-group-1");
            ObjectName name2 = node.get(JmxService.class).nameFor(CandidateJmx.class, "test-group-2");

            assertEquals("test-group-1", jmxAttribute(name1, "group", String.class, node));
            assertEquals("test-group-2", jmxAttribute(name2, "group", String.class, node));

            assertEquals(node.localNode().equals(leader1), jmxAttribute(name1, "leader", Boolean.class, node));
            assertEquals(node.localNode().equals(leader2), jmxAttribute(name2, "leader", Boolean.class, node));

            JmxTestUtils.verifyJmxNode(leader1, jmxAttribute(name1, "leaderNode", CompositeData.class, node));
            JmxTestUtils.verifyJmxNode(leader2, jmxAttribute(name2, "leaderNode", CompositeData.class, node));
        }
    }
}
