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

package io.hekate.javadoc.election;

import io.hekate.HekateInstanceTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.election.Candidate;
import io.hekate.election.CandidateConfig;
import io.hekate.election.ElectionService;
import io.hekate.election.ElectionServiceFactory;
import io.hekate.election.FollowerContext;
import io.hekate.election.LeaderContext;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class ElectionServiceJavadocTest extends HekateInstanceTestBase {
    @Test
    public void exampleBootstrap() throws Exception {
        // Start:candidate
        class ExampleCandidate implements Candidate {
            @Override
            public void becomeLeader(LeaderContext ctx) {
                System.out.println("I'm leader.");

                // ...do some work as a election...

                System.out.println("Done with the leader task ...will yield leadership.");

                // Let some other node to become a election.
                ctx.yieldLeadership();
            }

            @Override
            public void becomeFollower(FollowerContext ctx) {
                System.out.println("Leader: " + ctx.getLeader());

                // Listen for leader change events while we are staying in the follower state.
                ctx.addLeaderChangeListener(changed ->
                    System.out.println("Leader changed: " + changed.getLeader())
                );
            }

            @Override
            public void terminate() {
                // ...cleanup logic...
            }
        }
        // End:candidate

        // Start:configure
        // Prepare service factory.
        ElectionServiceFactory factory = new ElectionServiceFactory()
            // Register candidate.
            .withCandidate(new CandidateConfig()
                // Group name.
                .withGroup("example.election.group")
                // Candidate implementation.
                .withCandidate(new ExampleCandidate())
            );

        // Start node.
        Hekate hekate = new HekateBootstrap()
            .withService(factory)
            .join();
        // End:configure

        // Start:access
        // Get service.
        ElectionService election = hekate.get(ElectionService.class);

        // Get current leader (or wait up to 3 seconds for leader to be elected).
        ClusterNode leader = election.leader("example.election.group").get(3, TimeUnit.SECONDS);
        // End:access

        assertNotNull(leader);

        hekate.leave();
    }
}
