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

package io.hekate.javadoc.election;

import io.hekate.HekateNodeTestBase;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.election.Candidate;
import io.hekate.election.CandidateConfig;
import io.hekate.election.ElectionService;
import io.hekate.election.ElectionServiceFactory;
import io.hekate.election.FollowerContext;
import io.hekate.election.LeaderContext;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class ElectionServiceJavadocTest extends HekateNodeTestBase {
    @Test
    public void exampleBootstrap() throws Exception {
        // Start:candidate
        class ExampleCandidate implements Candidate {
            @Override
            public void becomeLeader(LeaderContext ctx) {
                System.out.println("I'm leader.");

                // ...do some work as leader...

                System.out.println("Done with the leader task ...will yield leadership.");

                // Let some other node to become a leader.
                ctx.yieldLeadership();
            }

            @Override
            public void becomeFollower(FollowerContext ctx) {
                System.out.println("Leader: " + ctx.leader());

                // Listen for leader change events while we are staying in the follower state.
                ctx.addListener(changed ->
                    System.out.println("Leader changed: " + changed.leader())
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

        // Access the service.
        ElectionService election = hekate.election();
        // End:configure

        assertNotNull(election);

        hekate.leave();
    }
}
