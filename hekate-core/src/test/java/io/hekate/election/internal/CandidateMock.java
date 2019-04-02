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

package io.hekate.election.internal;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.election.Candidate;
import io.hekate.election.FollowerContext;
import io.hekate.election.LeaderContext;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertNotNull;

class CandidateMock implements Candidate {
    private final CountDownLatch terminateLatch = new CountDownLatch(1);

    private final List<ClusterNode> leadersHistory = new CopyOnWriteArrayList<>();

    private volatile LeaderContext leaderContext;

    private volatile FollowerContext followerContext;

    @Override
    public void becomeLeader(LeaderContext ctx) {
        followerContext = null;
        leaderContext = ctx;

        leadersHistory.add(ctx.localNode());
    }

    @Override
    public void becomeFollower(FollowerContext ctx) {
        leaderContext = null;
        followerContext = ctx;

        leadersHistory.add(ctx.leader());

        ctx.addListener(sameCtx -> leadersHistory.add(ctx.leader()));
    }

    @Override
    public void terminate() {
        terminateLatch.countDown();
    }

    public boolean isLeader() {
        return leaderContext != null;
    }

    public boolean isFollower() {
        return followerContext != null;
    }

    public void yieldLeadership() {
        LeaderContext localCtx = this.leaderContext;

        assertNotNull("Not a leader.", localCtx);

        this.leaderContext = null;
        this.followerContext = null;

        localCtx.yieldLeadership();
    }

    public void awaitForBecomeLeader() throws Exception {
        HekateTestBase.busyWait("become leader", () -> leaderContext != null);
    }

    public void awaitForLeaderChange(ClusterNode oldLeader) throws Exception {
        HekateTestBase.busyWait("leader change", () -> {
            ClusterNode lastLeader = lastLeader();

            return lastLeader != null && !lastLeader.equals(oldLeader);
        });
    }

    public ClusterNode lastLeader() {
        return leadersHistory.isEmpty() ? null : leadersHistory.get(leadersHistory.size() - 1);
    }

    public void awaitTermination() {
        HekateTestBase.await(terminateLatch);
    }
}
