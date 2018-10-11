package io.hekate.election;

import io.hekate.cluster.ClusterNode;
import io.hekate.util.HekateFutureTestBase;

public class LeaderFutureTest extends HekateFutureTestBase<ClusterNode, LeaderFuture, LeaderException> {
    @Override
    protected LeaderFuture createFuture() {
        return new LeaderFuture();
    }

    @Override
    protected Class<LeaderException> errorType() {
        return LeaderException.class;
    }

    @Override
    protected ClusterNode createValue() throws Exception {
        return newNode();
    }
}
