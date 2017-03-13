package io.hekate.failover;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.failover.internal.DefaultFailoverContext;
import java.util.Optional;

import static io.hekate.failover.FailoverRoutingPolicy.RETRY_SAME_NODE;
import static java.util.Collections.singleton;

public abstract class FailoverPolicyTestBase extends HekateTestBase {
    protected DefaultFailoverContext newContext(int attempt) throws Exception {
        ClusterNode failedNode = newNode();

        return new DefaultFailoverContext(attempt, new Exception(), Optional.of(failedNode), singleton(failedNode), RETRY_SAME_NODE);
    }
}
