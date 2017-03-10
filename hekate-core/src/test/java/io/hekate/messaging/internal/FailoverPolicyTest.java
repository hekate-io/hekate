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

package io.hekate.messaging.internal;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.failover.FailoverPolicy;
import io.hekate.failover.FailureResolution;
import java.util.Optional;
import org.junit.Test;

import static io.hekate.failover.FailoverRoutingPolicy.RETRY_SAME_NODE;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertFalse;

public class FailoverPolicyTest extends HekateTestBase {
    @Test
    public void testAlwaysFail() throws Exception {
        FailoverPolicy policy = FailoverPolicy.alwaysFail();

        FailureResolution resolution = policy.apply(newContext(0));

        assertFalse(resolution.isRetry());
    }

    private DefaultFailoverContext newContext(int attempt) throws Exception {
        ClusterNode failedNode = newNode();

        return new DefaultFailoverContext(attempt, new Exception(), Optional.of(failedNode), singleton(failedNode), RETRY_SAME_NODE);
    }
}
