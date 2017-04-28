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

package io.hekate.javadoc.messaging.unicast;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.internal.DefaultClusterTopology;
import io.hekate.core.HekateException;
import io.hekate.messaging.internal.LoadBalancerContextBridge;
import io.hekate.messaging.unicast.LoadBalancer;
import io.hekate.messaging.unicast.LoadBalancerContext;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class LoadBalancerJavadocTest extends HekateTestBase {
    // Start:load_balancer
    public static class ExampleLoadBalancer implements LoadBalancer<Object> {
        @Override
        public ClusterNodeId route(Object message, LoadBalancerContext ctx) throws HekateException {
            // Calculate position of a destination node within the cluster topology.
            int idx = Math.abs(message.hashCode() % ctx.size());

            // Select the destination node
            // Note that nodes are always sorted by their IDs within the topology.
            return ctx.getTopology().getNodes().get(idx).getId();
        }
    }
    // End:load_balancer

    @Test
    public void exampleLoadBalancer() throws Exception {
        ExampleLoadBalancer balancer = new ExampleLoadBalancer();

        ClusterTopology topology = DefaultClusterTopology.of(1, toSet(newNode(), newNode(), newNode()));

        LoadBalancerContext ctx = new LoadBalancerContextBridge(100, null, topology);

        assertNotNull(balancer.route("test", ctx));
    }
}
