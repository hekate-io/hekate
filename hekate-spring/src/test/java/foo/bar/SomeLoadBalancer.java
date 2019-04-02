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

package foo.bar;

import io.hekate.cluster.ClusterNodeId;
import io.hekate.messaging.loadbalance.LoadBalancer;
import io.hekate.messaging.loadbalance.LoadBalancerContext;

public class SomeLoadBalancer implements LoadBalancer<Object> {
    @Override
    public ClusterNodeId route(Object msg, LoadBalancerContext ctx) {
        return ctx.topology().random().id();
    }
}
