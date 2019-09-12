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

package io.hekate.cluster.internal;

import io.hekate.cluster.ClusterNodeJmx;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterServiceJmx;

class DefaultClusterServiceJmx implements ClusterServiceJmx {
    private final ClusterService service;

    public DefaultClusterServiceJmx(ClusterService service) {
        assert service != null : "Cluster service is null.";

        this.service = service;
    }

    @Override
    public ClusterNodeJmx getLocalNode() {
        return ClusterNodeJmx.of(service.localNode());
    }

    @Override
    public long getTopologyVersion() {
        return service.topology().version();
    }

    @Override
    public int getTopologySize() {
        return service.topology().size();
    }

    @Override
    public ClusterNodeJmx[] getTopology() {
        return service.topology().nodes().stream()
            .map(ClusterNodeJmx::of)
            .toArray(ClusterNodeJmx[]::new);
    }
}
