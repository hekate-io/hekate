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

package io.hekate.lock.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.core.ServiceInfo;
import io.hekate.lock.LockService;
import io.hekate.util.format.ToString;

class LockRegionNodeFilter implements ClusterNodeFilter {
    private final String regionName;

    public LockRegionNodeFilter(String regionName) {
        assert regionName != null : "Region name is null.";

        this.regionName = regionName;
    }

    public static String serviceProperty(String region) {
        return "region." + region;
    }

    @Override
    public boolean accept(ClusterNode node) {
        if (DefaultLockService.HAS_SERVICE_FILTER.accept(node)) {
            ServiceInfo service = node.service(LockService.class);

            if (service != null) {
                return service.property(serviceProperty(regionName)) != null;
            }
        }

        return false;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
