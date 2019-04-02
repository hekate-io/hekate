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

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNodeRuntime;
import io.hekate.core.ServiceInfo;
import io.hekate.util.format.ToString;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class DefaultClusterNodeBuilder {
    private ClusterAddress address;

    private String name;

    private boolean localNode;

    private int joinOrder = DefaultClusterNode.NON_JOINED_ORDER;

    private Set<String> roles = Collections.emptySet();

    private Map<String, String> properties = Collections.emptyMap();

    private Map<String, ServiceInfo> services = Collections.emptyMap();

    private ClusterNodeRuntime sysInfo = DefaultClusterNodeRuntime.getLocalInfo();

    public DefaultClusterNodeBuilder withAddress(ClusterAddress address) {
        this.address = address;

        return this;
    }

    public DefaultClusterNodeBuilder withName(String name) {
        this.name = name;

        return this;
    }

    public DefaultClusterNodeBuilder withLocalNode(boolean localNode) {
        this.localNode = localNode;

        return this;
    }

    public DefaultClusterNodeBuilder withJoinOrder(int joinOrder) {
        this.joinOrder = joinOrder;

        return this;
    }

    public DefaultClusterNodeBuilder withRoles(Set<String> roles) {
        this.roles = roles;

        return this;
    }

    public DefaultClusterNodeBuilder withProperties(Map<String, String> properties) {
        this.properties = properties;

        return this;
    }

    public DefaultClusterNodeBuilder withServices(Map<String, ServiceInfo> services) {
        this.services = services;

        return this;
    }

    public DefaultClusterNodeBuilder withSysInfo(ClusterNodeRuntime sysInfo) {
        this.sysInfo = sysInfo;

        return this;
    }

    public DefaultClusterNode createNode() {
        return new DefaultClusterNode(address, name, localNode, joinOrder, roles, properties, services, sysInfo);
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
