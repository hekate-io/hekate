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
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterNodeRuntime;
import io.hekate.core.ServiceInfo;
import io.hekate.core.internal.util.Utils;
import io.hekate.core.service.Service;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringFormat;
import io.hekate.util.format.ToStringIgnore;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

public class DefaultClusterNode implements Serializable, ClusterNode {
    private static class JoinOrderFormatter implements ToStringFormat.Formatter {
        @Override
        public String format(Object val) {
            Integer order = (Integer)val;

            return order == null || order == NON_JOINED_ORDER ? null : order.toString();
        }
    }

    private static class HostInfoFormatter implements ToStringFormat.Formatter {
        @Override
        public String format(Object val) {
            ClusterNodeRuntime info = (ClusterNodeRuntime)val;

            return ClusterNodeRuntime.class.getSimpleName()
                + "[cpus=" + info.cpus()
                + ", mem=" + Utils.byteSizeFormat(info.maxMemory())
                + ", os=" + info.osName()
                + ", jvm=" + info.jvmVersion()
                + ", pid=" + info.pid()
                + "]";
        }
    }

    public static final int NON_JOINED_ORDER = 0;

    private static final long serialVersionUID = 1;

    private final ClusterAddress address;

    private final String name;

    private final Set<String> roles;

    private final Map<String, String> properties;

    @ToStringIgnore
    private final Map<String, ServiceInfo> services;

    @ToStringFormat(HostInfoFormatter.class)
    private final ClusterNodeRuntime runtime;

    @ToStringIgnore
    private final boolean local;

    @ToStringFormat(JoinOrderFormatter.class)
    private volatile int joinOrder;

    public DefaultClusterNode(ClusterAddress address, String name, boolean localNode, int joinOrder, Set<String> roles,
        Map<String, String> properties, Map<String, ServiceInfo> services, ClusterNodeRuntime runtime) {
        assert address != null : "Address is null.";
        assert roles != null : "Node roles are null.";
        assert properties != null : "Node properties are null.";
        assert services != null : "Node services are null.";
        assert runtime != null : "Runtime info is null.";

        this.address = address;
        this.name = name != null ? name : "";
        this.local = localNode;
        this.joinOrder = joinOrder;
        this.roles = roles;
        this.properties = properties;
        this.services = services;
        this.runtime = runtime;
    }

    public DefaultClusterNode(ClusterNode src) {
        this.address = src.address();
        this.name = src.name();
        this.local = src.isLocal();
        this.properties = src.properties();
        this.roles = src.roles();
        this.services = src.services();
        this.runtime = src.runtime();
    }

    @Override
    public ClusterNodeId id() {
        return address.id();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean isLocal() {
        return local;
    }

    @Override
    public boolean isRemote() {
        return !isLocal();
    }

    @Override
    public int joinOrder() {
        return joinOrder;
    }

    public void setJoinOrder(int joinOrder) {
        this.joinOrder = joinOrder;
    }

    @Override
    public Set<String> roles() {
        return roles;
    }

    @Override
    public boolean hasRole(String role) {
        return roles.contains(role);
    }

    @Override
    public Map<String, String> properties() {
        return properties;
    }

    @Override
    public String property(String name) {
        return properties.get(name);
    }

    @Override
    public boolean hasProperty(String name) {
        return properties.containsKey(name);
    }

    @Override
    public boolean hasService(Class<? extends Service> type) {
        return services.keySet().contains(type.getName());
    }

    @Override
    public ServiceInfo service(Class<? extends Service> type) {
        return services.get(type.getName());
    }

    @Override
    public Map<String, ServiceInfo> services() {
        return services;
    }

    @Override
    public ClusterAddress address() {
        return address;
    }

    @Override
    public InetSocketAddress socket() {
        return address.socket();
    }

    @Override
    public ClusterNodeRuntime runtime() {
        return runtime;
    }

    @Override
    public int compareTo(ClusterNode o) {
        return address.compareTo(o.address());
    }

    public String toDetailedString() {
        return ToString.format(ClusterNode.class, this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof ClusterNode)) {
            return false;
        }

        ClusterNode that = (ClusterNode)o;

        return address.equals(that.address());
    }

    @Override
    public int hashCode() {
        return address.hashCode();
    }

    @Override
    public String toString() {
        String name = name();

        if (name.isEmpty()) {
            return address.toString();
        } else {
            return name + '#' + address;
        }
    }
}
