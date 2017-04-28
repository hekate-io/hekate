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

package io.hekate.cluster.internal;

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterNodeService;
import io.hekate.core.SystemInfo;
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

    private static class SystemInfoFormatter implements ToStringFormat.Formatter {
        @Override
        public String format(Object val) {
            SystemInfo info = (SystemInfo)val;

            return SystemInfo.class.getSimpleName()
                + "[cpus=" + info.getCpus()
                + ", mem=" + Utils.byteSizeFormat(info.getMaxMemory())
                + ", os=" + info.getOsName()
                + ", jvm=" + info.getJvmVersion()
                + ", pid=" + info.getPid()
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
    private final Map<String, ClusterNodeService> services;

    @ToStringFormat(SystemInfoFormatter.class)
    private final SystemInfo sysInfo;

    @ToStringIgnore
    private final boolean local;

    @ToStringFormat(JoinOrderFormatter.class)
    private volatile int joinOrder;

    public DefaultClusterNode(ClusterAddress address, String name, boolean localNode, int joinOrder, Set<String> roles,
        Map<String, String> properties, Map<String, ClusterNodeService> services, SystemInfo sysInfo) {
        assert address != null : "Address is null.";
        assert roles != null : "Node roles are null.";
        assert properties != null : "Node properties are null.";
        assert services != null : "Node services are null.";
        assert sysInfo != null : "Node system info is null.";

        this.address = address;
        this.name = name != null ? name : "";
        this.local = localNode;
        this.joinOrder = joinOrder;
        this.roles = roles;
        this.properties = properties;
        this.services = services;
        this.sysInfo = sysInfo;
    }

    public DefaultClusterNode(ClusterNode src) {
        this.address = src.getAddress();
        this.name = src.getName();
        this.local = src.isLocal();
        this.properties = src.getProperties();
        this.roles = src.getRoles();
        this.services = src.getServices();
        this.sysInfo = src.getSysInfo();
    }

    @Override
    public ClusterNodeId getId() {
        return address.getId();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isLocal() {
        return local;
    }

    @Override
    public int getJoinOrder() {
        return joinOrder;
    }

    public void setJoinOrder(int joinOrder) {
        this.joinOrder = joinOrder;
    }

    @Override
    public Set<String> getRoles() {
        return roles;
    }

    @Override
    public boolean hasRole(String role) {
        return roles.contains(role);
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String getProperty(String name) {
        return properties.get(name);
    }

    @Override
    public boolean hasProperty(String name) {
        return properties.containsKey(name);
    }

    @Override
    public boolean hasService(Class<? extends Service> type) {
        return hasService(type.getCanonicalName());
    }

    @Override
    public boolean hasService(String type) {
        return services.keySet().contains(type);
    }

    @Override
    public ClusterNodeService getService(String type) {
        return services.get(type);
    }

    @Override
    public ClusterNodeService getService(Class<? extends Service> type) {
        return getService(type.getCanonicalName());
    }

    @Override
    public Map<String, ClusterNodeService> getServices() {
        return services;
    }

    @Override
    public ClusterAddress getAddress() {
        return address;
    }

    @Override
    public InetSocketAddress getSocket() {
        return address.getSocket();
    }

    @Override
    public SystemInfo getSysInfo() {
        return sysInfo;
    }

    @Override
    public int compareTo(ClusterNode o) {
        return address.compareTo(o.getAddress());
    }

    public String toDetailedString() {
        return ToString.format(ClusterNode.class, this);
    }

    @Override
    public ClusterNodeId asClusterNodeId() {
        return getId();
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

        return address.equals(that.getAddress());
    }

    @Override
    public int hashCode() {
        return address.hashCode();
    }

    @Override
    public String toString() {
        String name = getName();

        if (name.isEmpty()) {
            return address.toString();
        } else {
            return name + '#' + address;
        }
    }
}
