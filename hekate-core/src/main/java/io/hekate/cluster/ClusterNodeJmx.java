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

package io.hekate.cluster;

import io.hekate.util.format.ToString;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Information about a {@link ClusterNode} that is exposed to JMX.
 *
 * @see ClusterServiceJmx
 */
public class ClusterNodeJmx {
    private final String name;

    private final String host;

    private final int port;

    private final String id;

    private final Set<String> roles;

    private final Map<String, String> properties;

    /**
     * Constructs a new instance.
     *
     * @param name See {@link #getName()}.
     * @param host See {@link #getHost()}.
     * @param port See {@link #getPort()}.
     * @param id See {@link #getId()}.
     * @param roles See {@link #getRoles()}.
     * @param properties See {@link #getProperties()}.
     */
    public ClusterNodeJmx(String name, String host, int port, String id, Set<String> roles, Map<String, String> properties) {
        this.name = name;
        this.host = host;
        this.port = port;
        this.id = id;
        this.roles = roles;
        this.properties = properties;
    }

    /**
     * Constructs a new {@link ClusterNodeJmx} instance from the specified node.
     *
     * @param node Cluster node.
     *
     * @return {@link ClusterNodeJmx} instance of the specified node.
     */
    public static ClusterNodeJmx of(ClusterNode node) {
        return new ClusterNodeJmx(
            node.name(),
            node.address().socket().getHostString(),
            node.address().socket().getPort(),
            node.id().toString(),
            node.roles(),
            node.properties()
        );
    }

    /**
     * Returns the node name.
     *
     * @return Node name.
     *
     * @see ClusterNode#name()
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the node identifier.
     *
     * @return Node identifier.
     *
     * @see ClusterNode#id()
     */
    public String getId() {
        return id;
    }

    /**
     * Returns the host address.
     *
     * @return Host address.
     *
     * @see ClusterNode#socket()
     */
    public String getHost() {
        return host;
    }

    /**
     * Returns the network port.
     *
     * @return Network port.
     *
     * @see ClusterNode#socket()
     */
    public int getPort() {
        return port;
    }

    /**
     * Returns the node's roles.
     *
     * @return Roles.
     *
     * @see ClusterNode#roles()
     */
    public Set<String> getRoles() {
        return roles;
    }

    /**
     * Returns the node's properties.
     *
     * @return Properties.
     *
     * @see ClusterNode#properties()
     */
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof ClusterNodeJmx)) {
            return false;
        }

        ClusterNodeJmx that = (ClusterNodeJmx)o;

        if (port != that.port) {
            return false;
        }

        if (!Objects.equals(name, that.name)) {
            return false;
        }

        if (!Objects.equals(host, that.host)) {
            return false;
        }

        if (!Objects.equals(id, that.id)) {
            return false;
        }

        if (!Objects.equals(roles, that.roles)) {
            return false;
        }

        return Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;

        result = 31 * result + (host != null ? host.hashCode() : 0);
        result = 31 * result + port;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (roles != null ? roles.hashCode() : 0);
        result = 31 * result + (properties != null ? properties.hashCode() : 0);

        return result;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
