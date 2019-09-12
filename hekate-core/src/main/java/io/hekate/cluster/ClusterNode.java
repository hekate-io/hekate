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

import io.hekate.core.Hekate.State;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.ServiceInfo;
import io.hekate.core.service.Service;
import io.hekate.network.NetworkServiceFactory;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Provides information about a single cluster node.
 *
 * @see ClusterTopology
 * @see ClusterService
 */
public interface ClusterNode extends Comparable<ClusterNode>, ClusterNodeIdSupport {
    /**
     * Returns the universally unique identifier of this node.
     *
     * @return Universally unique identifier of this node.
     */
    @Override
    ClusterNodeId id();

    /**
     * Returns the name of this node. Returns an empty string if this node doesn't have a configured name.
     *
     * <p>
     * Value of this property can be configured via {@link HekateBootstrap#setNodeName(String)} method.
     * </p>
     *
     * @return Name of this node or an empty string if node name is not configured.
     */
    String name();

    /**
     * Returns {@code true} if this is a local node.
     *
     * @return {@code true} if this is a local node.
     *
     * @see #isRemote()
     */
    boolean isLocal();

    /**
     * Returns {@code true} if this is a remote node.
     *
     * @return {@code true} if this is a remote node.
     *
     * @see #isLocal()
     */
    boolean isRemote();

    /**
     * Returns the network address of this node.
     *
     * <p>
     * Network address can be configured by the following methods:
     * </p>
     * <ul>
     * <li>{@link NetworkServiceFactory#setHost(String)}</li>
     * <li>{@link NetworkServiceFactory#setPort(int)}</li>
     * </ul>
     *
     * @return Network address of this node.
     */
    ClusterAddress address();

    /**
     * Returns the network socket address of this node.
     *
     * <p>
     * This is a shortcut for {@link #address()}.{@link ClusterAddress#socket() getSocket()}
     * </p>
     *
     * @return Network socket address of this node.
     */
    InetSocketAddress socket();

    /**
     * Returns information about the JVM of this node.
     *
     * @return Information about the JVM of this node.
     */
    ClusterNodeRuntime runtime();

    /**
     * Returns the immutable set of roles that are configured for this node. Returns an empty set if roles are not configured for this
     * node.
     *
     * <p>
     * Roles can be configured via {@link HekateBootstrap#setRoles(List)} method.
     * </p>
     *
     * @return Immutable set of node roles aor an empty set if roles are not configured.
     */
    Set<String> roles();

    /**
     * Returns {@code true} if this node has the specified role (see {@link #roles()}).
     *
     * @param role Role.
     *
     * @return {@code true} if this node has the specified role.
     */
    boolean hasRole(String role);

    /**
     * Returns the immutable map of properties that are configured for this node. Returns an empty map if properties are not configured for
     * this node.
     *
     * <p>
     * Properties can be configured via {@link HekateBootstrap#setProperties(Map)} method.
     * </p>
     *
     * @return Immutable map of node properties or an empty map if properties are not configured.
     */
    Map<String, String> properties();

    /**
     * Returns value for the specified property name (see {@link #properties()}).
     *
     * @param name Property name.
     *
     * @return Property value or {@code null} if there is no such property.
     */
    String property(String name);

    /**
     * Returns {@code true} if this node has a property with the specified name (see {@link #properties()}).
     *
     * @param name Property name.
     *
     * @return {@code true} if this node has a property with the specified name.
     */
    boolean hasProperty(String name);

    /**
     * Returns {@code true} if this node has a service of the specified type.
     *
     * @param type Service type (must be an interface that extends {@link Service}).
     *
     * @return {@code true} if this node has a service of the specified type.
     *
     * @see #services()
     */
    boolean hasService(Class<? extends Service> type);

    /**
     * Returns the service information for the specified type.
     *
     * @param type Service type (must be an interface that extends {@link Service}).
     *
     * @return Service instance or {@code null} if there is no such service.
     *
     * @see #services()
     */
    ServiceInfo service(Class<? extends Service> type);

    /**
     * Returns the immutable map of services that are provided by this node, with the {@link ServiceInfo#type() service type} as
     * the key.
     *
     * @return Immutable map of services.
     *
     * @see HekateBootstrap#setServices(List)
     */
    Map<String, ServiceInfo> services();

    /**
     * Returns the cluster join order. Indexing the join order begins with 1, so the first node that joins the cluster has a value of 1,
     * the
     * second node has a value of 2, and so on. The value of this property is initialized only when the node joins the cluster and is NOT
     * updated if the previous (old) node leaves the cluster.
     *
     * <p>
     * Note that the default value for this property is 0 and it is initialized with its actual value only when the node switches to the
     * {@link State#UP} state.
     * </p>
     *
     * @return Cluster join order or 0 if node is not joined yet.
     */
    int joinOrder();

    /**
     * Compares this node with the specified one based on {@link #id()} value.
     *
     * @param o Other node.
     *
     * @return Result of {@link #id()} values comparison.
     */
    @Override
    int compareTo(ClusterNode o);
}
