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

package io.hekate.core;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecService;
import io.hekate.coordinate.CoordinationService;
import io.hekate.coordinate.CoordinationServiceFactory;
import io.hekate.core.service.Service;
import io.hekate.core.service.ServiceFactory;
import io.hekate.election.ElectionService;
import io.hekate.election.ElectionServiceFactory;
import io.hekate.lock.LockService;
import io.hekate.lock.LockServiceFactory;
import io.hekate.messaging.MessagingService;
import io.hekate.messaging.MessagingServiceFactory;
import io.hekate.network.NetworkService;
import io.hekate.network.NetworkServiceFactory;
import io.hekate.rpc.RpcService;
import io.hekate.rpc.RpcServiceFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <span class="startHere">&laquo; start here</span>Main entry point to Hekate services.
 *
 * <h2>Overview</h2>
 * <p>
 * Hekate is a Java Library for cluster discovery and communications. It provides a number of services for building a cluster of
 * interconnected processes with messaging capabilities.
 * </p>
 *
 * <h2>Services</h2>
 * <p>
 * This interface is the main entry point for accessing the following services:
 * </p>
 *
 * <ul>
 * <li>
 * <b>{@link ClusterService Cluster}</b> - manages dynamic information about the cluster members and provides support for application
 * to get notified upon cluster membership changes
 * </li>
 * <li>
 * <b>{@link MessagingService Messaging}</b> - provides high-level API for asynchronous messaging among the cluster nodes with built-in
 * load balancing and retries.
 * </li>
 * <li>
 * <b>{@link RpcService Remote Procedure Calls (RPC)}</b> - provides support for remote calls of Java objects
 * </li>
 * <li>
 * <b>{@link LockService Distributed Locks}</b> - provides support for distributed locks
 * </li>
 * <li>
 * <b>{@link ElectionService Leader Election}</b> - provides support for cluster-wide leader election (aka cluster singleton)
 * </li>
 * <li>
 * <b>{@link CoordinationService Distributed Coordination}</b> - provides support for implementing distributed coordination protocols
 * </li>
 * <li>
 * <b>{@link NetworkService Networking}</b> - provides configuration options and low level API for network communications
 * </li>
 * <li>
 * <b>{@link CodecService Data Encoding/Decoding}</b> - provides abstraction layer of data serialization API
 * </li>
 * </ul>
 *
 * <h2>Configuration and Bootstrapping</h2>
 * <p>
 * Instances of {@link Hekate} interface can be constructed by calling the {@link HekateBootstrap#join()} method (or its
 * {@link HekateBootstrap#joinAsync() asynchronous equivalent}). This method creates a new {@link Hekate} instance and joins it to the
 * cluster.
 * </p>
 *
 * <p>
 * It is possible to create and run multiple {@link Hekate} instances within a single JVM. Each such instance is an independent cluster
 * node with its own set of resources (threads, sockets, etc).
 * </p>
 *
 * <p>
 * Key configuration options of {@link HekateBootstrap} are:
 * </p>
 * <ul>
 * <li>{@link HekateBootstrap#setClusterName(String) Cluster name}</li>
 * <li>{@link HekateBootstrap#setNodeName(String) Node name}</li>
 * <li>{@link HekateBootstrap#setProperties(Map) Node properties}</li>
 * <li>{@link HekateBootstrap#setRoles(List) Node roles}</li>
 * <li>{@link HekateBootstrap#setDefaultCodec(CodecFactory) Serialization codec}</li>
 * <li>{@link HekateBootstrap#setServices(List) Services} to be provided by the node</li>
 * <li>{@link HekateBootstrap#setPlugins(List) Plugins} that should run within the node</li>
 * </ul>
 *
 * <p>
 * For service-dependent configuration options please see the <a href="#service_factories">Services Factories</a> section below.
 * </p>
 *
 * <p>Minimalistic example of {@link Hekate} bootstrapping:</p>
 * <div class="tabs">
 * <ul>
 * <li><a href="#simple-java">Java</a></li>
 * <li><a href="#simple-xsd">Spring XSD</a></li>
 * <li><a href="#simple-bean">Spring bean</a></li>
 * </ul>
 * <div id="simple-java">
 * ${source: HekateJavadocTest.java#bootstrap}
 * </div>
 * <div id="simple-xsd">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source:simple-xsd.xml#example}
 * </div>
 * <div id="simple-bean">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source:simple-bean.xml#example}
 * </div>
 * </div>
 *
 * <a name="service_factories"></a>
 * <h2>Service Factories</h2>
 * <p>
 * Each service has a configurable {@link ServiceFactory} that can be registered within a {@link HekateBootstrap} instance via {@link
 * HekateBootstrap#setServices(List)} or {@link HekateBootstrap#withService(ServiceFactory)} methods. The code example below shows how
 * different services can be configured:
 * ${source: HekateJavadocTest.java#configure_services}
 * </p>
 *
 * <p>
 * For other configuration options please see the documentation of a relevant service factory:
 * </p>
 * <ul>
 * <li>{@link ClusterServiceFactory}</li>
 * <li>{@link NetworkServiceFactory}</li>
 * <li>{@link MessagingServiceFactory}</li>
 * <li>{@link RpcServiceFactory}</li>
 * <li>{@link LockServiceFactory}</li>
 * <li>{@link ElectionServiceFactory}</li>
 * <li>{@link CoordinationServiceFactory}</li>
 * </ul>
 *
 *
 * <a name="lifecycle"></a>
 * <h2>Lifecycle</h2>
 * <p>
 * The lifecycle of each {@link Hekate} instance is controlled by the following methods:
 * </p>
 * <ul>
 * <li>{@link #initialize()} - initializes this instance and all of services</li>
 * <li>{@link #join()} - initializes this instance (if not {@link #initializeAsync() initialized} yet) and joins the cluster</li>
 * <li>{@link #leave()} - leaves the cluster and then {@link #terminate() terminates} this instance. This is the recommended way to
 * shutdown gracefully</li>
 * <li>{@link #terminate()} - terminates this instance, bypassing the cluster leave protocol (i.e. remote nodes will notice that this node
 * left the cluster only based on their failure detection settings). In general, it is recommended to use {@link #leave() graceful}
 * shutdown and use this method for abnormal termination (f.e. in case of unrecoverable error) or for testing purposes to emulate failures
 * of cluster nodes</li>
 * </ul>
 *
 * <p>
 * Current state of a {@link Hekate} instance can be inspected via the {@link Hekate#state()} method. State changes can be monitored by
 * registering a listener via the {@link #addListener(LifecycleListener)} method.
 * </p>
 *
 * @see HekateBootstrap
 */
public interface Hekate extends HekateSupport {
    /**
     * State of the {@link Hekate} instance life cycle.
     * <p>
     * {@link #DOWN} &rarr;
     * {@link #INITIALIZING} &rarr;
     * {@link #JOINING} &rarr;
     * {@link #SYNCHRONIZING} &rarr;
     * {@link #UP} &rarr;
     * {@link #LEAVING} &rarr;
     * {@link #TERMINATING}
     * </p>
     *
     * @see Hekate#state()
     * @see Hekate#addListener(LifecycleListener)
     */
    enum State {
        /** Initial state. */
        DOWN,

        /** Initializing services and starting {@link SeedNodeProvider seed nodes} discovery . */
        INITIALIZING,

        /** Initialized and ready to start {@link #JOINING}. */
        INITIALIZED,

        /** Initiated the cluster joining with one of the {@link SeedNodeProvider seed nodes}. */
        JOINING,

        /** Synchronizing with remote nodes. */
        SYNCHRONIZING,

        /** Up and running. */
        UP,

        /** Started leaving the cluster. */
        LEAVING,

        /** Left the cluster and started terminating services. Switches to the {@link #DOWN} state once the termination is complete. */
        TERMINATING
    }

    /**
     * Listener of {@link Hekate.State} changes.
     *
     * @see Hekate#addListener(LifecycleListener)
     * @see Hekate#state()
     */
    interface LifecycleListener {
        /**
         * Gets called after each change of {@link Hekate#state()}.
         *
         * @param changed {@link Hekate} instance.
         */
        void onStateChanged(Hekate changed);
    }

    /**
     * Returns the {@link ClusterService}.
     *
     * @return Service.
     */
    ClusterService cluster();

    /**
     * Returns the {@link RpcService}.
     *
     * @return Service.
     */
    RpcService rpc();

    /**
     * Returns the {@link MessagingService}.
     *
     * @return Service.
     */
    MessagingService messaging();

    /**
     * Returns the {@link LockService}.
     *
     * @return Service.
     */
    LockService locks();

    /**
     * Returns the {@link ElectionService}.
     *
     * @return Service.
     */
    ElectionService election();

    /**
     * Returns the {@link CoordinationService}.
     *
     * @return Service.
     */
    CoordinationService coordination();

    /**
     * Returns the {@link NetworkService}.
     *
     * @return Service.
     */
    NetworkService network();

    /**
     * Returns the {@link CodecService}.
     *
     * @return Service.
     */
    CodecService codec();

    /**
     * Checks if the {@link Service service} of the specified type is {@link HekateBootstrap#withService(ServiceFactory) registered} within
     * this instance.
     *
     * @param service Service type.
     * @param <T> Service type.
     *
     * @return {@code true} if service is registered.
     */
    <T extends Service> boolean has(Class<T> service);

    /**
     * Returns the {@link Service service} of the specified type (f.e. {@link ClusterService}, {@link MessagingService} etc).
     * Throws an {@link IllegalArgumentException} if such a service is not {@link HekateBootstrap#withService(ServiceFactory) registered}.
     *
     * @param service Service type.
     * @param <T> Service type.
     *
     * @return Service instance.
     *
     * @throws IllegalArgumentException If service of the specified type doesn't exist (use {@link #has(Class)} to check if the service
     * exists).
     * @see #has(Class)
     * @see HekateBootstrap#setServices(List)
     */
    <T extends Service> T get(Class<T> service) throws IllegalArgumentException;

    /**
     * Returns types of all registered services.
     *
     * @return Immutable set of service types.
     *
     * @see #get(Class)
     * @see HekateBootstrap#setServices(List)
     */
    Set<Class<? extends Service>> services();

    /**
     * Returns the local cluster node.
     *
     * @return Local cluster node.
     */
    ClusterNode localNode();

    /**
     * Returns the current state of this instance (see <a href="#lifecycle">instance lifecycle</a>).
     *
     * @return Current state of this instance.
     *
     * @see #addListener(LifecycleListener)
     */
    State state();

    /**
     * Sets the attribute that should be associated with this {@link Hekate} instance.
     *
     * <p>
     * Note that such attributes are NOT cluster-wide and are not visible to other nodes of the cluster.
     * Please see the {@link ClusterNode#properties()} method for cluster-wide properties support.
     * </p>
     *
     * @param name Attribute name.
     * @param value Attribute value (if {@code null} then attribute of that name will be removed).
     *
     * @return Old attribute value or {@code null}.
     */
    Object setAttribute(String name, Object value);

    /**
     * Returns an attribute that was set via {@link #setAttribute(String, Object)} method.
     *
     * @param name Attribute name.
     *
     * @return Attribute value or {@code null} if there is no such attribute.
     */
    Object getAttribute(String name);

    /**
     * Asynchronously initializes this instance without joining to the cluster.
     *
     * <p>
     * The {@link InitializationFuture} object returned by this method can be used to obtain the result of this operation.
     * </p>
     *
     * @return Future of this operation.
     *
     * @see #joinAsync()
     */
    InitializationFuture initializeAsync();

    /**
     * Synchronously initializes this instance without joining to the cluster..
     *
     * <p>
     * This method is simply a shortcut for the following sequence of method calls:
     * ${source: HekateJavadocTest.java#sync_init}
     * </p>
     *
     * @return This instance.
     *
     * @throws HekateFutureException If failure occurred during initialization.
     * @throws InterruptedException If thread gets interrupted while awaiting for completion of this operation.
     * @see #initializeAsync()
     */
    Hekate initialize() throws InterruptedException, HekateFutureException;

    /**
     * Asynchronously initializes this instance and joins the cluster.
     *
     * <p>
     * The {@link JoinFuture} object returned by this method can be used to obtain the result of this operation.
     * </p>
     *
     * @return Future of this operation.
     */
    JoinFuture joinAsync();

    /**
     * Synchronously initializes this instance and joins the cluster.
     *
     * <p>
     * This method is simply a shortcut for the following sequence of method calls:
     * ${source: HekateJavadocTest.java#sync_join}
     * </p>
     *
     * @return This instance.
     *
     * @throws HekateFutureException If failure occurred while initializing or joining to cluster.
     * @throws InterruptedException If thread gets interrupted while awaiting for completion of this operation.
     * @see #joinAsync()
     */
    Hekate join() throws InterruptedException, HekateFutureException;

    /**
     * Asynchronously leaves the cluster and terminates this instance.
     *
     * <p>
     * The {@link LeaveFuture} object returned by this method can be used to obtain the result of this operation.
     * </p>
     *
     * @return Future of this operation.
     */
    LeaveFuture leaveAsync();

    /**
     * Synchronously leaves the cluster and terminates this instance.
     *
     * <p>
     * This method is simply a shortcut for the following sequence of method calls:
     * ${source: HekateJavadocTest.java#sync_leave}
     * </p>
     *
     * @return This instance.
     *
     * @throws HekateFutureException If failure occurred while leaving the cluster.
     * @throws InterruptedException If thread gets interrupted while awaiting for completion of this operation.
     * @see #leaveAsync()
     */
    Hekate leave() throws InterruptedException, HekateFutureException;

    /**
     * Asynchronously terminates this instance.
     *
     * <p>
     * Note that this method bypasses the cluster leave protocol and remote nodes will notice that this node left the cluster only based on
     * the failure detection settings. In general it is recommended to use {@link #leave() graceful} shutdown and use this method for
     * abnormal termination (f.e. in case of unrecoverable error) or for testing purposes to emulate failures of cluster nodes.
     * </p>
     *
     * <p>
     * The {@link TerminateFuture} object returned by this method can be used to obtain the result of this operation.
     * </p>
     *
     * @return This instance.
     */
    TerminateFuture terminateAsync();

    /**
     * Synchronously terminates this instance.
     *
     * <p>
     * This method is simply a shortcut for the following sequence of method calls:
     * ${source: HekateJavadocTest.java#sync_terminate}
     * </p>
     *
     * <p>
     * Note that this method bypasses the cluster leave protocol and remote nodes will notice that this node left the cluster only based on
     * the failure detection settings. In general it is recommended to use {@link #leave() graceful} shutdown and use this method for
     * abnormal termination (f.e. in case of unrecoverable error) or for testing purposes to emulate failures of cluster nodes.
     * </p>
     *
     * @return This instance.
     *
     * @throws HekateFutureException If failure occurred during termination.
     * @throws InterruptedException If thread gets interrupted while awaiting for completion of this operation.
     * @see #terminateAsync()
     */
    Hekate terminate() throws InterruptedException, HekateFutureException;

    /**
     * Adds <a href="#lifecycle">lifecycle</a> listener.
     *
     * @param listener Lifecycle listener.
     *
     * @see #state()
     */
    void addListener(LifecycleListener listener);

    /**
     * Removes <a href="#lifecycle">lifecycle</a> listener.
     *
     * @param listener Lifecycle listener.
     *
     * @return {@code true} if listener was removed or {@code false} if there is no such listener.
     */
    boolean removeListener(LifecycleListener listener);
}
