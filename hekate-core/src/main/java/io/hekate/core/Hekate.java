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

package io.hekate.core;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.health.FailureDetector;
import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.cluster.split.SplitBrainDetector;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecService;
import io.hekate.coordinate.CoordinationService;
import io.hekate.core.service.Service;
import io.hekate.core.service.ServiceFactory;
import io.hekate.election.ElectionService;
import io.hekate.inject.InjectionService;
import io.hekate.lock.LockService;
import io.hekate.messaging.MessagingService;
import io.hekate.metrics.MetricsService;
import io.hekate.metrics.cluster.ClusterMetricsService;
import io.hekate.network.NetworkService;
import io.hekate.network.NetworkServiceFactory;
import io.hekate.partition.PartitionService;
import io.hekate.task.TaskService;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <span class="startHere">&laquo; start here</span>Main entry point to Hekate services.
 *
 * <h2>Instantiation</h2>
 * <p>
 * Instances of this interface can be constructed by calling {@link HekateBootstrap#join()} method (or its {@link
 * HekateBootstrap#joinAsync() asynchronous equivalent}). This method builds a new {@link Hekate}instance and joins it to a cluster.
 * It is possible to create and start multiple {@link Hekate} instances within a single JVM. Each instance represents an independent
 * cluster node with its own set of resources (threads, sockets, etc).
 * </p>
 *
 * <p>
 * If application is running on top of the <a href="http://projects.spring.io/spring-framework" target="_blank">Spring Framework</a> then
 * it is possible to use Spring Framework integration that is provided by the
 * <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a> class (please see its documentation for
 * more details).
 * </p>
 *
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
 * <h2>Configuration</h2>
 * <p>
 * Configuration options for {@link Hekate} instance can be specified via properties of {@link HekateBootstrap} class.
 * This class provides bean-style properties with getters/setters (for <a href="http://projects.spring.io/spring-framework"
 * target="_blank">Spring Framework</a> XML files) as well as <a href="https://en.wikipedia.org/wiki/Fluent_interface"
 * target="_blank">fluent</a>-style API for programmatic configuration via Java code.
 * </p>
 *
 * <p>
 * Key configuration options are:
 * </p>
 * <ul>
 * <li>{@link HekateBootstrap#setClusterName(String) Cluster name}</li>
 * <li>{@link HekateBootstrap#setNodeName(String) Node name}</li>
 * <li>{@link HekateBootstrap#setNodeProperties(Map) Node properties}</li>
 * <li>{@link HekateBootstrap#setNodeRoles(Set) Node roles}</li>
 * <li>{@link HekateBootstrap#setDefaultCodec(CodecFactory) Data serialization codec}</li>
 * <li>{@link HekateBootstrap#setServices(List) Services} to be provided by the node</li>
 * <li>{@link HekateBootstrap#setPlugins(List) Plugins} that should run within the node</li>
 * </ul>
 *
 * <p>
 * Other noticeable configuration options are:
 * </p>
 * <ul>
 * <li>
 * Network {@link NetworkServiceFactory#setHost(String) address} and {@link NetworkServiceFactory#setPort(int) port}
 * </li>
 * <li>
 * Cluster {@link ClusterServiceFactory#setSeedNodeProvider(SeedNodeProvider) discovery} and {@link
 * ClusterServiceFactory#setJoinValidators(List) validation}
 * </li>
 * <li>
 * Cluster {@link ClusterServiceFactory#setFailureDetector(FailureDetector) failure} and
 * {@link ClusterServiceFactory#setSplitBrainDetector(SplitBrainDetector) split-brain} detection
 * </li>
 * </ul>
 *
 * <a name="services"></a>
 * <h2>Services</h2>
 * <p>
 * Services in {@link Hekate} are represented by sub-classes of the {@link Service} interface. Each service has a configurable {@link
 * ServiceFactory} that can be registered within a {@link HekateBootstrap} instance. Such approach allows a fine grained control over
 * resources utilization and functionality that is provided by each individual {@link Hekate} instance.
 * </p>
 *
 * <p>
 * The following services are available:
 * </p>
 * <ul>
 * <li><b>{@link ClusterService}</b> - manages dynamic information about the cluster members and provides support for application to get
 * notified upon cluster membership changes</li>
 * <li><b>{@link TaskService}</b> - provides support for distributed execution of tasks and closures</li>
 * <li><b>{@link MessagingService}</b> - provides high-level API for asynchronous messaging among the cluster nodes with built-in failover
 * and load balancing</li>
 * <li><b>{@link LockService}</b> - provides support for distributed locks</li>
 * <li><b>{@link ElectionService}</b> - provides support for cluster-wide leader election (aka cluster singleton)</li>
 * <li><b>{@link CoordinationService}</b> - provides support for implementing distributed coordination protocols</li>
 * <li><b>{@link MetricsService}</b> -  provides support for managing user-defined metrics</li>
 * <li><b>{@link ClusterMetricsService}</b> - provides access to metrics of remote cluster nodes</li>
 * <li><b>{@link NetworkService}</b> - provides configuration options and low level API for network communications</li>
 * <li><b>{@link PartitionService}</b> - provides support for consistent mapping and routing of messages among the cluster nodes</li>
 * <li><b>{@link CodecService}</b> - provides abstraction layer of data serialization API</li>
 * <li><b>{@link InjectionService}</b> - provides support for dependency injection</li>
 * </ul>
 *
 * <p>
 * Services can be obtained via {@link #get(Class)} method by specifying a service interface as in the examples below:
 * ${source: HekateJavadocTest.java#get_service}
 * </p>
 *
 * <a name="lifecycle"></a>
 * <h2>Lifecycle</h2>
 * <p>
 * Lifecycle of each {@link Hekate} instance is controlled by the following methods:
 * </p>
 * <ul>
 * <li>{@link #join()} - initializes this instance and joins the cluster. Usually this methods shouldn't be called manually unless you
 * instructed this instance to {@link #leave() leave} the cluster and then want it to join back.</li>
 *
 * <li>{@link #leave()} - leaves the cluster and then {@link #terminate() terminates} this instance. This is the recommended way of
 * graceful
 * shutdown.</li>
 *
 * <li>{@link #terminate()} - terminates this instance bypassing the cluster leave protocol (i.e. remote nodes will notice that this node
 * left the cluster only based on their failure detection settings). In general it is recommended to use {@link #leave() graceful} shutdown
 * and to use this method for abnormal termination (f.e. in case of unrecoverable error) or for testing purposes to emulate cluster node
 * failures.</li>
 * </ul>
 *
 * <p>
 * Each {@link Hekate} instance goes thorough the following lifecycle stages:
 * </p>
 * <ul>
 * <li>{@link State#DOWN} - initial state</li>
 *
 * <li>{@link State#INITIALIZING} - node is initializing its services, gets ready to join the cluster and starts contacting {@link
 * SeedNodeProvider seed nodes}.</li>
 *
 * <li>{@link State#JOINING} - node successfully discovered a seed node and started joining the cluster.</li>
 *
 * <li>{@link State#UP} - node successfully joined the cluster and is ready to perform user operations.</li>
 *
 * <li>{@link State#LEAVING} - node started leaving the cluster.</li>
 *
 * <li>{@link State#TERMINATING} - node left the cluster and started terminating all of its services. Once this stage is completed then
 * node will go back to the {@link State#DOWN} state.</li>
 * </ul>
 *
 * <p>
 * Current state of {@link Hekate} instance can be inspected via {@link Hekate#getState()} and monitored via {@link
 * #addListener(LifecycleListener)} methods.
 * </p>
 *
 * @see HekateBootstrap
 */
public interface Hekate {
    /**
     * Lifecycle state of {@link Hekate} instance.
     * <p>
     * {@link #DOWN} &rarr; {@link #INITIALIZING} &rarr; {@link #JOINING} &rarr; {@link #UP} &rarr; {@link #LEAVING} &rarr; {@link
     * #TERMINATING}
     * </p>
     *
     * @see Hekate#getState()
     * @see Hekate#addListener(LifecycleListener)
     */
    enum State {
        /** Initial state. */
        DOWN,

        /** Initializing services and starting {@link SeedNodeProvider seed nodes} discovery . */
        INITIALIZING,

        /** Initiated the cluster join protocol with one of {@link SeedNodeProvider seed nodes}. */
        JOINING,

        /** Up and running. */
        UP,

        /** Started leaving the cluster. */
        LEAVING,

        /** Left the cluster and started terminating services. Will switch to {@link #DOWN} once completed. */
        TERMINATING
    }

    /**
     * Listener of {@link Hekate.State} changes.
     *
     * @see Hekate#addListener(LifecycleListener)
     * @see Hekate#getState()
     */
    interface LifecycleListener {
        /**
         * Gets called after each change of {@link Hekate#getState()}.
         *
         * @param changed {@link Hekate} instance.
         */
        void onStateChanged(Hekate changed);
    }

    /**
     * Checks if a {@link Service service} of the specified type is {@link HekateBootstrap#withService(ServiceFactory) registered} within
     * this instance.
     *
     * @param service Service class.
     * @param <T> Concrete service type.
     *
     * @return {@code true} if service exists.
     */
    <T extends Service> boolean has(Class<T> service);

    /**
     * Returns a {@link Service service} of the specified type (for example {@link ClusterService}, {@link MessagingService} etc).
     * Throws {@link IllegalArgumentException} if such service is not {@link HekateBootstrap#withService(ServiceFactory) registered}.
     *
     * @param service Service class.
     * @param <T> Concrete service type.
     *
     * @return Service instance.
     *
     * @throws IllegalArgumentException If service of the specified type doesn't exist (use {@link #has(Class)} to check if service
     * exists).
     * @see #has(Class)
     * @see HekateBootstrap#withService(ServiceFactory)
     */
    <T extends Service> T get(Class<T> service) throws IllegalArgumentException;

    /**
     * Returns types of all registered services.
     *
     * @return Immutable set of service types.
     *
     * @see #get(Class)
     * @see HekateBootstrap#withService(ServiceFactory)
     */
    Set<Class<? extends Service>> getServiceTypes();

    /**
     * Returns the local cluster node.
     *
     * @return Local cluster node.
     *
     * @throws IllegalStateException If this instance is not in {@link State#JOINING JOINING}, {@link State#UP UP} or {@link State#LEAVING
     * LEAVING} state.
     */
    ClusterNode getNode() throws IllegalStateException;

    /**
     * Returns the current state of this instance (see <a href="#lifecycle">instance lifecycle</a>).
     *
     * @return Current state of this instance.
     *
     * @see #addListener(LifecycleListener)
     */
    State getState();

    /**
     * Sets the attribute that should be associated with this {@link Hekate} instance.
     *
     * <p>
     * Note that such attributes are NOT cluster-wide and are not visible to other nodes of the cluster.
     * Please see {@link ClusterNode#getProperties()} for cluster-wide properties support.
     * </p>
     *
     * @param name Attribute name.
     * @param value Attribute value (if {@code null} then attribute of that name will be removed).
     * @param <A> Attribute type.
     *
     * @return Old attribute value or {@code null}.
     */
    <A> A setAttribute(String name, Object value);

    /**
     * Returns an attribute that was set via {@link #setAttribute(String, Object)} method.
     *
     * @param name Attribute name.
     * @param <A> Attribute type.
     *
     * @return Attribute value or {@code null} if there is no such attribute.
     */
    <A> A getAttribute(String name);

    /**
     * Returns the system information.
     *
     * @return System information.
     */
    SystemInfo getSysInfo();

    /**
     * Asynchronously initializes this instance and joins the cluster.
     *
     * <p>
     * {@link JoinFuture} object that is returned by this method can be used to get a result of this operation.
     * </p>
     *
     * @return Future of this operation.
     *
     * @throws IllegalStateException If this instance is in {@link State#LEAVING LEAVING} or {@link State#TERMINATING TERMINATING} state.
     */
    JoinFuture joinAsync() throws IllegalStateException;

    /**
     * Synchronously initializes this instance and joins the cluster.
     *
     * <p>
     * This method is merely a shortcut to the following call sequence:
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
     * {@link LeaveFuture} object that is returned by this method can be used to get a result of this operation.
     * </p>
     *
     * @return Future of this operation.
     */
    LeaveFuture leaveAsync();

    /**
     * Synchronously leaves the cluster and terminates this instance.
     *
     * <p>
     * This method is merely a shortcut to the following call sequence:
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
     * their failure detection settings. In general it is recommended to use {@link #leave() graceful} shutdown and to use this method for
     * abnormal termination (f.e. in case of unrecoverable error) or for testing purposes to emulate cluster node failures.
     * </p>
     *
     * <p>
     * {@link TerminateFuture} object that is returned by this method can be used to get a result of this operation.
     * </p>
     *
     * @return This instance.
     */
    TerminateFuture terminateAsync();

    /**
     * Synchronously terminates this instance.
     *
     * <p>
     * This method is merely a shortcut to the following call sequence:
     * ${source: HekateJavadocTest.java#sync_terminate}
     * </p>
     *
     * <p>
     * Note that this method bypasses the cluster leave protocol and remote nodes will notice that this node left the cluster only based on
     * their failure detection settings. In general it is recommended to use {@link #leave() graceful} shutdown and to use this method for
     * abnormal termination (f.e. in case of unrecoverable error) or for testing purposes to emulate cluster node failures.
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
     * @see #getState()
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
