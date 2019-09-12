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

package io.hekate.rpc;

import io.hekate.cluster.ClusterFilterSupport;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterView;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.DefaultServiceFactory;
import io.hekate.core.service.Service;
import io.hekate.messaging.loadbalance.LoadBalancerContext;
import io.hekate.messaging.retry.GenericRetryConfigurer;
import io.hekate.partition.RendezvousHashMapper;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * <span class="startHere">&laquo; start here</span>Main entry point to Remote Procedure Call (RPC) API.
 *
 *
 * <h2>Overview</h2>
 * <p>
 * This service provides support for remote calls of Java objects in a cluster of {@link Hekate} nodes. Each such object must declare one
 * or more Java interfaces marked with the @{@link Rpc} annotation. Client nodes use this interface to build a proxy object that
 * transparently executes all local methods calls on remote nodes.
 * </p>
 *
 * <ul>
 * <li><a href="#service_configuration">Service Configuration</a></li>
 * <li><a href="#rpc_interface">RPC Interface</a></li>
 * <li><a href="#rpc_server">RPC Server</a></li>
 * <li><a href="#rpc_client">RPC Client</a></li>
 * <li><a href="#routing_and_load_balancing">Routing and Load Balancing</a></li>
 * <li><a href="#retrying_on_error">Retrying on Error</a></li>
 * </ul>
 *
 * <a name="service_configuration"></a>
 * <h2>Service Configuration</h2>
 * <p>
 * {@link RpcService} can be configured and registered in {@link HekateBootstrap} with the help of {@link RpcServiceFactory} as shown in
 * the example below:
 * </p>
 *
 * <div class="tabs">
 * <ul>
 * <li><a href="#configure-java">Java</a></li>
 * <li><a href="#configure-xsd">Spring XSD</a></li>
 * <li><a href="#configure-bean">Spring bean</a></li>
 * </ul>
 * <div id="configure-java">
 * ${source: rpc/RpcServiceJavadocTest.java#configure}
 * </div>
 * <div id="configure-xsd">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: rpc/rpc-xsd.xml#example}
 * </div>
 * <div id="configure-bean">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: rpc/rpc-bean.xml#example}
 * </div>
 * </div>
 *
 * <a name="rpc_interface"></a>
 * <h2>RPC Interface</h2>
 * <p>
 * Every object that is exposed for RPC access must implement at least one @{@link Rpc}-annotated interface.
 * Below is the example of a simple RPC interface:
 * ${source: rpc/RpcServiceJavadocTest.java#interface}
 * </p>
 *
 * <h3>Synchronous vs Asynchronous</h3>
 * <p>
 * RPC interfaces can declare both synchronous and asynchronous methods. Synchronous methods are regular methods that return an object of
 * arbitrary Java type. Invocations of such methods are always synchronous and blocks the client thread unless remote invocation is
 * completed.
 * </p>
 *
 * <p>
 * Asynchronous methods must use a {@link CompletableFuture} as their result type. Such methods are always executed asynchronously by the
 * {@link RpcService} without blocking the client thread.
 * </p>
 *
 * <h3>Aggregate and Broadcast</h3>
 * <p>
 * RPC service provides support for broadcast/aggregate operations. Each such RPC operations gets submitted to multiple nodes at once and
 * all RPC results from those nodes gets aggregated into a single result object.
 * </p>
 * <p>
 * In order to enable such functionality, an RPC method must be annotated with @{@link RpcAggregate}. Such method must be declared
 * with one of the following result types:
 * </p>
 * <ul>
 * <li>{@link List}</li>
 * <li>{@link Set}</li>
 * <li>{@link Map}</li>
 * <li>{@link Collection}</li>
 * <li>{@link CompletableFuture}{@code <}{@link List}|{@link Set}|{@link Collection}|{@link Map}{@code >}</li>
 * </ul>
 *
 * <p>
 * If no results are expected to be returned by an RPC method then such method must be annotated with @{@link RpcBroadcast} and must have a
 * {@code void} return type (or {@link CompletableFuture}{@code <?>} for asynchronous calls).
 * </p>
 *
 * <h3>Arguments Splitting</h3>
 * <p>
 * RPC service provides support for Map/Reduce style of data processing. This can be achieved by placing @{@link RpcSplit} annotation on
 * one of an @{@link RpcAggregate}-annotated method's parameter. Such parameter must be of one of the following types:
 * </p>
 * <ul>
 * <li>{@link List}</li>
 * <li>{@link Set}</li>
 * <li>{@link Map}</li>
 * <li>{@link Collection}</li>
 * </ul>
 * <p>
 * If @{@link RpcSplit} annotation is present then the value of that parameter will be split into smaller chunks (sub-collections) based
 * on the number of available cluster nodes. All chunks will be evenly distributed among the cluster nodes for parallel processing and once
 * processing on all nodes is completed then results of each node will be aggregated the same way as during the regular {@link
 * RpcAggregate}
 * call.
 * </p>
 *
 * <h3>Versioning</h3>
 * <p>
 * RPC interface can define client compatibility rules by using the interface versioning approach. Version can be specified via {@link
 * Rpc#version()} and {@link Rpc#minClientVersion()} attributes. {@link Rpc#version()}  defines the current version of the RPC interface
 * while {@link Rpc#minClientVersion()} defines the minimum version of this interface that can be used on the client side.  If client
 * detects that its local version is less than the minimum required version of the server then such client will not route any RPC requests
 * to such server.
 * </p>
 * <p>
 * Consider the following scenario:
 * </p>
 * <ol>
 * <li>Same jar with an RPC interface {@link Rpc#version()}{@code =1} is deployed both on the client and on the server nodes</li>
 * <li>After some time a new {@link Rpc#version()}={@code 2} of this interface  is implemented (possibly with some breaking changes of
 * API)</li>
 * <li>New jar file with {@link Rpc#version()}{@code =2} is deployed on a new node</li>
 * <li>At this point, if {@link Rpc#minClientVersion()} is set to {@code 2} then old client with version {@code 1} will know that its API
 * is not compatible with the server version {@code 2} and will not try to route any requests to such server.</li>
 * <li>Alternatively, if {@link Rpc#minClientVersion()} is set to {@code 1} (meaning that there were no breaking changes) then old client
 * will still be able to route requests to the new server.</li>
 * </ol>
 *
 * <a name="rpc_server"></a>
 * <h2>RPC Server</h2>
 * <p>
 * RPC server is a Java class that implements one or more @{@link Rpc}-annotated interfaces. Below is the example of such class:
 * ${source: rpc/RpcServiceJavadocTest.java#impl}
 * </p>
 *
 * <h3>Server Registration</h3>
 * <p>
 * Each RPC server must be registered within the {@link RpcServiceFactory} in order to be exposed for remote access. Configuration of each
 * RPC server is represented by the {@link RpcServerConfig} class. Below is the example of RPC server registration:
 * ${source: rpc/RpcServiceJavadocTest.java#server}
 * </p>
 *
 * <h3>Tagging</h3>
 * <p>
 * If multiple servers implement the same RPC interface and must be deployed to the same {@link Hekate} node then each such server must
 * have an additional qualifier that will help RPC clients to distinguish which exact RPC server they are communicating with.
 * </p>
 *
 * <p>
 * Such qualifiers are called "tags" and can be specified for each RPC server via {@link RpcServerConfig#setTags(Set)} method. If one or
 * more tags are specified for an RPC server then RPC clients of that server must be constructed via
 * {@link RpcService#clientFor(Class, String)} method. The {@code tag} parameter of this method must match with one of the RPC server's
 * tags. If client doesn't specify a tag or if tag doesn't match any of the RPC server's tags then such client will not be able to discover
 * and communicate with that server.
 * </p>
 *
 * <a name="rpc_client"></a>
 * <h2>RPC Client</h2>
 * <p>
 * The client side of RPC communication is represented by a Java reflections proxy of an @{@link Rpc}-annotated interface. Such proxies can
 * be constructed on an RPC client node via {@link RpcService#clientFor(Class)} method. This method returns an instance of {@link
 * RpcClientBuilder} interface that provides support for dynamically configuring different aspects of a client proxy object (f.e. timeouts,
 * load balancing, retry policies, etc). The client proxy object can be created by calling the {@link RpcClientBuilder#build()} method as
 * in the example below:
 * ${source: rpc/RpcServiceJavadocTest.java#client}
 * </p>
 *
 * <p>
 * Instead of configuring all of the RPC client's options dynamically, it is possible to pre-configure some of those options by
 * registering an instance of the {@link RpcClientConfig} class for each such client individually
 * (see {@link RpcServiceFactory#setClients(List)} method).
 * </p>
 *
 * <p>
 * If such configuration is registered then an instance of {@link RpcClientBuilder}б that is returned from the {@link #clientFor(Class)}
 * method, will contain all of the pre-preconfigured options.
 * </p>
 *
 * <p>
 * For the complete list of pre-configurable options please see the documentation of {@link RpcClientConfig} class.
 * </p>
 *
 * <a name="routing_and_load_balancing"></a>
 * <h2>Routing and Load Balancing</h2>
 * <p>
 * Every RPC client proxy uses an instance of {@link RpcLoadBalancer} interface to perform routing of RPC unicast operations. Load balancer
 * can be pre-configured via the {@link RpcClientConfig#setLoadBalancer(RpcLoadBalancer)} method or specified dynamically via the {@link
 * RpcClientBuilder#withLoadBalancer(RpcLoadBalancer)} method. If load balancer is not specified then the RPC client will fall back to
 * the {@link DefaultRpcLoadBalancer}.
 * </p>
 * <p>
 * Note that load balancing does not get applied to RPC broadcast operations (i.e. @{@link RpcAggregate}-annotated methods). Such
 * operations are submitted to all nodes within the RPC client's cluster topology.
 * Please see the "<a href="#topology_filterring">Cluster topology filtering</a>" section for details of how to control the RPC client's
 * cluster topology.
 * </p>
 *
 * <h3>Consistent Routing</h3>
 * <p>
 * Applications can provide an affinity key to the {@link RpcLoadBalancer} so that it could perform consistent routing based on some
 * application-specific criteria. For example, if the {@link DefaultRpcLoadBalancer} is being used by the RPC client then it will
 * utilize the the {@link RendezvousHashMapper} algorithm make sure that all RPC operations with the same affinity key will always be
 * routed to the same cluster node (unless the cluster topology doesn't change). Custom implementations of the {@link RpcLoadBalancer}
 * interface can use their own algorithms for consistent routing.
 * </p>
 * <p>
 * Affinity key can be specified by annotating one of the RPC method's parameters with @{@link RpcAffinityKey}. If such such annotation is
 * present then RPC client will transparently use the value of that parameter as an {@link LoadBalancerContext#affinityKey() affinity key}
 * for the {@link RpcLoadBalancer}.
 * </p>
 * <p>
 * Note that there can be only one @{@link RpcAffinityKey}-annotated parameter per RPC method and value of that parameter can't be {@code
 * null}. Also it is important to make sure that type of that parameter provides consistent implementation of {@link Object#hashCode()} and
 * {@link Object#equals(Object)} methods.
 * </p>
 *
 * <h3>Thread Affinity</h3>
 * <p>
 * Besides providing a hint to the {@link RpcLoadBalancer}, specifying an {@link RpcAffinityKey} also instructs the RPC service to
 * process all RPC operations of the same affinity key on the same thread. This applies both to the server side and to the client side of
 * RPC interactions. Thus, if RPC method returns an instance of {@link CompletableFuture} then such future will be
 * {@link CompletableFuture#complete(Object) completed} on a thread that is mapped to the value of {@link RpcAffinityKey}.
 * </p>
 *
 * <a name="topology_filterring"></a>
 * <h3>Cluster Topology Filtering</h3>
 * <p>
 * Routing of RPC operations among the cluster nodes is based on the RPC client's cluster view. By default, it includes all of the cluster
 * nodes that have a compliant {@link RpcServerConfig RPC server}.
 * </p>
 *
 * <p>
 * It is possible to narrow down the list of client-visible nodes via the following methods of the {@link RpcClientBuilder} class:
 * </p>
 * <ul>
 * <li>{@link RpcClientBuilder#forRemotes() forRemotes()}</li>
 * <li>{@link RpcClientBuilder#forRole(String) forRole(String)}</li>
 * <li>{@link RpcClientBuilder#forProperty(String) forProperty(String)}</li>
 * <li>{@link RpcClientBuilder#forNode(ClusterNode) forNode(ClusterNode)}</li>
 * <li>{@link RpcClientBuilder#forOldest() forOldest()}</li>
 * <li>{@link RpcClientBuilder#forYoungest() forYoungest()}</li>
 * <li>...and other methods from the {@link ClusterFilterSupport} base interface</li>
 * </ul>
 *
 * <p>
 * If filtering rules are specified for an RPC client then all RPC operations of that client will be distributed only among those nodes
 * that do match the filtering criteria.
 * </p>
 *
 * <a name="retrying_on_error"></a>
 * <h2>Retrying on Error</h2>
 * <p>
 * RPC service provides support for specifying a retry behavior in case of a remote invocation error.
 * </p>
 *
 * <p>
 * This can be done by marking a method of an RPC interface with the {@link RpcRetry} annotation. If such annotation is present on a method
 * then all failed invocations of that method will be transparently retried based on the annotation attribute values.
 * </p>
 *
 * <p>
 * {@link RpcRetry}'s attributes provide support for specifying different parameters of retry behavior (like maximum attempts, delay
 * between retries, etc). It is also possible to configure default values of those attributes by registering an instance
 * of {@link GenericRetryConfigurer} interface in the RPC client's configuration via the
 * {@link RpcClientConfig#setRetryPolicy(GenericRetryConfigurer)} or at the RPC client construction time via the
 * {@link RpcClientBuilder#withRetryPolicy(GenericRetryConfigurer)} method.
 * </p>
 *
 * @see RpcServiceFactory
 */
@DefaultServiceFactory(RpcServiceFactory.class)
public interface RpcService extends Service {
    /**
     * Constructs a new RPC client proxy builder for the specified Java interface and tag.
     *
     * <p>
     * This method returns an instance of {@link RpcClientBuilder} that can be used to configure and {@link RpcClientBuilder#build() build}
     * a Java proxy object for the specified RPC interface that will redirect all method invocations to remote cluster nodes.
     * </p>
     *
     * <p>
     * <b>Note:</b> Some of the builder's options can be preconfigured via {@link RpcClientConfig} (see its javadocs for more details).
     * </p>
     *
     * @param type RPC interface (must be an @{@link Rpc}-annotated Java interface).
     * @param tag Tag (see {@link RpcServerConfig#setTags(Set)}).
     * @param <T> RPC interface.
     *
     * @return Builder.
     */
    <T> RpcClientBuilder<T> clientFor(Class<T> type, String tag);

    /**
     * Constructs a new RPC client proxy builder for the specified Java interface.
     *
     * <p>
     * This method returns an instance of {@link RpcClientBuilder} that can be used to configure and {@link RpcClientBuilder#build() build}
     * a Java proxy object for the specified RPC interface that will redirect all method invocations to remote cluster nodes.
     * </p>
     *
     * <p>
     * <b>Note:</b> Some of the builder's options can be preconfigured via {@link RpcClientConfig} (see its javadocs for more details).
     * </p>
     *
     * @param type RPC interface (must be an @{@link Rpc}-annotated Java interface).
     * @param <T> RPC interface.
     *
     * @return Builder.
     */
    <T> RpcClientBuilder<T> clientFor(Class<T> type);

    /**
     * Returns the cluster view that includes only those nodes that have a {@link #servers() server} for the specified
     * {@link Rpc}-annotated Java interface and tag.
     *
     * @param type {@link Rpc}-annotated Java interface.
     * @param tag Tag (see {@link RpcServerConfig#setTags(Set)}).
     *
     * @return Cluster view that includes only those node that have a {@link #servers() server} for the specified {@link Rpc}-annotated
     * Java interface and tag.
     */
    ClusterView clusterOf(Class<?> type, String tag);

    /**
     * Returns the cluster view that includes only those nodes that have a {@link #servers() server} for the specified
     * {@link Rpc}-annotated Java interface.
     *
     * @param type {@link Rpc}-annotated Java interface.
     *
     * @return Cluster view that includes only those node that have a {@link #servers() server} for the specified {@link Rpc}-annotated
     * Java interface.
     */
    ClusterView clusterOf(Class<?> type);

    /**
     * Returns an immutable list of all RPC servers registered on this node.
     *
     * @return Immutable list of RPC servers.
     *
     * @see RpcServiceFactory#setServers(List)
     */
    List<RpcServerInfo> servers();

    /**
     * Returns the size of a thread pool for handling NIO-based socket connections
     * (see {@link RpcServiceFactory#setNioThreads(int)}).
     *
     * @return Size of a thread pool for handling NIO-based socket connections.
     */
    int nioThreads();

    /**
     * Returns the worker thread pool size (see {@link RpcServiceFactory#setWorkerThreads(int)}).
     *
     * @return Worker thread pool size.
     */
    int workerThreads();

}
