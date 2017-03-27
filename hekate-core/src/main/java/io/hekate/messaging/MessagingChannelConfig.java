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

package io.hekate.messaging;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.codec.CodecFactory;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.failover.FailoverPolicy;
import io.hekate.messaging.unicast.LoadBalancer;
import io.hekate.network.NetworkService;
import io.hekate.network.NetworkServiceFactory;
import io.hekate.util.format.ToString;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Configuration options for a {@link MessagingChannel}.
 *
 * <p>
 * For configuration options please see the documentation of setter-methods defined in this class.
 * </p>
 *
 * <p>
 * For generic information about messaging and channels please see the documentation of {@link MessagingService}.
 * </p>
 *
 * @param <T> Base class of messages that can be handled by the channel.
 *
 * @see MessagingServiceFactory#setChannels(List)
 */
public class MessagingChannelConfig<T> {
    /** Default value (={@value}) for {@link #setSockets(int)}. */
    public static final int DEFAULT_SOCKETS = 1;

    private String name;

    private int workerThreads;

    private int sockets = DEFAULT_SOCKETS;

    private int nioThreads;

    private long idleTimeout;

    private CodecFactory<T> messageCodec;

    private ClusterNodeFilter clusterFilter;

    private MessageReceiver<T> receiver;

    private FailoverPolicy failoverPolicy;

    private LoadBalancer<T> loadBalancer;

    private long messagingTimeout;

    private MessagingBackPressureConfig backPressure = new MessagingBackPressureConfig();

    private String logCategory;

    /**
     * Default constructor.
     */
    public MessagingChannelConfig() {
        // No-op.
    }

    /**
     * Constructs new instance.
     *
     * @param name Channel name (see {@link #setName(String)}).
     */
    public MessagingChannelConfig(String name) {
        this.name = name;
    }

    /**
     * Returns the channel name (see {@link #setName(String)}).
     *
     * @return Channel name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the channel name.
     *
     * <p>
     * Each channel must have a unique name within the {@link MessagingService} instance. Messages that were sent by the particular channel
     * can be handled only by a channel with the same name on the remote node.
     * </p>
     *
     * <p>
     * This name can be used to obtain channel by its name via {@link MessagingService#channel(String)}.
     * </p>
     *
     * <p>
     * This parameter is mandatory and doesn't have a default value.
     * </p>
     *
     * @param name Unique channel name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Fluent-style version of {@link #setName(String)}.
     *
     * @param name Unique channel name.
     *
     * @return This instance.
     */
    public MessagingChannelConfig<T> withName(String name) {
        setName(name);

        return this;
    }

    /**
     * Returns the idle socket timeout in milliseconds (see {@link #setIdleTimeout(long)}).
     *
     * @return Idle socket timeout in milliseconds.
     */
    public long getIdleTimeout() {
        return idleTimeout;
    }

    /**
     * Sets idle socket timeout in milliseconds.
     *
     * <p>
     * If there were no communication with some remote node for the duration of this time interval then all sockets connections with such
     * node will be closed in order to save system resource. Connections will be automatically reestablish on the next attempt to send a
     * message to that node.
     * </p>
     *
     * <p>
     * If value of this parameter is less than or equals to zero (default value) then connections will not be closed while remote node stays
     * alive.
     * </p>
     *
     * @param idleTimeout Timeout in milliseconds.
     */
    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    /**
     * Fluent-style version of {@link #setIdleTimeout(long)}.
     *
     * @param idleTimeout Timeout in milliseconds.
     *
     * @return This instance.
     */
    public MessagingChannelConfig<T> withIdleTimeout(long idleTimeout) {
        setIdleTimeout(idleTimeout);

        return this;
    }

    /**
     * Returns the codec factory (see {@link #setMessageCodec(CodecFactory)}).
     *
     * @return Codec factory.
     */
    public CodecFactory<T> getMessageCodec() {
        return messageCodec;
    }

    /**
     * Sets the codec factory to be used for messages serialization/deserialization by this channel.
     *
     * <p>
     * If not specified then the default general-purpose codec will be used (see
     * {@link HekateBootstrap#setDefaultCodec(CodecFactory)}).
     * </p>
     *
     * @param messageCodec Codec factory.
     */
    public void setMessageCodec(CodecFactory<T> messageCodec) {
        this.messageCodec = messageCodec;
    }

    /**
     * Fluent-style version of {@link #setMessageCodec(CodecFactory)}.
     *
     * @param codecFactory Codec factory.
     *
     * @return This instance.
     */
    public MessagingChannelConfig<T> withMessageCodec(CodecFactory<T> codecFactory) {
        setMessageCodec(codecFactory);

        return this;
    }

    /**
     * Returns the worker thread pool size (see {@link #setWorkerThreads(int)}).
     *
     * @return Worker thread pool size.
     */
    public int getWorkerThreads() {
        return workerThreads;
    }

    /**
     * Sets the worker thread pool size.
     *
     * <p>
     * Setting this parameter to a positive value instructs the channel to create a dedicated thread pool for messages handling and
     * callbacks notification. If this parameter is negative or zero (default value) then messages handling and callbacks notification will
     * be performed on the NIO thread. It is recommended to use the dedicated thread pool in case of long and heavy message processing
     * operations.
     * </p>
     *
     * @param workerThreads Worker thread pool size.
     */
    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    /**
     * Fluent-style version of {@link #setWorkerThreads(int)}.
     *
     * @param workerThreads Worker thread pool size.
     *
     * @return This instance.
     */
    public MessagingChannelConfig<T> withWorkerThreads(int workerThreads) {
        setWorkerThreads(workerThreads);

        return this;
    }

    /**
     * Returns the socket pool size (see {@link #setSockets(int)}).
     *
     * @return Socket pool size.
     */
    public int getSockets() {
        return sockets;
    }

    /**
     * Sets the size of a socket pool that should be created by this channel per each remote node that this channel will communicate with.
     *
     * <p>
     * Value of this parameter must be greater than zero. Default value is {@value #DEFAULT_SOCKETS}.
     * </p>
     *
     * @param sockets Socket pool size.
     */
    public void setSockets(int sockets) {
        this.sockets = sockets;
    }

    /**
     * Fluent-style version of {@link #setSockets(int)}.
     *
     * @param sockets Socket pool size.
     *
     * @return This instance.
     */
    public MessagingChannelConfig<T> withSockets(int sockets) {
        setSockets(sockets);

        return this;
    }

    /**
     * Returns the size of a thread pool for handling NIO-based socket connections (see {@link #setNioThreads(int)}).
     *
     * @return Size of a thread pool for handling NIO-based socket connections.
     */
    public int getNioThreads() {
        return nioThreads;
    }

    /**
     * Sets the size of a thread pool for handling NIO-based socket connections.
     *
     * <p>
     * If this parameter is less than or equals to zero (default value) then this channel will use the core thread pool of
     * {@link NetworkService} (see {@link NetworkServiceFactory#setNioThreads(int)}).
     * </p>
     *
     * @param nioThreads Size of a thread pool for handling NIO-based socket connections.
     */
    public void setNioThreads(int nioThreads) {
        this.nioThreads = nioThreads;
    }

    /**
     * Fluent-style version of {@link #setNioThreads(int)}.
     *
     * @param nioThreads Size of a thread pool for handling NIO-based socket connections.
     *
     * @return This instance.
     */
    public MessagingChannelConfig<T> withNioThreads(int nioThreads) {
        setNioThreads(nioThreads);

        return this;
    }

    /**
     * Returns the message receiver that should be used to handle incoming messages (see {@link
     * #setReceiver(MessageReceiver)}).
     *
     * @return Message receiver.
     */
    public MessageReceiver<T> getReceiver() {
        return receiver;
    }

    /**
     * Sets the message receiver that should be used to handle incoming messages.
     *
     * <p>
     * This parameter is optional and if not configured then channel will act in a client-only mode (i.e. it will be able to issue requests
     * to remote nodes but will not be able to process request from remote nodes).
     * </p>
     *
     * <p>
     * For more details about channels and messages handling please see the documentation of {@link MessagingService} interface.
     * </p>
     *
     * @param receiver Message receiver.
     */
    public void setReceiver(MessageReceiver<T> receiver) {
        this.receiver = receiver;
    }

    /**
     * Fluent style version of {@link #setReceiver(MessageReceiver)}.
     *
     * @param messageReceiver Message receiver.
     *
     * @return This instance.
     */
    public MessagingChannelConfig<T> withReceiver(MessageReceiver<T> messageReceiver) {
        setReceiver(messageReceiver);

        return this;
    }

    /**
     * Returns the cluster node filter for this channel (see {@link #setClusterFilter(ClusterNodeFilter)}).
     *
     * @return Cluster node filter.
     */
    public ClusterNodeFilter getClusterFilter() {
        return clusterFilter;
    }

    /**
     * Sets the cluster node filter that should be used to filter nodes during the the message sending.
     *
     * <p>
     * Filtering can be used to limit communications between nodes based on some custom criteria (f.e. enable communications only between
     * those nodes that have some specific {@link ClusterNode#getProperty(String) property} or {@link ClusterNode#getRoles() role}). Only
     * those nodes that match the specified filter will be included into the messaging channel's cluster topology.
     * </p>
     *
     * <p>
     * This parameter is optional.
     * </p>
     *
     * @param clusterFilter Cluster node filter.
     */
    public void setClusterFilter(ClusterNodeFilter clusterFilter) {
        this.clusterFilter = clusterFilter;
    }

    /**
     * Fluent-style version of {@link #setClusterFilter(ClusterNodeFilter)}.
     *
     * @param clusterFilter Cluster node filter.
     *
     * @return This instance.
     */
    public MessagingChannelConfig<T> withClusterFilter(ClusterNodeFilter clusterFilter) {
        setClusterFilter(clusterFilter);

        return this;
    }

    /**
     * Returns the default failover policy that should be used by the channel (see {@link #setFailoverPolicy(FailoverPolicy)}).
     *
     * @return Default failover policy of this channel.
     */
    public FailoverPolicy getFailoverPolicy() {
        return failoverPolicy;
    }

    /**
     * Sets the default failover policy that should be used by the {@link MessagingChannel}.
     *
     * <p>
     * Failover policy can be overridden dynamically via {@link MessagingChannel#withFailover(FailoverPolicy)} method.
     * </p>
     *
     * @param failoverPolicy Default failover policy.
     */
    public void setFailoverPolicy(FailoverPolicy failoverPolicy) {
        this.failoverPolicy = failoverPolicy;
    }

    /**
     * Fluent-style version of {@link #setFailoverPolicy(FailoverPolicy)}.
     *
     * @param failoverPolicy Default failover policy.
     *
     * @return This instance.
     */
    public MessagingChannelConfig<T> withFailoverPolicy(FailoverPolicy failoverPolicy) {
        setFailoverPolicy(failoverPolicy);

        return this;
    }

    /**
     * Returns the unicast load balancer that should be used by the channel (see {@link #setLoadBalancer(LoadBalancer)}).
     *
     * @return Unicast load balancer.
     */
    public LoadBalancer<T> getLoadBalancer() {
        return loadBalancer;
    }

    /**
     * Sets the load balancer that should be used by the {@link MessagingChannel}.
     *
     * <p>
     * <b>Note:</b> it is also possible to set load balancer dynamically via {@link MessagingChannel#withLoadBalancer(LoadBalancer)}.
     * </p>
     *
     * @param loadBalancer Unicast load balancer.
     */
    public void setLoadBalancer(LoadBalancer<T> loadBalancer) {
        this.loadBalancer = loadBalancer;
    }

    /**
     * Fluent-style version of {@link #setLoadBalancer(LoadBalancer)}.
     *
     * @param loadBalancer Unicast load balancer.
     *
     * @return This instance.
     */
    public MessagingChannelConfig<T> withLoadBalancer(LoadBalancer<T> loadBalancer) {
        setLoadBalancer(loadBalancer);

        return this;
    }

    /**
     * Returns the back pressure configuration (see {@link #setBackPressure(MessagingBackPressureConfig)}).
     *
     * @return Back pressure configuration.
     */
    public MessagingBackPressureConfig getBackPressure() {
        return backPressure;
    }

    /**
     * Sets the back pressure configuration.
     *
     * <p>
     * If not specified then {@link MessagingBackPressureConfig}'s defaults will be used.
     * </p>
     *
     * @param backPressure Back pressure configuration.
     */
    public void setBackPressure(MessagingBackPressureConfig backPressure) {
        ArgAssert.notNull(backPressure, "Back pressure configuration");

        this.backPressure = backPressure;
    }

    /**
     * Fluent-style version of {@link #setBackPressure(MessagingBackPressureConfig)}.
     *
     * @param backPressure Back pressure configuration.
     *
     * @return This instance.
     */
    public MessagingChannelConfig<T> withBackPressure(MessagingBackPressureConfig backPressure) {
        setBackPressure(backPressure);

        return this;
    }

    /**
     * Applies the specified consumer to the current {@link #getBackPressure()} configuration.
     *
     * @param configurer Configuration Consumer.
     *
     * @return This instance.
     */
    public MessagingChannelConfig<T> withBackPressure(Consumer<MessagingBackPressureConfig> configurer) {
        configurer.accept(getBackPressure());

        return this;
    }

    /**
     * Returns the timeout in milliseconds that should be applied to all messaging operations within this channel (see {@link
     * #setMessagingTimeout(long)}).
     *
     * @return Timeout in milliseconds.
     */
    public long getMessagingTimeout() {
        return messagingTimeout;
    }

    /**
     * Sets the timeout in milliseconds that should be applied to all messaging operations within this channel.
     *
     * <p>
     * If particular messaging operation (f.e. {@link MessagingChannel#request(Object) request(...)}
     * or {@link MessagingChannel#send(Object) send}) can't be completed within the specified timeout then such operation will fail with
     * {@link MessagingTimeoutException}.
     * </p>
     *
     * <p>
     * If value of this parameter is less than or equals to zero then timeouts will not be applied to messaging operations. Default value
     * of this parameter is 0.
     * </p>
     *
     * <p>
     * <b>Note:</b> it is possible to dynamically specify timeout via {@link MessagingChannel#withTimeout(long, TimeUnit)}.
     * </p>
     *
     * @param messagingTimeout Timeout in milliseconds.
     */
    public void setMessagingTimeout(long messagingTimeout) {
        this.messagingTimeout = messagingTimeout;
    }

    /**
     * Fluent-style version of {@link #setMessagingTimeout(long)}.
     *
     * @param messagingTimeout Timeout in milliseconds.
     *
     * @return This instance.
     */
    public MessagingChannelConfig<T> withMessagingTimeout(long messagingTimeout) {
        setMessagingTimeout(messagingTimeout);

        return this;
    }

    /**
     * Returns the logging category that should be used by the channel (see {@link #setLogCategory(String)}).
     *
     * @return Logging category.
     */
    public String getLogCategory() {
        return logCategory;
    }

    /**
     * Sets the logging category that should be used by the channel.
     *
     * @param logCategory Logging category.
     */
    public void setLogCategory(String logCategory) {
        this.logCategory = logCategory;
    }

    /**
     * Fluent-style version of {@link #setLogCategory(String)}.
     *
     * @param logCategory Logging category.
     *
     * @return This instance.
     */
    public MessagingChannelConfig<T> withLogCategory(String logCategory) {
        setLogCategory(logCategory);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
