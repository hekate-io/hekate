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

package io.hekate.messaging;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.codec.Codec;
import io.hekate.codec.CodecFactory;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.messaging.intercept.MessageInterceptor;
import io.hekate.messaging.loadbalance.LoadBalancer;
import io.hekate.messaging.operation.Aggregate;
import io.hekate.messaging.operation.AggregateRetryConfigurer;
import io.hekate.messaging.operation.Broadcast;
import io.hekate.messaging.operation.BroadcastRetryConfigurer;
import io.hekate.messaging.operation.Request;
import io.hekate.messaging.operation.RequestRetryConfigurer;
import io.hekate.messaging.operation.Send;
import io.hekate.messaging.operation.SendRetryConfigurer;
import io.hekate.messaging.retry.GenericRetryConfigurer;
import io.hekate.partition.Partition;
import io.hekate.partition.RendezvousHashMapper;
import io.hekate.util.format.ToString;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration options for a {@link MessagingChannel}.
 *
 * <p>
 * Instances of this class are strongly recommended to be constructed via the {@link #MessagingChannelConfig(Class)} constructor in order
 * to provide the type safety of messaging operations. {@link #MessagingChannelConfig() Default} (no-arg) constructor of this class is
 * provide for reflections-based instantiations by IoC frameworks only.
 * </p>
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
public class MessagingChannelConfig<T> extends MessagingConfigBase<MessagingChannelConfig<T>> {
    /** See {@link #MessagingChannelConfig(Class)}. */
    private final Class<T> baseType;

    /** See {@link #setName(String)}. */
    private String name;

    /** See {@link #setWorkerThreads(int)}. */
    private int workerThreads;

    /** See {@link #setPartitions(int)}. */
    private int partitions = RendezvousHashMapper.DEFAULT_PARTITIONS;

    /** See {@link #setBackupNodes(int)}. */
    private int backupNodes;

    /** See {@link #setMessageCodec(CodecFactory)}. */
    private CodecFactory<T> messageCodec;

    /** See {@link #setClusterFilter(ClusterNodeFilter)}. */
    private ClusterNodeFilter clusterFilter;

    /** See {@link #setReceiver(MessageReceiver)}. */
    private MessageReceiver<T> receiver;

    /** See {@link #setRetryPolicy(GenericRetryConfigurer)}. */
    private GenericRetryConfigurer retryPolicy;

    /** See {@link #setLoadBalancer(LoadBalancer)}. */
    private LoadBalancer<T> loadBalancer;

    /** See {@link #setInterceptors(List)}. */
    private List<MessageInterceptor> interceptors;

    /** See {@link #setMessagingTimeout(long)}. */
    private long messagingTimeout;

    /** See {@link #setLogCategory(String)}. */
    private String logCategory;

    /** See {@link #setWarnOnRetry(int)}. */
    private int warnOnRetry = -1;

    /**
     * Unsafe default constructor that should be used only for reflections-based instantiation by IoC frameworks. For programmatic
     * construction the {@link #MessagingChannelConfig(Class)} constructor must be used instead of this one.
     *
     * <p>
     * Instances that are constructed via this constructor will have their {@link #getBaseType() base type} set to {@link Object}.
     * </p>
     *
     * @see #MessagingChannelConfig(Class)
     * @deprecated Not really deprecated, but set so in order to produce warnings when this constructor is used instead of the {@link
     * #MessagingChannelConfig(Class) recommended} one.
     */
    @Deprecated
    public MessagingChannelConfig() {
        baseType = uncheckedObjectType();
    }

    /**
     * Type safe constructor.
     *
     * <p>
     * This constructor sets the base type for messages that can be transferred over this channel. If an attempt is made to transfer a
     * message who's type is not compatible with the specified one then an error will
     * </p>
     *
     * <p>
     * <b>Important:</b> the specified base type must be compatible with the {@link Codec#baseType() base type} of the
     * {@link #setMessageCodec(CodecFactory) message codec} of this channel and must be the same across all cluster nodes.
     * </p>
     *
     * @param baseType Base type of messages that can be transferred over this channel.
     */
    public MessagingChannelConfig(Class<T> baseType) {
        ArgAssert.notNull(baseType, "base type");

        this.baseType = baseType;
    }

    /**
     * Shortcut method for {@link #MessagingChannelConfig(Class)} constructor.
     *
     * @param baseType Base type of messages that can be transferred over this channel.
     * @param <T> Base type of messages that can be transferred over this channel.
     *
     * @return New instance.
     */
    public static <T> MessagingChannelConfig<T> of(Class<T> baseType) {
        return new MessagingChannelConfig<>(baseType);
    }

    /**
     * Shortcut method for {@link #MessagingChannelConfig(Class)} constructor to produce polyglot channels with {@link Object} base type.
     *
     * @return New instance.
     *
     * @see #MessagingChannelConfig(Class)
     */
    public static MessagingChannelConfig<Object> unchecked() {
        return new MessagingChannelConfig<>(Object.class);
    }

    /**
     * Returns the base type of messages that can be transferred over this channel (see {@link #MessagingChannelConfig(Class)}).
     *
     * @return Base type of messages that can be transferred over this channel.
     */
    public Class<T> getBaseType() {
        return baseType;
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
     * Sets the channel name. Can contain only alpha-numeric characters and non-repeatable dots/hyphens.
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
     * @param name Unique channel name (can contain only alpha-numeric characters and non-repeatable dots/hyphens).
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
     * Returns the total amount of partitions that should be managed by the channel's {@link MessagingChannel#partitions() partition mapper}
     * (see {@link #setPartitions(int)}).
     *
     * @return Total amount of partitions.
     */
    public int getPartitions() {
        return partitions;
    }

    /**
     * Sets the total amount of partitions that should be managed by the channel's {@link MessagingChannel#partitions() partition mapper}.
     *
     * <p>
     * Value of this parameter must be above zero and must be a power of two.
     * Default value is specified by {@link RendezvousHashMapper#DEFAULT_PARTITIONS}.
     * </p>
     *
     * @param partitions Total amount of partitions that should be managed by the channel's partition mapper (value must be a power of
     * two).
     */
    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    /**
     * Fluent-style version of {@link #setPartitions(int)}.
     *
     * @param partitions Total amount of partitions that should be managed by the channel's partition mapper (value must be a power of
     * two).
     *
     * @return This instance.
     */
    public MessagingChannelConfig<T> withPartitions(int partitions) {
        setPartitions(partitions);

        return this;
    }

    /**
     * Returns the amount of backup nodes that should be assigned to each partition by the the channel's
     * {@link MessagingChannel#partitions() partition mapper} (see {@link #setBackupNodes(int)}).
     *
     * @return Amount of backup nodes.
     */
    public int getBackupNodes() {
        return backupNodes;
    }

    /**
     * Sets the amount of backup nodes that should be assigned to each partition by the the channel's
     * {@link MessagingChannel#partitions() partition mapper}.
     *
     * <p>
     * If value of this parameter is zero then the channel's mapper will not manage {@link Partition#backupNodes() backup nodes}. If value
     * of this parameter is negative then all available cluster nodes will be used as {@link Partition#backupNodes() backup nodes}.
     * </p>
     *
     * <p>
     * Default value of this parameter is 0 (i.e. backup nodes management is disabled).
     * </p>
     *
     * @param backupNodes Amount of backup nodes that should be assigned to each partition of the channel's
     * {@link MessagingChannel#partitions() partition mapper}.
     */
    public void setBackupNodes(int backupNodes) {
        this.backupNodes = backupNodes;
    }

    /**
     * Fluent-style version of {@link #setBackupNodes(int)}.
     *
     * @param backupNodes Amount of backup nodes that should be assigned to each partition of the channel's
     * {@link MessagingChannel#partitions() partition mapper}.
     *
     * @return This instance.
     */
    public MessagingChannelConfig<T> withBackupNodes(int backupNodes) {
        setBackupNodes(backupNodes);

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
     * Returns the message receiver that should be used to handle incoming messages (see {@link #setReceiver(MessageReceiver)}).
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
     * Returns {@code true} if {@link #setReceiver(MessageReceiver) receiver} is specified.
     *
     * @return {@code true} if {@link #setReceiver(MessageReceiver) receiver} is specified.
     */
    public boolean hasReceiver() {
        return receiver != null;
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
     * those nodes that have some specific {@link ClusterNode#property(String) property} or {@link ClusterNode#roles() role}). Only
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
     * Returns the generic retry policy to be applied to all messaging operations of this channel.
     *
     * @return Generic retry policy to be applied to all messaging operations of this channel.
     */
    public GenericRetryConfigurer getRetryPolicy() {
        return retryPolicy;
    }

    /**
     * Sets the generic retry policy to be applied to all messaging operations of this channel.
     *
     * <p>
     * This policy can be overridden for each individual operation via the following methods:
     * </p>
     * <ul>
     * <li>{@link Request#withRetry(RequestRetryConfigurer)}</li>
     * <li>{@link Send#withRetry(SendRetryConfigurer)}</li>
     * <li>{@link Broadcast#withRetry(BroadcastRetryConfigurer)}</li>
     * <li>{@link Aggregate#withRetry(AggregateRetryConfigurer)}</li>
     * </ul>
     *
     * @param retryPolicy Retry policy.
     */
    public void setRetryPolicy(GenericRetryConfigurer retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

    /**
     * Fluent-style version of {@link #setRetryPolicy(GenericRetryConfigurer)}.
     *
     * @param retryPolicy Retry policy.
     *
     * @return This instance.
     */
    public MessagingChannelConfig<T> withRetryPolicy(GenericRetryConfigurer retryPolicy) {
        setRetryPolicy(retryPolicy);

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
     * Returns a list of message interceptors that should be used by the channel (see {@link #setInterceptors(List)}).
     *
     * @return Message interceptors.
     */
    public List<MessageInterceptor> getInterceptors() {
        return interceptors;
    }

    /**
     * Sets the list of message interceptors that should be used by the channel.
     *
     * <p>
     * Interceptors can be registered to the messaging channel in order to track message flow or to apply transformations to messages.
     * </p>
     *
     * @param interceptors Message interceptors.
     */
    public void setInterceptors(List<MessageInterceptor> interceptors) {
        this.interceptors = interceptors;
    }

    /**
     * Fluent-style version of {@link #setInterceptors(List)}.
     *
     * @param interceptor Message interceptor.
     *
     * @return This instance.
     */
    public MessagingChannelConfig<T> withInterceptor(MessageInterceptor interceptor) {
        if (getInterceptors() == null) {
            setInterceptors(new ArrayList<>());
        }

        getInterceptors().add(interceptor);

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
     * Sets the timeout in milliseconds that should be applied to all messaging operations of this channel.
     *
     * <p>
     * If particular messaging operation can't be completed within the specified timeout then such operation will fail with
     * {@link MessageTimeoutException}.
     * </p>
     *
     * <p>
     * If value of this parameter is less than or equals to zero then timeouts will not be applied to messaging operations. Default value
     * of this parameter is 0.
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

    /**
     * Returns the amount of retry attempts to skip before logging a warn message (see {@link #setWarnOnRetry(int)}).
     *
     * @return Amount of retry attempts to skip before logging a warn message.
     */
    public int getWarnOnRetry() {
        return warnOnRetry;
    }

    /**
     * Sets the amount of retry attempts to skip before logging a warn message.
     *
     * <ul>
     * <li>0 - log every attempt</li>
     * <li>positive N - log every N'th attempt</li>
     * <li>negative - do not log warning on retries</li>
     * </ul>
     *
     * <p>
     * By default this property is set to a negative value (i.e. retry warnings are disabled).
     * </p>
     *
     * @param warnOnRetry How many attempts to skip before logging a warn message.
     */
    public void setWarnOnRetry(int warnOnRetry) {
        this.warnOnRetry = warnOnRetry;
    }

    /**
     * Fluent-style version of {@link #setWarnOnRetry(int)}.
     *
     * @param warnOnRetry How many attempts to skip before logging a warn message.
     *
     * @return This instance.
     */
    public MessagingChannelConfig<T> withWarnOnRetry(int warnOnRetry) {
        setWarnOnRetry(warnOnRetry);

        return this;
    }

    @SuppressWarnings("unchecked")
    private static <T> Class<T> uncheckedObjectType() {
        return (Class<T>)Object.class;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
