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

import io.hekate.cluster.ClusterFilterSupport;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.cluster.ClusterService;
import io.hekate.codec.CodecFactory;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.DefaultServiceFactory;
import io.hekate.core.service.Service;
import io.hekate.failover.FailoverPolicy;
import io.hekate.messaging.broadcast.AggregateCallback;
import io.hekate.messaging.broadcast.AggregateResult;
import io.hekate.messaging.broadcast.BroadcastCallback;
import io.hekate.messaging.unicast.LoadBalancer;
import io.hekate.messaging.unicast.ResponseCallback;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.messaging.unicast.TooManyRoutesException;
import java.util.List;

/**
 * <span class="startHere">&laquo; start here</span>Main entry point to messaging API.
 *
 * <h2>Overview</h2>
 * <p>
 * Messaging service provides support for building message-oriented communications among {@link Hekate} nodes. Message exchange is based on
 * the concept of messaging channels. Messaging channels hide all the complexity of managing resources (like socket and threads) and
 * provide
 * high level API for implementing various messaging patterns.
 * </p>
 *
 * <h2>Accessing service</h2>
 * <p>
 * Messaging service can be accessed via {@link Hekate#messaging()} method as in the example below:
 * ${source: messaging/MessagingServiceJavadocTest.java#access}
 * </p>
 *
 * <h2>Channels</h2>
 * <p>
 * Messages exchange is based on the concept of channels. Channel is a communication unit that can act as a sender, as a receiver or
 * perform both of those roles simultaneously. Channels provide support for unicast messaging (node to node communication) and broadcast
 * messaging (node to many nodes communication). Note that unicast and broadcast in this context are NOT related to UDP (all communications
 * are TCP-based) and merely outline communication patterns.
 * </p>
 *
 * <h2>Configuring channels</h2>
 * <p>
 * Channels can be registered within the service via {@link MessagingServiceFactory#withChannel(MessagingChannelConfig)} method.
 * This methods accept an instance of {@link MessagingChannelConfig} class that specifies the channel configuration options.
 * </p>
 *
 * <p>
 * Below are the key configuration options:
 * </p>
 * <ul>
 * <li>
 * {@link MessagingChannelConfig#setName(String) Channel Name} - only channels with the same name can communicate with each other.
 * Note that channel name must be unique within each {@link MessagingService} instance and any attempt to register multiple channels with
 * the same name will result in an error.
 * </li>
 * <li>
 * {@link MessagingChannelConfig#setReceiver(MessageReceiver) Message Receiver} if channel should be able to receive messages from remote
 * nodes (i.e. act as a server)
 * </li>
 * <li>
 * {@link MessagingChannelConfig#setMessageCodec(CodecFactory) Message Codec} - for messages serialization/deserialization.
 * If not specified then the {@link HekateBootstrap#setDefaultCodec(CodecFactory) default codec} will be used.
 * </li>
 * <li>
 * {@link MessagingChannelConfig#setNioThreads(int) NIO} and {@link MessagingChannelConfig#setWorkerThreads(int) worker} thread pool
 * options
 * </li>
 * </ul>
 *
 * <p>
 * Below is the example of messaging channel configuration:
 * </p>
 *
 * <div class="tabs">
 * <ul>
 * <li><a href="#configure-java">Java</a></li>
 * <li><a href="#configure-xsd">Spring XSD</a></li>
 * <li><a href="#configure-bean">Spring bean</a></li>
 * </ul>
 * <div id="configure-java">
 * ${source: messaging/MessagingServiceJavadocTest.java#configure_channel}
 * </div>
 * <div id="configure-xsd">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: messaging/service-xsd.xml#example}
 * </div>
 * <div id="configure-bean">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: messaging/service-bean.xml#example}
 * </div>
 * </div>
 *
 * <p>
 * For more details about the available configuration options please see the documentation of {@link MessagingChannelConfig} class.
 * </p>
 *
 * <h2>Accessing channels</h2>
 * <p>
 * Once configured and registered, channels can be accessed via {@link MessagingService#channel(String)} method with the {@link
 * MessagingChannelConfig#setName(String) channel name} passed in as a parameter.
 * ${source: messaging/MessagingServiceJavadocTest.java#access_channel}
 * </p>
 *
 * <h2>Sending messages</h2>
 * <p>
 * {@link MessagingChannel} provides API for the following communication patterns:
 * </p>
 * <ul>
 * <li>
 * <a href="#request_response">Request/Response</a> - bidirectional node to node communication
 * </li>
 * <li>
 * <a href="#send_and_forget">Send and Forget</a> - unidirectional node to node communication
 * </li>
 * <li>
 * <a href="#aggregate">Aggregation</a> - bidirectional node to many nodes communication
 * </li>
 * <li>
 * <a href="#broadcast">Broadcasting</a> - bidirectional node to many nodes communication
 * </li>
 * </ul>
 *
 * <a name="request_response"></a>
 * <h3>Node-to-node: Request/Response</h3>
 * <p>
 * {@link MessagingChannel#request(Object)} can be used for bidirectional communications with remote nodes using the request-response
 * pattern:
 * ${source: messaging/MessagingServiceJavadocTest.java#unicast_request_sync}
 * </p>
 *
 * <p>
 * ... or using a completely asynchronous callback-based approach (see {@link MessagingChannel#request(Object, ResponseCallback)}):
 * ${source: messaging/MessagingServiceJavadocTest.java#unicast_request_async}
 * </p>
 *
 * <a name="send_and_forget"></a>
 * <h3>Node-to-node: Send and Forget</h3>
 * <p>
 * {@link MessagingChannel#send(Object)} provides support for unidirectional communications (i.e. when remote node doesn't need to send
 * any reply) using the send and forget approach:
 * ${source: messaging/MessagingServiceJavadocTest.java#unicast_send_sync}
 * </p>
 *
 * <p>
 * ... or using a completely asynchronous callback-based approach (see {@link MessagingChannel#send(Object, SendCallback)}):
 * ${source: messaging/MessagingServiceJavadocTest.java#unicast_send_async}
 * </p>
 *
 * <a name="aggregate"></a>
 * <h3>Node-to-many: Aggregation</h3>
 * <p>
 * {@link MessagingChannel#aggregate(Object)} can be used for bidirectional communications by submitting a message to multiple nodes and
 * gathering (aggregating) replies from those nodes. Results of such aggregation are represented by the {@link AggregateResult} interface.
 * This interface provides methods for analyzing responses from remote nodes and checking for possible failures.
 * </p>
 *
 * <p>
 * Below is the example of synchronous aggregation:
 * ${source: messaging/MessagingServiceJavadocTest.java#aggregate_sync}
 * </p>
 *
 * <p>
 * ... or using a completely asynchronous callback-based approach (see {@link MessagingChannel#aggregate(Object, AggregateCallback)}):
 * ${source: messaging/MessagingServiceJavadocTest.java#aggregate_async}
 * </p>
 *
 * <a name="broadcast"></a>
 * <h3>Node-to-many: Broadcasting</h3>
 * <p>
 * {@link MessagingChannel#broadcast(Object)} provides support for unidirectional broadcasting (i.e. when remote nodes do not need to send
 * a reply and no aggregation should take place) using the fire and forget approach.
 * </p>
 *
 * <p>
 * Below is the example of synchronous broadcast:
 * ${source: messaging/MessagingServiceJavadocTest.java#broadcast_sync}
 * </p>
 * <p>
 * ... or using a completely asynchronous callback-based approach (see {@link MessagingChannel#broadcast(Object, BroadcastCallback)}):
 * ${source: messaging/MessagingServiceJavadocTest.java#broadcast_async}
 * </p>
 *
 * <h2>Receiving messages</h2>
 * <p>
 * Messaging channel can process operation requests from remote nodes (i.e. act as a messaging server) by registering an instance of {@link
 * MessageReceiver} interface within the channel configuration. Such receiver will be notified on every message that was submitted by a
 * remote node via {@link MessagingChannel#send(Object) send(...)}/{@link MessagingChannel#request(Object) request(...)}/{@link
 * MessagingChannel#broadcast(Object) broadcast(...)}/{@link MessagingChannel#aggregate(Object) aggregate(...)} methods.
 * </p>
 *
 * <p>
 * Received messages are wrapped with an instance of {@link Message} interface. This interface provides methods for getting the message
 * payload and also sending back a response (in case of request-response communications).
 * </p>
 *
 * <p>
 * Below is the example of {@link MessageReceiver} implementation:
 * ${source: messaging/MessagingServiceJavadocTest.java#message_receiver}
 * </p>
 *
 * <p>
 * Receiver can be registered within the channel configuration via {@link MessagingChannelConfig#setReceiver(MessageReceiver)}
 * method. Note that only one receiver can be registered per each channel.
 * </p>
 *
 * <h2>Message routing and load balancing</h2>
 * <p>
 * {@link MessagingChannel} uses {@link ClusterService} to find remote nodes that are capable of receiving messages from this channel. By
 * default each channel sees all nodes who's messaging service is configured with a channel of the same {@link
 * MessagingChannelConfig#setName(String) name} and having a {@link MessagingChannelConfig#setReceiver(MessageReceiver) message receiver}
 * (i.e. capable to receive messages from remote nodes).
 * </p>
 *
 * <p>
 * By default all messaging operations will try to use all nodes that are visible to each particular channel instance as a messaging
 * destination. For example, if {@link MessagingChannel#aggregate(Object, AggregateCallback) aggregate(...)} method is called on a channel
 * instance that doesn't have any filtering applied, then all nodes in the cluster that have a {@link MessageReceiver} configured (for the
 * same channel name) will receive an aggregation request.
 * </p>
 *
 * <p>
 * It is possible to narrow down the list of nodes that are visible to a channel. This can be done by configuring an instance of {@link
 * ClusterNodeFilter}. Filter can be configured statically within the {@link MessagingChannelConfig#setClusterFilter(ClusterNodeFilter)
 * channel configuration} or {@link MessagingChannel#filter(ClusterNodeFilter) dynamically} for each {@link MessagingChannel} instance.
 * Key difference between static and dynamic filter is that static filter is always gets applied to the cluster topology before any other
 * dynamic filter and is always automatically applied when channel is {@link MessagingService#channel(String) obtained} from the {@link
 * MessagingService}.
 * </p>
 *
 * <p>
 * {@link MessagingChannel} extends the {@link ClusterFilterSupport} interface which provides a general purpose {@link
 * MessagingChannel#filter(ClusterNodeFilter)} method for dynamic filtering as well as several shortcut methods for frequently used cases:
 * </p>
 * <ul>
 * <li>{@link MessagingChannel#forRemotes()}</li>
 * <li>{@link MessagingChannel#forRole(String)}</li>
 * <li>{@link MessagingChannel#forProperty(String)}</li>
 * <li>{@link MessagingChannel#forNode(ClusterNode)}</li>
 * <li>{@link MessagingChannel#forOldest()}</li>
 * <li>{@link MessagingChannel#forYoungest()}</li>
 * <li>...{@link ClusterFilterSupport etc}</li>
 * </ul>
 *
 * <p>
 * <b>NOTICE:</b> For {@link MessagingChannel#send(Object, SendCallback) send(...)} and {@link MessagingChannel#request(Object,
 * ResponseCallback) request(...)} (unicast operations) it is important to make sure that there is no uncertainty in which node should
 * receive a message. If there are multiple receivers visible to a {@link MessagingChannel} instance then unicast operation will fail with
 * {@link TooManyRoutesException}. Besides channel topology filtering it is possible to
 * {@link MessagingChannel#withLoadBalancer(LoadBalancer) specify} an instance of {@link LoadBalancer} interface that can apply
 * additional rules on which node should be used as a destination for each particular messaging operation.
 * </p>
 *
 * <h2>Thread pooling</h2>
 * <p>
 * Messaging service manages a pool of threads for each of its registered channels. The following  threads pools are managed:
 * </p>
 *
 * <ul>
 * <li>
 * <b>NIO thread pool</b> - thread pool for managing NIO socket channels. The size of this thread pool is controlled by the {@link
 * MessagingChannelConfig#setNioThreads(int)} configuration option.
 * </li>
 * <li>
 * <b>Worker thread pool</b> - Optional thread pool to offload messages processing work from NIO threads. The size of this pool is
 * controlled by the {@link MessagingChannelConfig#setWorkerThreads(int)} configuration option. It is recommended to set this parameter in
 * those cases where message processing is a heavy operation that can block NIO thread for a long time.
 * </li>
 * </ul>
 *
 * <p>
 * Below is the example of how those options can be configured for a messaging channel.
 * </p>
 * <div class="tabs">
 * <ul>
 * <li><a href="#channel-opts-java">Java</a></li>
 * <li><a href="#channel-opts-xsd">Spring XSD</a></li>
 * <li><a href="#channel-opts-bean">Spring bean</a></li>
 * </ul>
 * <div id="channel-opts-java">
 * ${source: messaging/MessagingServiceJavadocTest.java#channel_options}
 * </div>
 * <div id="channel-opts-xsd">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: messaging/channel-opts-xsd.xml#example}
 * </div>
 * <div id="channel-opts-bean">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: messaging/channel-opts-bean.xml#example}
 * </div>
 * </div>
 *
 * <h2>Messaging failover</h2>
 * <p>
 * Failover of messaging errors is controlled by the {@link FailoverPolicy} interface. Implementations of this interface can be
 * configured for each {@link MessagingChannel} individually via {@link MessagingChannelConfig#withFailoverPolicy(FailoverPolicy)} method.
 * In case of a messaging error this interface will be called by a channel in order to decided on whether another attempt should be
 * performed or operation should fail.
 * </p>
 *
 * <p>
 * For more details and usage examples please see the documentation of {@link FailoverPolicy} interface.
 * </p>
 */
@DefaultServiceFactory(MessagingServiceFactory.class)
public interface MessagingService extends Service {
    /**
     * Returns all channels that are {@link MessagingServiceFactory#setChannels(List) registered} within this service.
     *
     * @return Channels or an empty list if there are no registered channels.
     */
    List<MessagingChannel<?>> allChannels();

    /**
     * Returns a messaging channel for the specified name.
     *
     * @param channelName Channel name (see {@link MessagingChannelConfig#setName(String)}).
     * @param <T> Base type of messages that can be supported by the messaging channels.
     *
     * @return Unicast channel.
     *
     * @throws IllegalArgumentException if there is no such channel configuration with the specified name.
     * @see MessagingServiceFactory#setChannels(List)
     */
    <T> MessagingChannel<T> channel(String channelName) throws IllegalArgumentException;

    /**
     * Returns {@code true} if this service has a messaging channel with the specified name.
     *
     * @param channelName Channel name (see {@link MessagingChannelConfig#setName(String)}).
     *
     * @return {@code true} if messaging channel exists.
     */
    boolean hasChannel(String channelName);
}
