/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterView;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecService;
import io.hekate.codec.ThreadLocalCodecFactory;
import io.hekate.core.Hekate;
import io.hekate.core.internal.util.StreamUtils;
import io.hekate.core.internal.util.Utils;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessagingBackPressureConfig;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingChannelId;
import io.hekate.messaging.MessagingOverflowPolicy;
import io.hekate.messaging.broadcast.AggregateCallback;
import io.hekate.messaging.broadcast.AggregateFuture;
import io.hekate.messaging.broadcast.BroadcastCallback;
import io.hekate.messaging.broadcast.BroadcastFuture;
import io.hekate.messaging.intercept.MessageInterceptor;
import io.hekate.messaging.loadbalance.DefaultLoadBalancer;
import io.hekate.messaging.loadbalance.LoadBalancer;
import io.hekate.messaging.unicast.ResponseCallback;
import io.hekate.messaging.unicast.ResponseFuture;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.messaging.unicast.SendFuture;
import io.hekate.messaging.unicast.SubscribeFuture;
import io.hekate.partition.RendezvousHashMapper;
import io.hekate.util.format.ToStringIgnore;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;

class MessagingGateway<T> {
    private final String name;

    private final Class<T> baseType;

    private final int nioThreads;

    private final int workerThreads;

    private final long idleTimeout;

    private final int partitions;

    private final int backupNodes;

    private final SendPressureGuard sendPressure;

    private final ReceivePressureGuard receivePressure;

    private final CodecFactory<T> codecFactory;

    @ToStringIgnore
    private final MessageReceiver<T> unguardedReceiver;

    @ToStringIgnore
    private final InterceptorManager<T> interceptor;

    @ToStringIgnore
    private final DefaultMessagingChannel<T> rootChannel;

    @ToStringIgnore
    private final Logger log;

    @ToStringIgnore
    private final String logCategory;

    @ToStringIgnore
    private final Hekate hekate;

    @ToStringIgnore
    private volatile MessagingGatewayContext<T> ctx;

    public MessagingGateway(
        MessagingChannelConfig<T> cfg,
        Hekate hekate,
        ClusterService cluster,
        CodecService codec,
        List<MessageInterceptor<?>> interceptors
    ) {
        assert cfg != null : "Messaging channel configuration is null.";
        assert hekate != null : "Hekate instance is null.";
        assert cluster != null : "Cluster service is null.";
        assert codec != null : "Codec service is null.";

        this.hekate = hekate;
        this.name = Utils.nullOrTrim(cfg.getName());
        this.baseType = cfg.getBaseType();
        this.nioThreads = cfg.getNioThreads();
        this.workerThreads = cfg.getWorkerThreads();
        this.idleTimeout = cfg.getIdleSocketTimeout();
        this.unguardedReceiver = cfg.getReceiver();
        this.partitions = cfg.getPartitions();
        this.backupNodes = cfg.getBackupNodes();

        // Interceptors.
        this.interceptor = new InterceptorManager<>(
            Stream.concat(
                StreamUtils.nullSafe(interceptors),
                StreamUtils.nullSafe(cfg.getInterceptors())
            ).collect(toList())
        );

        // Codec.
        this.codecFactory = optimizeCodecFactory(cfg.getMessageCodec(), codec);

        // Logger.
        this.logCategory = resolveLogCategory(cfg.getLogCategory());

        this.log = LoggerFactory.getLogger(logCategory);

        // Prepare back pressure guards.
        MessagingBackPressureConfig pressureCfg = cfg.getBackPressure();

        if (pressureCfg != null) {
            int inHiWatermark = pressureCfg.getInHighWatermark();
            int inLoWatermark = pressureCfg.getInLowWatermark();
            int outHiWatermark = pressureCfg.getOutHighWatermark();
            int outLoWatermark = pressureCfg.getOutLowWatermark();
            MessagingOverflowPolicy outOverflow = pressureCfg.getOutOverflowPolicy();

            if (outOverflow == MessagingOverflowPolicy.IGNORE) {
                sendPressure = null;
            } else {
                sendPressure = new SendPressureGuard(outLoWatermark, outHiWatermark, outOverflow);
            }

            if (inHiWatermark <= 0) {
                receivePressure = null;
            } else {
                receivePressure = new ReceivePressureGuard(inLoWatermark, inHiWatermark);
            }
        } else {
            sendPressure = null;
            receivePressure = null;
        }

        // Apply cluster view filter.
        ClusterView clusterView = cluster.filter(ChannelMetaData.hasReceiver(name, cfg.getClusterFilter()));

        // Fallback to the default load balancer if none is specified.
        LoadBalancer<T> loadBalancer = cfg.getLoadBalancer();

        if (loadBalancer == null) {
            loadBalancer = new DefaultLoadBalancer<>();
        }

        // Prepare partition mapper.
        RendezvousHashMapper mapper = RendezvousHashMapper.of(clusterView)
            .withPartitions(partitions)
            .withBackupNodes(backupNodes)
            .build();

        // Prepare default (root) channel.
        rootChannel = new DefaultMessagingChannel<>(
            this,
            clusterView,
            mapper,
            loadBalancer,
            cfg.getFailoverPolicy(),
            cfg.getMessagingTimeout()
        );
    }

    public String name() {
        return name;
    }

    public Class<T> baseType() {
        return baseType;
    }

    public DefaultMessagingChannel<T> rootChannel() {
        return rootChannel;
    }

    public int nioThreads() {
        return nioThreads;
    }

    public int workerThreads() {
        return workerThreads;
    }

    public long idleSocketTimeout() {
        return idleTimeout;
    }

    public int partitions() {
        return partitions;
    }

    public int backupNodes() {
        return backupNodes;
    }

    public MessageReceiver<T> unguardedReceiver() {
        return unguardedReceiver;
    }

    public InterceptorManager<T> interceptor() {
        return interceptor;
    }

    public SendPressureGuard sendPressureGuard() {
        return sendPressure;
    }

    public ReceivePressureGuard receivePressureGuard() {
        return receivePressure;
    }

    public CodecFactory<T> codecFactory() {
        return codecFactory;
    }

    public Logger log() {
        return log;
    }

    public String logCategory() {
        return logCategory;
    }

    public Hekate hekate() {
        return hekate;
    }

    public SendFuture send(Object affinityKey, T msg, MessagingOpts<T> opts) {
        return requireContext().send(affinityKey, msg, opts);
    }

    public void send(Object affinityKey, T msg, MessagingOpts<T> opts, SendCallback callback) {
        requireContext().send(affinityKey, msg, opts, callback);
    }

    public ResponseFuture<T> request(Object affinityKey, T msg, MessagingOpts<T> opts) {
        return requireContext().request(affinityKey, msg, opts);
    }

    public void request(Object affinityKey, T msg, MessagingOpts<T> opts, ResponseCallback<T> callback) {
        requireContext().request(affinityKey, msg, opts, callback);
    }

    public SubscribeFuture<T> subscribe(Object affinityKey, T msg, MessagingOpts<T> opts) {
        return requireContext().subscribe(affinityKey, msg, opts);
    }

    public void subscribe(Object affinityKey, T msg, MessagingOpts<T> opts, ResponseCallback<T> callback) {
        requireContext().subscribe(affinityKey, msg, opts, callback);
    }

    public BroadcastFuture<T> broadcast(Object affinityKey, T msg, MessagingOpts<T> opts) {
        return requireContext().broadcast(affinityKey, msg, opts);
    }

    public void broadcast(Object affinityKey, T msg, MessagingOpts<T> opts, BroadcastCallback<T> callback) {
        requireContext().broadcast(affinityKey, msg, opts, callback);
    }

    public AggregateFuture<T> aggregate(Object affinityKey, T msg, MessagingOpts<T> opts) {
        return requireContext().aggregate(affinityKey, msg, opts);
    }

    public void aggregate(Object affinityKey, T msg, MessagingOpts<T> opts, AggregateCallback<T> callback) {
        requireContext().aggregate(affinityKey, msg, opts, callback);
    }

    public MessagingChannelId channelId() {
        return requireContext().channelId();
    }

    public Executor executor() {
        return requireContext().executor();
    }

    public MessagingGatewayContext<T> requireContext() {
        // Volatile read.
        MessagingGatewayContext<T> ctx = this.ctx;

        if (ctx == null) {
            throw new IllegalStateException("Messaging channel is not initialized [name=" + name + ']');
        }

        return ctx;
    }

    public MessagingGatewayContext<T> context() {
        // Volatile read.
        return ctx;
    }

    public void init(MessagingGatewayContext<T> ctx) {
        assert ctx != null : "Messaging context is null.";

        // Volatile write.
        this.ctx = ctx;
    }

    public boolean hasReceiver() {
        return unguardedReceiver != null;
    }

    private static <T> CodecFactory<T> optimizeCodecFactory(CodecFactory<T> factory, CodecService fallback) {
        if (factory == null) {
            return fallback.codecFactory();
        } else {
            return ThreadLocalCodecFactory.tryWrap(factory);
        }
    }

    private static String resolveLogCategory(String category) {
        String name = Utils.nullOrTrim(category);

        return name != null ? name : MessagingChannel.class.getName();
    }
}
