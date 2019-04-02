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

package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterView;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecService;
import io.hekate.codec.ThreadLocalCodecFactory;
import io.hekate.core.internal.util.StreamUtils;
import io.hekate.core.internal.util.Utils;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessagingBackPressureConfig;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingChannelId;
import io.hekate.messaging.MessagingOverflowPolicy;
import io.hekate.messaging.intercept.MessageInterceptor;
import io.hekate.messaging.loadbalance.DefaultLoadBalancer;
import io.hekate.messaging.loadbalance.LoadBalancer;
import io.hekate.messaging.retry.FixedBackoffPolicy;
import io.hekate.messaging.retry.GenericRetryConfigurer;
import io.hekate.messaging.retry.RetryErrorPredicate;
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

    private final long messagingTimeout;

    private final long idleTimeout;

    private final int partitions;

    private final int backupNodes;

    private final SendPressureGuard sendPressure;

    private final ReceivePressureGuard receivePressure;

    private final CodecFactory<T> codecFactory;

    @ToStringIgnore
    private final int warnOnRetry;

    @ToStringIgnore
    private final GenericRetryConfigurer retryPolicy;

    @ToStringIgnore
    private final MessageReceiver<T> unguardedReceiver;

    @ToStringIgnore
    private final MessageInterceptors<T> interceptors;

    @ToStringIgnore
    private final DefaultMessagingChannel<T> rootChannel;

    @ToStringIgnore
    private final Logger log;

    @ToStringIgnore
    private final String logCategory;

    @ToStringIgnore
    private volatile MessagingGatewayContext<T> ctx;

    public MessagingGateway(
        MessagingChannelConfig<T> cfg,
        ClusterService cluster,
        CodecService codec,
        List<MessageInterceptor> interceptors
    ) {
        assert cfg != null : "Messaging channel configuration is null.";
        assert cluster != null : "Cluster service is null.";
        assert codec != null : "Codec service is null.";

        this.name = Utils.nullOrTrim(cfg.getName());
        this.baseType = cfg.getBaseType();
        this.nioThreads = cfg.getNioThreads();
        this.workerThreads = cfg.getWorkerThreads();
        this.messagingTimeout = cfg.getMessagingTimeout();
        this.idleTimeout = cfg.getIdleSocketTimeout();
        this.unguardedReceiver = cfg.getReceiver();
        this.partitions = cfg.getPartitions();
        this.backupNodes = cfg.getBackupNodes();
        this.warnOnRetry = cfg.getWarnOnRetry();

        // Retry policy.
        GenericRetryConfigurer retryPolicy;

        if (cfg.getRetryPolicy() == null) {
            // Use an empty stub that doesn't support retries.
            retryPolicy = GenericRetryConfigurer.noRetries();
        } else {
            // Use custom policy.
            GenericRetryConfigurer customPolicy = cfg.getRetryPolicy();

            retryPolicy = retry -> {
                // Make sure that retries are enabled for all types of errors by default.
                // Custom policy can override this setting.
                retry.whileError(RetryErrorPredicate.acceptAll());

                // Apply real policy
                customPolicy.configure(retry);
            };
        }

        this.retryPolicy = retry -> {
            // Make sure that backoff delay is always set.
            // Custom policy can override this setting.
            retry.withBackoff(FixedBackoffPolicy.defaultPolicy());

            // Apply real policy.
            retryPolicy.configure(retry);
        };

        // Interceptors.
        this.interceptors = new MessageInterceptors<>(
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
        ClusterView clusterView = cluster.filter(MessagingMetaData.hasReceiver(name, cfg.getClusterFilter()));

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
            loadBalancer
        );
    }

    public void init(MessagingGatewayContext<T> ctx) {
        assert ctx != null : "Messaging context is null.";

        // Volatile write.
        this.ctx = ctx;
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

    public long messagingTimeout() {
        return messagingTimeout;
    }

    public int partitions() {
        return partitions;
    }

    public int backupNodes() {
        return backupNodes;
    }

    public int warnOnRetry() {
        return warnOnRetry;
    }

    public GenericRetryConfigurer baseRetryPolicy() {
        return retryPolicy;
    }

    public MessageReceiver<T> unguardedReceiver() {
        return unguardedReceiver;
    }

    public MessageInterceptors<T> interceptors() {
        return interceptors;
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

    public MessagingChannelId channelId() {
        return requireContext().channelId();
    }

    public Executor executor() {
        return requireContext().executor();
    }

    public MessagingExecutor async() {
        return requireContext().async();
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
