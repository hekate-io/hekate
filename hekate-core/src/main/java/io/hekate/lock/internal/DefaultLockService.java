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

package io.hekate.lock.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterView;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.codec.SingletonCodecFactory;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.core.internal.util.Utils;
import io.hekate.core.internal.util.Waiting;
import io.hekate.core.service.ConfigurableService;
import io.hekate.core.service.ConfigurationContext;
import io.hekate.core.service.DependencyContext;
import io.hekate.core.service.DependentService;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.InitializingService;
import io.hekate.core.service.TerminatingService;
import io.hekate.lock.LockConfigProvider;
import io.hekate.lock.LockRegion;
import io.hekate.lock.LockRegionConfig;
import io.hekate.lock.LockService;
import io.hekate.lock.LockServiceFactory;
import io.hekate.lock.internal.LockProtocol.LockOwnerRequest;
import io.hekate.lock.internal.LockProtocol.LockOwnerResponse;
import io.hekate.lock.internal.LockProtocol.LockRequest;
import io.hekate.lock.internal.LockProtocol.LockResponse;
import io.hekate.lock.internal.LockProtocol.MigrationApplyRequest;
import io.hekate.lock.internal.LockProtocol.MigrationPrepareRequest;
import io.hekate.lock.internal.LockProtocol.MigrationResponse;
import io.hekate.lock.internal.LockProtocol.UnlockRequest;
import io.hekate.lock.internal.LockProtocol.UnlockResponse;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingConfigProvider;
import io.hekate.messaging.MessagingService;
import io.hekate.partition.PartitionConfigProvider;
import io.hekate.partition.PartitionMapper;
import io.hekate.partition.PartitionMapperConfig;
import io.hekate.partition.PartitionService;
import io.hekate.util.StateGuard;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;

public class DefaultLockService implements LockService, InitializingService, DependentService, ConfigurableService, TerminatingService,
    MessagingConfigProvider, PartitionConfigProvider {
    static final ClusterNodeFilter NODE_FILTER = node -> node.hasService(LockService.class);

    static final String REGIONS_PROPERTY = "regions";

    private static final Logger log = LoggerFactory.getLogger(DefaultLockService.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final String LOCK_PREFIX = "hekate.locks";

    private final StateGuard guard = new StateGuard(LockService.class);

    private final List<LockRegionConfig> regionsConfig = new LinkedList<>();

    private final long retryInterval;

    private final int nioThreads;

    private final int workerThreads;

    private final Map<String, DefaultLockRegion> regions = new HashMap<>();

    private ScheduledThreadPoolExecutor scheduler;

    private ClusterView cluster;

    private MessagingService messaging;

    private PartitionService partitions;

    public DefaultLockService(LockServiceFactory factory) {
        assert factory != null : "Factory is null.";

        ConfigCheck check = ConfigCheck.get(LockServiceFactory.class);

        check.positive(factory.getRetryInterval(), "retry interval");
        check.positive(factory.getWorkerThreads(), "worker thread pool size");

        this.retryInterval = factory.getRetryInterval();
        this.workerThreads = factory.getWorkerThreads();
        this.nioThreads = factory.getNioThreads();

        Utils.nullSafe(factory.getRegions()).forEach(regionsConfig::add);

        Utils.nullSafe(factory.getConfigProviders()).forEach(provider ->
            Utils.nullSafe(provider.configureLocking()).forEach(regionsConfig::add)
        );
    }

    @Override
    public void resolve(DependencyContext ctx) {
        messaging = ctx.require(MessagingService.class);
        partitions = ctx.require(PartitionService.class);
        cluster = ctx.require(ClusterService.class).filter(NODE_FILTER);
    }

    @Override
    public void configure(ConfigurationContext ctx) {
        // Collect configurations from providers.
        Collection<LockConfigProvider> providers = ctx.findComponents(LockConfigProvider.class);

        Utils.nullSafe(providers).forEach(provider -> {
            Collection<LockRegionConfig> regions = provider.configureLocking();

            Utils.nullSafe(regions).forEach(regionsConfig::add);
        });

        // Validate configs.
        ConfigCheck check = ConfigCheck.get(LockRegionConfig.class);

        Set<String> uniqueNames = new HashSet<>();

        regionsConfig.forEach(cfg -> {
            check.notEmpty(cfg.getName(), "name");

            String name = cfg.getName().trim();

            check.unique(name, uniqueNames, "name");

            uniqueNames.add(name);
        });

        // Register region names as service property.
        regionsConfig.forEach(cfg ->
            ctx.addServiceProperty(REGIONS_PROPERTY, cfg.getName().trim())
        );
    }

    @Override
    public Collection<MessagingChannelConfig<?>> configureMessaging() {
        if (regionsConfig.isEmpty()) {
            return Collections.emptyList();
        }

        return Collections.singleton(
            new MessagingChannelConfig<LockProtocol>()
                .withName(LOCK_PREFIX)
                .withLogCategory(getClass().getName())
                .withNioThreads(nioThreads)
                .withWorkerThreads(workerThreads)
                .withMessageCodec(new SingletonCodecFactory<>(new LockProtocolCodec()))
                .withReceiver(this::processMessage)
        );
    }

    @Override
    public Collection<PartitionMapperConfig> configurePartitions() {
        if (regionsConfig.isEmpty()) {
            return Collections.emptyList();
        }

        return regionsConfig.stream().map(cfg -> {
            String name = cfg.getName().trim();

            return new PartitionMapperConfig()
                .withName(LOCK_PREFIX + '.' + name)
                .withBackupNodes(0)
                .withFilter(new LockRegionNodeFilter(name));
        }).collect(toList());
    }

    @Override
    public void initialize(InitializationContext ctx) throws HekateException {
        guard.lockWrite();

        try {
            guard.becomeInitialized();

            if (DEBUG) {
                log.debug("Initializing...");
            }

            if (!regionsConfig.isEmpty()) {
                ClusterNode node = ctx.getNode();

                cluster.addListener(evt -> processTopologyChange(), ClusterEventType.JOIN, ClusterEventType.CHANGE);

                scheduler = new ScheduledThreadPoolExecutor(1, new HekateThreadFactory("LockService"));

                scheduler.setRemoveOnCancelPolicy(true);

                MessagingChannel<LockProtocol> channel = messaging.channel(LOCK_PREFIX);

                regionsConfig.forEach(cfg -> {
                    if (DEBUG) {
                        log.debug("Registering new lock region [config={}]", cfg);
                    }

                    String name = cfg.getName().trim();

                    PartitionMapper mapper = partitions.mapper(LOCK_PREFIX + '.' + name);

                    regions.put(name, new DefaultLockRegion(name, node.getId(), scheduler, mapper, channel, retryInterval));
                });
            }

            if (DEBUG) {
                log.debug("Initialized.");
            }
        } finally {
            guard.unlockWrite();
        }
    }

    @Override
    public void preTerminate() {
        guard.lockWrite();

        try {
            if (guard.becomeTerminating()) {
                regions.values().forEach(DefaultLockRegion::terminate);
            }
        } finally {
            guard.unlockWrite();
        }
    }

    @Override
    public void terminate() throws HekateException {
        Waiting waiting = null;

        guard.lockWrite();

        try {
            if (guard.becomeTerminated()) {
                if (DEBUG) {
                    log.debug("Terminating...");
                }

                // Shutdown scheduler.
                if (scheduler != null) {
                    waiting = Utils.shutdown(scheduler);

                    scheduler = null;
                }

                regions.clear();
            }
        } finally {
            guard.unlockWrite();
        }

        if (waiting != null) {
            waiting.awaitUninterruptedly();

            if (DEBUG) {
                log.debug("Terminated.");
            }
        }
    }

    @Override
    public List<LockRegion> allRegions() {
        guard.lockReadWithStateCheck();

        try {
            return new ArrayList<>(regions.values());
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public DefaultLockRegion region(String region) {
        guard.lockReadWithStateCheck();

        try {
            DefaultLockRegion lockRegion = regions.get(region);

            ArgAssert.check(lockRegion != null, "Lock region is not configured [name=" + region + ']');

            return lockRegion;
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public boolean hasRegion(String region) {
        guard.lockReadWithStateCheck();

        try {
            return regions.containsKey(region);
        } finally {
            guard.unlockRead();
        }
    }

    private void processMessage(Message<LockProtocol> msg) {
        guard.lockRead();

        try {
            LockProtocol.Type type = msg.get().getType();

            switch (type) {
                case LOCK_REQUEST: {
                    LockRequest request = msg.get(LockRequest.class);

                    if (guard.isInitialized()) {
                        DefaultLockRegion region = regions.get(request.getRegion());

                        if (region == null) {
                            throw new IllegalStateException("Received lock request for unsupported region: " + request);
                        }

                        region.processLock(msg);
                    } else {
                        msg.reply(new LockResponse(LockResponse.Status.RETRY, null, 0));
                    }

                    break;
                }
                case UNLOCK_REQUEST: {
                    UnlockRequest request = msg.get(UnlockRequest.class);

                    if (guard.isInitialized()) {
                        DefaultLockRegion region = regions.get(request.getRegion());

                        if (region == null) {
                            throw new IllegalStateException("Received lock request for unsupported region: " + request);
                        }

                        region.processUnlock(msg);
                    } else {
                        msg.reply(new UnlockResponse(UnlockResponse.Status.RETRY));
                    }

                    break;
                }
                case OWNER_REQUEST: {
                    LockOwnerRequest request = msg.get(LockOwnerRequest.class);

                    if (guard.isInitialized()) {
                        DefaultLockRegion region = regions.get(request.getRegion());

                        if (region == null) {
                            throw new IllegalStateException("Received lock owner request for unsupported region: " + request);
                        }

                        region.processLockOwnerQuery(msg);
                    } else {
                        msg.reply(new LockOwnerResponse(0, null, LockOwnerResponse.Status.RETRY));
                    }

                    break;
                }
                case MIGRATION_PREPARE: {
                    MigrationPrepareRequest request = msg.get(MigrationPrepareRequest.class);

                    if (guard.isInitialized()) {
                        DefaultLockRegion region = regions.get(request.getRegion());

                        if (region == null) {
                            throw new IllegalStateException("Received migration prepare request for unsupported region: " + request);
                        }

                        region.processMigrationPrepare(msg);
                    } else {
                        msg.reply(new MigrationResponse(MigrationResponse.Status.RETRY));
                    }

                    break;
                }
                case MIGRATION_APPLY: {
                    MigrationApplyRequest request = msg.get(MigrationApplyRequest.class);

                    if (guard.isInitialized()) {
                        DefaultLockRegion region = regions.get(request.getRegion());

                        if (region == null) {
                            throw new IllegalStateException("Received migration prepare request for unsupported region: " + request);
                        }

                        region.processMigrationApply(msg);
                    } else {
                        msg.reply(new MigrationResponse(MigrationResponse.Status.RETRY));
                    }

                    break;
                }
                case MIGRATION_RESPONSE:
                case LOCK_RESPONSE:
                case UNLOCK_RESPONSE:
                case OWNER_RESPONSE:
                default: {
                    throw new IllegalArgumentException("Unexpected message type: " + type);
                }
            }
        } finally {
            guard.unlockRead();
        }
    }

    private void processTopologyChange() {
        guard.lockRead();

        try {
            regions.values().forEach(DefaultLockRegion::processTopologyChange);
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public String toString() {
        return LockService.class.getSimpleName() + "[regions=" + Utils.toString(regionsConfig, LockRegionConfig::getName) + ']';
    }
}
