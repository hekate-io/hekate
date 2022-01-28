/*
 * Copyright 2022 The Hekate Project
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
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterView;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.codec.SingletonCodecFactory;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.core.report.ConfigReporter;
import io.hekate.core.service.ConfigurationContext;
import io.hekate.core.service.CoreService;
import io.hekate.core.service.DependencyContext;
import io.hekate.core.service.InitializationContext;
import io.hekate.lock.LockConfigProvider;
import io.hekate.lock.LockRegion;
import io.hekate.lock.LockRegionConfig;
import io.hekate.lock.LockService;
import io.hekate.lock.LockServiceFactory;
import io.hekate.lock.internal.LockProtocol.LockOwnerRequest;
import io.hekate.lock.internal.LockProtocol.LockOwnerResponse;
import io.hekate.lock.internal.LockProtocol.LockRequest;
import io.hekate.lock.internal.LockProtocol.LockRequestBase;
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
import io.hekate.messaging.intercept.ClientMessageInterceptor;
import io.hekate.messaging.intercept.ClientSendContext;
import io.hekate.util.StateGuard;
import io.hekate.util.async.Waiting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.core.internal.util.StreamUtils.nullSafe;
import static io.hekate.util.async.AsyncUtils.shutdown;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

public class DefaultLockService implements LockService, CoreService, MessagingConfigProvider {
    private static final Logger log = LoggerFactory.getLogger(DefaultLockService.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final String CHANNEL_NAME = "hekate.locks";

    private final long retryInterval;

    private final int nioThreads;

    private final int workerThreads;

    private final StateGuard guard = new StateGuard(LockService.class);

    private final Map<String, LockRegionProxy> regions = new HashMap<>();

    private ScheduledThreadPoolExecutor scheduler;

    private ClusterView cluster;

    private MessagingService messaging;

    public DefaultLockService(LockServiceFactory factory) {
        ArgAssert.notNull(factory, "Factory");

        ConfigCheck check = ConfigCheck.get(LockServiceFactory.class);

        check.positive(factory.getRetryInterval(), "retry interval");
        check.positive(factory.getWorkerThreads(), "worker thread pool size");

        this.retryInterval = factory.getRetryInterval();
        this.workerThreads = factory.getWorkerThreads();
        this.nioThreads = factory.getNioThreads();

        nullSafe(factory.getRegions()).forEach(this::register);

        nullSafe(factory.getConfigProviders()).forEach(provider ->
            nullSafe(provider.configureLocking()).forEach(this::register)
        );
    }

    @Override
    public void resolve(DependencyContext ctx) {
        messaging = ctx.require(MessagingService.class);
        cluster = ctx.require(ClusterService.class).filter(LockRegionNodeFilter.hasLockService());
    }

    @Override
    public void configure(ConfigurationContext ctx) {
        // Collect configurations from providers.
        nullSafe(ctx.findComponents(LockConfigProvider.class)).forEach(provider ->
            nullSafe(provider.configureLocking()).forEach(this::register)
        );

        // Register region names as service property.
        regions.values().forEach(region ->
            ctx.setBoolProperty(LockRegionNodeFilter.serviceProperty(region.name()), true)
        );
    }

    @Override
    public void report(ConfigReporter report) {
        guard.withReadLockIfInitialized(() -> {
            if (!regions.isEmpty()) {
                report.section("locks", locksSec -> {
                    locksSec.value("retry-interval", retryInterval);

                    locksSec.section("regions", regionsSec ->
                        regions.values().forEach(region ->
                            regionsSec.section("region", regionSec ->
                                regionSec.value("name", region.name())
                            )
                        )
                    );
                });
            }
        });
    }

    @Override
    public Collection<MessagingChannelConfig<?>> configureMessaging() {
        if (regions.isEmpty()) {
            return emptyList();
        }

        return singleton(
            MessagingChannelConfig.of(LockProtocol.class)
                .withName(CHANNEL_NAME)
                .withLogCategory(LockProtocol.class.getName())
                .withNioThreads(nioThreads)
                .withWorkerThreads(workerThreads)
                .withMessageCodec(new SingletonCodecFactory<>(new LockProtocolCodec()))
                .withBackupNodes(0)
                .withRetryPolicy(retry ->
                    retry.withFixedDelay(retryInterval)
                )
                .withInterceptor(new ClientMessageInterceptor<LockProtocol>() {
                    @Override
                    public void interceptClientSend(ClientSendContext<LockProtocol> ctx) {
                        if (ctx.payload() instanceof LockRequestBase) {
                            LockRequestBase req = (LockRequestBase)ctx.payload();

                            // Store routed topology within the lock request so that it would be possible
                            // to detect routing collisions (in case of cluster topology changes) on the receiving side.
                            ctx.overrideMessage(req.withTopology(ctx.topology().hash()));
                        }
                    }
                })
                .withReceiver(this::processMessage)
        );
    }

    @Override
    public void initialize(InitializationContext ctx) throws HekateException {
        if (DEBUG) {
            log.debug("Initializing...");
        }

        guard.becomeInitialized(() -> {
            if (!regions.isEmpty()) {
                ClusterNode node = ctx.localNode();

                // Register cluster listener to trigger locks rebalancing.
                cluster.addListener(evt ->
                        processTopologyChange(),
                    ClusterEventType.JOIN,
                    ClusterEventType.CHANGE
                );

                // Get channel for locking messages.
                MessagingChannel<LockProtocol> channel = messaging.channel(CHANNEL_NAME, LockProtocol.class);

                // Prepare schedule for asynchronous operations in lock regions.
                scheduler = new ScheduledThreadPoolExecutor(1, new HekateThreadFactory("LockService"));

                scheduler.setRemoveOnCancelPolicy(true);

                // Register lock regions.
                regions.values().forEach(proxy -> {
                    if (DEBUG) {
                        log.debug("Registering new lock region [config={}]", proxy);
                    }

                    String name = proxy.name();

                    DefaultLockRegion region = new DefaultLockRegion(
                        name,
                        node.id(),
                        scheduler,
                        ctx.metrics(),
                        channel.filter(new LockRegionNodeFilter(name))
                    );

                    proxy.initialize(region);
                });
            }
        });

        if (DEBUG) {
            log.debug("Initialized.");
        }
    }

    @Override
    public void preTerminate() {
        guard.becomeTerminating(() ->
            regions.values().forEach(LockRegionProxy::preTerminate)
        );
    }

    @Override
    public void terminate() throws HekateException {
        if (DEBUG) {
            log.debug("Terminating...");
        }

        Waiting done = guard.becomeTerminated(() -> {
            // Shutdown scheduler.
            Waiting waiting = shutdown(scheduler);

            scheduler = null;

            // Terminate regions.
            regions.values().forEach(LockRegionProxy::terminate);

            return singletonList(waiting);
        });

        done.awaitUninterruptedly();

        if (DEBUG) {
            log.debug("Terminated.");
        }
    }

    @Override
    public List<LockRegion> allRegions() {
        return guard.withReadLockAndStateCheck(() ->
            new ArrayList<>(regions.values())
        );
    }

    @Override
    public LockRegionProxy region(String region) {
        ArgAssert.notNull(region, "Region name");

        return guard.withReadLockAndStateCheck(() ->
            regions.computeIfAbsent(region, missing -> {
                throw new IllegalArgumentException("Lock region is not configured [name=" + missing + ']');
            })
        );
    }

    @Override
    public boolean hasRegion(String region) {
        return guard.withReadLockAndStateCheck(() ->
            regions.containsKey(region)
        );
    }

    private void register(LockRegionConfig cfg) {
        // Validate configs.
        ConfigCheck check = ConfigCheck.get(LockRegionConfig.class);

        check.notEmpty(cfg.getName(), "name");
        check.validSysName(cfg.getName(), "name");

        String name = cfg.getName().trim();

        check.unique(name, regions.keySet(), "name");

        regions.put(name, new LockRegionProxy(name));
    }

    private void processMessage(Message<LockProtocol> msg) {
        guard.lockRead();

        try {
            LockProtocol.Type type = msg.payload().type();

            switch (type) {
                case LOCK_REQUEST: {
                    LockRequest request = msg.payload(LockRequest.class);

                    if (guard.isInitialized()) {
                        LockRegionProxy proxy = regions.get(request.region());

                        if (proxy == null) {
                            throw new IllegalStateException("Received lock request for an unsupported region: " + request);
                        }

                        proxy.requireRegion().processLock(msg);
                    } else {
                        msg.reply(new LockResponse(LockResponse.Status.RETRY, null, 0));
                    }

                    break;
                }
                case UNLOCK_REQUEST: {
                    UnlockRequest request = msg.payload(UnlockRequest.class);

                    if (guard.isInitialized()) {
                        LockRegionProxy proxy = regions.get(request.region());

                        if (proxy == null) {
                            throw new IllegalStateException("Received lock request for an unsupported region: " + request);
                        }

                        proxy.requireRegion().processUnlock(msg);
                    } else {
                        msg.reply(new UnlockResponse(UnlockResponse.Status.RETRY));
                    }

                    break;
                }
                case OWNER_REQUEST: {
                    LockOwnerRequest request = msg.payload(LockOwnerRequest.class);

                    if (guard.isInitialized()) {
                        LockRegionProxy proxy = regions.get(request.region());

                        if (proxy == null) {
                            throw new IllegalStateException("Received lock owner request for an unsupported region: " + request);
                        }

                        proxy.requireRegion().processLockOwnerQuery(msg);
                    } else {
                        msg.reply(new LockOwnerResponse(0, null, LockOwnerResponse.Status.RETRY));
                    }

                    break;
                }
                case MIGRATION_PREPARE: {
                    MigrationPrepareRequest request = msg.payload(MigrationPrepareRequest.class);

                    if (guard.isInitialized()) {
                        LockRegionProxy proxy = regions.get(request.region());

                        if (proxy == null) {
                            throw new IllegalStateException("Received migration prepare request for an unsupported region: " + request);
                        }

                        proxy.requireRegion().processMigrationPrepare(msg);
                    } else {
                        msg.reply(new MigrationResponse(MigrationResponse.Status.RETRY));
                    }

                    break;
                }
                case MIGRATION_APPLY: {
                    MigrationApplyRequest request = msg.payload(MigrationApplyRequest.class);

                    if (guard.isInitialized()) {
                        LockRegionProxy proxy = regions.get(request.region());

                        if (proxy == null) {
                            throw new IllegalStateException("Received migration prepare request for an unsupported region: " + request);
                        }

                        proxy.requireRegion().processMigrationApply(msg);
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
        guard.withReadLockIfInitialized(() ->
            regions.values().stream()
                .map(LockRegionProxy::requireRegion)
                .forEach(DefaultLockRegion::processTopologyChange)
        );
    }

    @Override
    public String toString() {
        return LockService.class.getSimpleName();
    }
}
