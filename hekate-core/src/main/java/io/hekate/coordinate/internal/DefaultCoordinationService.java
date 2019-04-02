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

package io.hekate.coordinate.internal;

import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.ClusterView;
import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecService;
import io.hekate.coordinate.CoordinationConfigProvider;
import io.hekate.coordinate.CoordinationFuture;
import io.hekate.coordinate.CoordinationProcess;
import io.hekate.coordinate.CoordinationProcessConfig;
import io.hekate.coordinate.CoordinationService;
import io.hekate.coordinate.CoordinationServiceFactory;
import io.hekate.core.Hekate;
import io.hekate.core.HekateException;
import io.hekate.core.ServiceInfo;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.core.internal.util.StreamUtils;
import io.hekate.core.internal.util.Utils;
import io.hekate.core.service.ConfigurableService;
import io.hekate.core.service.ConfigurationContext;
import io.hekate.core.service.DependencyContext;
import io.hekate.core.service.DependentService;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.InitializingService;
import io.hekate.core.service.TerminatingService;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingConfigProvider;
import io.hekate.messaging.MessagingService;
import io.hekate.util.StateGuard;
import io.hekate.util.async.AsyncUtils;
import io.hekate.util.async.Waiting;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;

public class DefaultCoordinationService implements CoordinationService, ConfigurableService, DependentService, InitializingService,
    TerminatingService, MessagingConfigProvider {
    private static final Logger log = LoggerFactory.getLogger(DefaultCoordinationProcess.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final String CHANNEL_NAME = "hekate.coordination";

    private static final ClusterNodeFilter HAS_SERVICE_FILTER = node -> node.hasService(CoordinationService.class);

    private final long retryDelay;

    private final int nioThreads;

    private final long idleSocketTimeout;

    @ToStringIgnore
    private final StateGuard guard = new StateGuard(CoordinationService.class);

    @ToStringIgnore
    private final List<CoordinationProcessConfig> processesConfig = new ArrayList<>();

    @ToStringIgnore
    private final Map<String, DefaultCoordinationProcess> processes = new HashMap<>();

    @ToStringIgnore
    private Hekate hekate;

    @ToStringIgnore
    private MessagingService messaging;

    @ToStringIgnore
    private ClusterView cluster;

    @ToStringIgnore
    private CodecService defaultCodec;

    public DefaultCoordinationService(CoordinationServiceFactory factory) {
        assert factory != null : "Factory is null.";

        ConfigCheck check = ConfigCheck.get(CoordinationServiceFactory.class);

        check.positive(factory.getRetryInterval(), "retry interval");

        this.nioThreads = factory.getNioThreads();
        this.retryDelay = factory.getRetryInterval();
        this.idleSocketTimeout = factory.getIdleSocketTimeout();

        StreamUtils.nullSafe(factory.getProcesses()).forEach(processesConfig::add);
    }

    @Override
    public void resolve(DependencyContext ctx) {
        hekate = ctx.hekate();

        messaging = ctx.require(MessagingService.class);
        cluster = ctx.require(ClusterService.class).filter(HAS_SERVICE_FILTER);
        defaultCodec = ctx.require(CodecService.class);
    }

    @Override
    public void configure(ConfigurationContext ctx) {
        // Collect configurations from providers.
        Collection<CoordinationConfigProvider> providers = ctx.findComponents(CoordinationConfigProvider.class);

        StreamUtils.nullSafe(providers).forEach(provider -> {
            Collection<CoordinationProcessConfig> processesCfg = provider.configureCoordination();

            StreamUtils.nullSafe(processesCfg).forEach(processesConfig::add);
        });

        // Validate configs.
        ConfigCheck check = ConfigCheck.get(CoordinationProcessConfig.class);

        Set<String> uniqueNames = new HashSet<>();

        processesConfig.forEach(cfg -> {
            check.notEmpty(cfg.getName(), "name");
            check.validSysName(cfg.getName(), "name");
            check.notNull(cfg.getHandler(), "handler");

            String name = cfg.getName().trim();

            check.unique(name, uniqueNames, "name");

            uniqueNames.add(name);
        });

        // Register process names as service property.
        processesConfig.forEach(cfg ->
            ctx.setBoolProperty(propertyName(cfg.getName().trim()), true)
        );
    }

    @Override
    public Collection<MessagingChannelConfig<?>> configureMessaging() {
        if (processesConfig.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, CodecFactory<Object>> processCodecs = new HashMap<>();

        processesConfig.forEach(cfg -> {
            String name = cfg.getName().trim();

            if (cfg.getMessageCodec() == null) {
                processCodecs.put(name, defaultCodec.codecFactory());
            } else {
                processCodecs.put(name, cfg.getMessageCodec());
            }
        });

        return Collections.singleton(
            MessagingChannelConfig.of(CoordinationProtocol.class)
                .withName(CHANNEL_NAME)
                .withClusterFilter(HAS_SERVICE_FILTER)
                .withNioThreads(nioThreads)
                .withIdleSocketTimeout(idleSocketTimeout)
                .withRetryPolicy(retry ->
                    retry.withFixedDelay(retryDelay)
                )
                .withLogCategory(CoordinationProtocol.class.getName())
                .withMessageCodec(() -> new CoordinationProtocolCodec(processCodecs))
                .withReceiver(this::handleMessage)
        );
    }

    @Override
    public void initialize(InitializationContext ctx) throws HekateException {
        guard.lockWrite();

        try {
            guard.becomeInitialized();

            if (DEBUG) {
                log.debug("Initializing...");
            }

            if (!processesConfig.isEmpty()) {
                cluster.addListener(this::processTopologyChange, ClusterEventType.JOIN, ClusterEventType.CHANGE);

                MessagingChannel<CoordinationProtocol> channel = messaging.channel(CHANNEL_NAME, CoordinationProtocol.class);

                for (CoordinationProcessConfig cfg : processesConfig) {
                    initializeProcess(cfg, ctx, channel);
                }
            }

            if (DEBUG) {
                log.debug("Initialized.");
            }
        } finally {
            guard.unlockWrite();
        }
    }

    @Override
    public void preTerminate() throws HekateException {
        Waiting waiting = guard.withWriteLock(() -> {
            if (guard.becomeTerminating()) {
                if (DEBUG) {
                    log.debug("Pre-terminating.");
                }

                return Waiting.awaitAll(processes.values().stream()
                    .map(DefaultCoordinationProcess::terminate)
                    .collect(toList())
                );
            } else {
                return Waiting.NO_WAIT;
            }
        });

        waiting.awaitUninterruptedly();
    }

    @Override
    public void terminate() throws HekateException {
        Waiting waiting = null;

        guard.lockWrite();

        try {
            if (guard.becomeTerminated()) {
                if (DEBUG) {
                    log.debug("Terminating.");
                }

                waiting = Waiting.awaitAll(processes.values().stream()
                    .map(DefaultCoordinationProcess::terminate)
                    .collect(toList())
                );

                processes.clear();
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
    public List<CoordinationProcess> allProcesses() {
        guard.lockReadWithStateCheck();

        try {
            return new ArrayList<>(processes.values());
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public CoordinationProcess process(String name) {
        DefaultCoordinationProcess process;

        guard.lockReadWithStateCheck();

        try {
            process = processes.get(name);
        } finally {
            guard.unlockRead();
        }

        ArgAssert.check(process != null, "Coordination process not configured [name=" + name + ']');

        return process;
    }

    @Override
    public boolean hasProcess(String name) {
        guard.lockReadWithStateCheck();

        try {
            return processes.containsKey(name);
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public CoordinationFuture futureOf(String process) {
        return process(process).future();
    }

    private void initializeProcess(
        CoordinationProcessConfig cfg,
        InitializationContext ctx,
        MessagingChannel<CoordinationProtocol> channel
    ) throws HekateException {
        assert guard.isWriteLocked() : "Thread must hold write lock.";

        if (DEBUG) {
            log.debug("Registering new process [configuration={}]", cfg);
        }

        String name = cfg.getName().trim();

        ExecutorService async = Executors.newSingleThreadExecutor(new HekateThreadFactory("Coordination-" + name));

        DefaultCoordinationProcess process = new DefaultCoordinationProcess(name, hekate, cfg.getHandler(), async, channel);

        processes.put(name, process);

        if (!cfg.isAsyncInit()) {
            ctx.cluster().addSyncFuture(process.future());
        }

        try {
            AsyncUtils.getUninterruptedly(process.initialize());
        } catch (ExecutionException e) {
            throw new HekateException("Failed to initialize coordination handler [process=" + name + ']', e.getCause());
        }
    }

    private void handleMessage(Message<CoordinationProtocol> msg) {
        DefaultCoordinationProcess process = null;

        CoordinationProtocol.RequestBase request = msg.payload(CoordinationProtocol.RequestBase.class);

        guard.lockRead();

        try {
            if (guard.isInitialized()) {
                process = processes.get(request.processName());

                if (process == null) {
                    throw new IllegalStateException("Received coordination request for unknown process: " + request);
                }
            }
        } finally {
            guard.unlockRead();
        }

        if (process == null) {
            if (DEBUG) {
                log.debug("Rejecting coordination message since service is not initialized [message={}]", request);
            }

            msg.reply(CoordinationProtocol.Reject.INSTANCE);
        } else {
            process.processMessage(msg);
        }
    }

    private void processTopologyChange(ClusterEvent event) {
        guard.lockRead();

        try {
            if (guard.isInitialized()) {
                processes.values().forEach(process -> {
                    ClusterTopology topology = event.topology().filter(node -> {
                        ServiceInfo service = node.service(CoordinationService.class);

                        return service.property(propertyName(process.name())) != null;
                    });

                    process.processTopologyChange(topology);
                });
            }
        } finally {
            guard.unlockRead();
        }
    }

    private static String propertyName(String process) {
        return "process." + process;
    }

    @Override
    public String toString() {
        return CoordinationService.class.getSimpleName() + '['
            + ToString.formatProperties(this)
            + ", processes=" + Utils.toString(processesConfig, CoordinationProcessConfig::getName)
            + ']';
    }
}
