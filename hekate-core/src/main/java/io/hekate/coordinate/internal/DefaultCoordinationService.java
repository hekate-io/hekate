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

package io.hekate.coordinate.internal;

import io.hekate.cluster.ClusterTopology;
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
import io.hekate.coordinate.internal.CoordinationProtocol.RequestBase;
import io.hekate.core.HekateException;
import io.hekate.core.ServiceInfo;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.core.report.ConfigReporter;
import io.hekate.core.service.ConfigurationContext;
import io.hekate.core.service.CoreService;
import io.hekate.core.service.DependencyContext;
import io.hekate.core.service.InitializationContext;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingConfigProvider;
import io.hekate.messaging.MessagingService;
import io.hekate.util.StateGuard;
import io.hekate.util.async.AsyncUtils;
import io.hekate.util.async.Waiting;
import java.util.ArrayList;
import java.util.Collection;
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

import static io.hekate.core.internal.util.StreamUtils.nullSafe;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;

public class DefaultCoordinationService implements CoordinationService, CoreService, MessagingConfigProvider {
    private static final Logger log = LoggerFactory.getLogger(DefaultCoordinationProcess.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final String COORDINATION_CHANNEL = "hekate.coordination";

    private final long retryDelay;

    private final int nioThreads;

    private final long idleSocketTimeout;

    private final StateGuard guard = new StateGuard(CoordinationService.class);

    private final List<CoordinationProcessConfig> processesConfigs = new ArrayList<>();

    private final Map<String, DefaultCoordinationProcess> processes = new HashMap<>();

    private MessagingService messaging;

    private CodecService defaultCodec;

    public DefaultCoordinationService(CoordinationServiceFactory factory) {
        ArgAssert.notNull(factory, "Factory");

        ConfigCheck check = ConfigCheck.get(CoordinationServiceFactory.class);

        check.positive(factory.getRetryInterval(), "retry interval");

        this.nioThreads = factory.getNioThreads();
        this.retryDelay = factory.getRetryInterval();
        this.idleSocketTimeout = factory.getIdleSocketTimeout();

        nullSafe(factory.getProcesses()).forEach(processesConfigs::add);
    }

    @Override
    public void resolve(DependencyContext ctx) {
        messaging = ctx.require(MessagingService.class);
        defaultCodec = ctx.require(CodecService.class);
    }

    @Override
    public void configure(ConfigurationContext ctx) {
        // Collect configurations from providers.
        nullSafe(ctx.findComponents(CoordinationConfigProvider.class)).forEach(provider ->
            nullSafe(provider.configureCoordination()).forEach(processesConfigs::add)
        );

        // Validate configs.
        ConfigCheck check = ConfigCheck.get(CoordinationProcessConfig.class);

        Set<String> uniqueNames = new HashSet<>();

        processesConfigs.forEach(cfg -> {
            check.notEmpty(cfg.getName(), "name");
            check.validSysName(cfg.getName(), "name");
            check.notNull(cfg.getHandler(), "handler");

            String name = cfg.getName().trim();

            check.unique(name, uniqueNames, "name");

            uniqueNames.add(name);
        });

        // Register process names as service property.
        processesConfigs.forEach(cfg ->
            ctx.setBoolProperty(propertyName(cfg.getName().trim()), true)
        );
    }

    @Override
    public Collection<MessagingChannelConfig<?>> configureMessaging() {
        // Skip configuration is there are no registered processes.
        if (processesConfigs.isEmpty()) {
            return emptyList();
        }

        // Map processes to codecs.
        Map<String, CodecFactory<Object>> codecs = new HashMap<>();

        processesConfigs.forEach(cfg -> {
            String name = cfg.getName().trim();

            if (cfg.getMessageCodec() == null) {
                codecs.put(name, defaultCodec.codecFactory());
            } else {
                codecs.put(name, cfg.getMessageCodec());
            }
        });

        // Prepare codec factory.
        CodecFactory<CoordinationProtocol> codecFactory = () -> new CoordinationProtocolCodec(codecs);

        // Messaging channel configuration.
        return singleton(
            MessagingChannelConfig.of(CoordinationProtocol.class)
                .withName(COORDINATION_CHANNEL)
                .withClusterFilter(node -> node.hasService(CoordinationService.class))
                .withNioThreads(nioThreads)
                .withIdleSocketTimeout(idleSocketTimeout)
                .withMessageCodec(codecFactory)
                .withLogCategory(CoordinationProtocol.class.getName())
                .withRetryPolicy(retry -> retry
                    .withFixedDelay(retryDelay)
                )
                .withReceiver(this::handleMessage)
        );
    }

    @Override
    public void initialize(InitializationContext ctx) throws HekateException {
        if (DEBUG) {
            log.debug("Initializing...");
        }

        guard.becomeInitialized(() -> {
            if (!processesConfigs.isEmpty()) {
                // Get coordination messaging channel (shared among all coordination processes).
                MessagingChannel<CoordinationProtocol> channel = messaging.channel(COORDINATION_CHANNEL, CoordinationProtocol.class);

                // Register cluster listener to trigger coordination.
                channel.cluster().addListener(this::processTopologyChange, ClusterEventType.JOIN, ClusterEventType.CHANGE);

                // Initialize coordination processes.
                for (CoordinationProcessConfig cfg : processesConfigs) {
                    initializeProcess(cfg, ctx, channel);
                }
            }
        });

        if (DEBUG) {
            log.debug("Initialized.");
        }
    }

    @Override
    public void report(ConfigReporter report) {
        guard.withReadLockIfInitialized(() -> {
            if (!processes.isEmpty()) {
                report.section("coordination", coordinationSec -> {
                    coordinationSec.value("retry-delay", retryDelay);

                    for (DefaultCoordinationProcess process : processes.values()) {
                        coordinationSec.section("process", processSec -> {
                            processSec.value("name", process.name());
                            processSec.value("async-init", process.isAsyncInit());
                            processSec.value("handler", process.handler());
                        });
                    }
                });
            }
        });
    }

    @Override
    public void preTerminate() throws HekateException {
        if (DEBUG) {
            log.debug("Pre-terminating.");
        }

        Waiting done = guard.becomeTerminating(() ->
            processes.values().stream()
                .map(DefaultCoordinationProcess::terminate)
                .collect(toList())
        );

        done.awaitUninterruptedly();

        if (DEBUG) {
            log.debug("Pre-terminated.");
        }
    }

    @Override
    public void terminate() throws HekateException {
        if (DEBUG) {
            log.debug("Terminating.");
        }

        Waiting done = guard.becomeTerminated(() -> {
            List<Waiting> waiting = processes.values().stream()
                .map(DefaultCoordinationProcess::terminate)
                .collect(toList());

            processes.clear();

            return waiting;
        });

        done.awaitUninterruptedly();

        if (DEBUG) {
            log.debug("Terminated.");
        }
    }

    @Override
    public List<CoordinationProcess> allProcesses() {
        return guard.withReadLockAndStateCheck(() ->
            new ArrayList<>(processes.values())
        );
    }

    @Override
    public CoordinationProcess process(String name) {
        ArgAssert.notNull(name, "Process name");

        return guard.withReadLockAndStateCheck(() ->
            processes.computeIfAbsent(name, missing -> {
                throw new IllegalArgumentException("Coordination process not configured [name=" + missing + ']');
            })
        );
    }

    @Override
    public boolean hasProcess(String name) {
        return guard.withReadLockAndStateCheck(() ->
            processes.containsKey(name)
        );
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

        // Prepare worker thread.
        ExecutorService async = Executors.newSingleThreadExecutor(new HekateThreadFactory("Coordination-" + name));

        // Instantiate and register.
        DefaultCoordinationProcess process = new DefaultCoordinationProcess(
            name,
            ctx.hekate(),
            cfg.getHandler(),
            cfg.isAsyncInit(),
            async,
            channel
        );

        processes.put(name, process);

        // Register initial coordination future.
        if (!process.isAsyncInit()) {
            ctx.cluster().addListener(
                evt -> evt.attach(process.future()),
                ClusterEventType.JOIN
            );
        }

        // Initialize.
        try {
            AsyncUtils.getUninterruptedly(process.initialize());
        } catch (ExecutionException e) {
            throw new HekateException("Failed to initialize coordination handler [process=" + name + ']', e.getCause());
        }
    }

    private void handleMessage(Message<CoordinationProtocol> msg) {
        RequestBase req = msg.payload(RequestBase.class);

        DefaultCoordinationProcess process = null;

        guard.lockRead();

        try {
            if (guard.isInitialized()) {
                process = processes.get(req.processName());

                if (process == null) {
                    throw new IllegalStateException("Received coordination request for unknown process: " + req);
                }
            }
        } finally {
            guard.unlockRead();
        }

        if (process == null) {
            if (DEBUG) {
                log.debug("Rejecting coordination message since service is not initialized [message={}]", req);
            }

            msg.reply(CoordinationProtocol.Reject.INSTANCE);
        } else {
            process.processMessage(msg);
        }
    }

    private void processTopologyChange(ClusterEvent event) {
        guard.withReadLockIfInitialized(() ->
            processes.values().forEach(process -> {
                ClusterTopology topology = event.topology().filter(node -> {
                    ServiceInfo service = node.service(CoordinationService.class);

                    return service.property(propertyName(process.name())) != null;
                });

                process.processTopologyChange(topology);
            })
        );
    }

    private static String propertyName(String process) {
        return "process." + process;
    }

    @Override
    public String toString() {
        return CoordinationService.class.getSimpleName();
    }
}
