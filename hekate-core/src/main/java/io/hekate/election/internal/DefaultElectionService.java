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

package io.hekate.election.internal;

import io.hekate.cluster.event.ClusterEventType;
import io.hekate.core.Hekate;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.core.internal.util.StreamUtils;
import io.hekate.core.internal.util.Utils;
import io.hekate.core.jmx.JmxService;
import io.hekate.core.service.ConfigurableService;
import io.hekate.core.service.ConfigurationContext;
import io.hekate.core.service.DependencyContext;
import io.hekate.core.service.DependentService;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.InitializingService;
import io.hekate.core.service.TerminatingService;
import io.hekate.election.CandidateConfig;
import io.hekate.election.CandidateConfigProvider;
import io.hekate.election.ElectionService;
import io.hekate.election.ElectionServiceFactory;
import io.hekate.election.LeaderFuture;
import io.hekate.lock.DistributedLock;
import io.hekate.lock.LockConfigProvider;
import io.hekate.lock.LockRegionConfig;
import io.hekate.lock.LockService;
import io.hekate.util.StateGuard;
import io.hekate.util.async.Waiting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;

public class DefaultElectionService implements ElectionService, DependentService, ConfigurableService, InitializingService,
    TerminatingService, LockConfigProvider {
    private static final Logger log = LoggerFactory.getLogger(DefaultElectionService.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final String LOCK_REGION = "hekate.election";

    private final StateGuard guard = new StateGuard(ElectionService.class);

    private final List<CandidateConfig> candidatesConfig = new ArrayList<>();

    private final Map<String, CandidateHandler> handlers = new HashMap<>();

    private JmxService jmx;

    private LockService locks;

    public DefaultElectionService(ElectionServiceFactory factory) {
        ArgAssert.notNull(factory, "Factory");

        StreamUtils.nullSafe(factory.getCandidates()).forEach(candidatesConfig::add);

        StreamUtils.nullSafe(factory.getConfigProviders()).forEach(provider ->
            StreamUtils.nullSafe(provider.configureElection()).forEach(candidatesConfig::add)
        );
    }

    @Override
    public void resolve(DependencyContext ctx) {
        locks = ctx.require(LockService.class);

        jmx = ctx.optional(JmxService.class);
    }

    @Override
    public void configure(ConfigurationContext ctx) {
        // Collect configurations from providers.
        Collection<CandidateConfigProvider> providers = ctx.findComponents(CandidateConfigProvider.class);

        StreamUtils.nullSafe(providers).forEach(provider ->
            StreamUtils.nullSafe(provider.configureElection()).forEach(candidatesConfig::add)
        );

        // Validate configs.
        ConfigCheck check = ConfigCheck.get(CandidateConfig.class);

        Set<String> uniqueGroups = new HashSet<>();

        candidatesConfig.forEach(cfg -> {
            check.notEmpty(cfg.getGroup(), "group");
            check.validSysName(cfg.getGroup(), "group");
            check.notNull(cfg.getCandidate(), "candidate");

            String group = cfg.getGroup().trim();

            check.unique(group, uniqueGroups, "group");

            uniqueGroups.add(group);
        });
    }

    @Override
    public Collection<LockRegionConfig> configureLocking() {
        if (candidatesConfig.isEmpty()) {
            return Collections.emptyList();
        }

        return Collections.singletonList(new LockRegionConfig().withName(LOCK_REGION));
    }

    @Override
    public void initialize(InitializationContext ctx) throws HekateException {
        if (DEBUG) {
            log.debug("Initializing...");
        }

        guard.lockWrite();

        try {
            guard.becomeInitialized();

            if (!candidatesConfig.isEmpty()) {
                // Register handlers.
                candidatesConfig.forEach(cfg ->
                    doRegister(cfg, ctx.hekate())
                );

                // Register JMX for handlers.
                if (jmx != null) {
                    for (CandidateHandler handler : handlers.values()) {
                        jmx.register(handler, handler.group());
                    }
                }

                // Initialize handlers after joining the cluster.
                ctx.cluster().addListener(event ->
                    guard.withReadLockIfInitialized(() ->
                        handlers.values().forEach(CandidateHandler::initialize)
                    ), ClusterEventType.JOIN
                );
            }
        } finally {
            guard.unlockWrite();
        }

        if (DEBUG) {
            log.debug("Initialized.");
        }
    }

    @Override
    public void preTerminate() throws HekateException {
        Waiting waiting = null;

        guard.lockWrite();

        try {
            if (guard.becomeTerminating()) {
                waiting = Waiting.awaitAll(handlers.values().stream().map(CandidateHandler::terminate).collect(toList()));
            }
        } finally {
            guard.unlockWrite();
        }

        if (waiting != null) {
            waiting.awaitUninterruptedly();
        }
    }

    @Override
    public void terminate() throws HekateException {
        // Actual termination of handlers happens in postTerminate() in order to make sure that worker threads are terminated after
        // the lock service termination. Otherwise it can lead to RejectedExecutionException if lock service tries to process async lock
        // events while election service is already terminated.
    }

    @Override
    public void postTerminate() throws HekateException {
        Waiting waiting = null;

        guard.lockWrite();

        try {
            if (guard.becomeTerminated()) {
                if (DEBUG) {
                    log.debug("Terminating...");
                }

                waiting = Waiting.awaitAll(handlers.values().stream().map(CandidateHandler::shutdown).collect(toList()));

                handlers.clear();
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
    public LeaderFuture leader(String group) {
        return handler(group).leaderFuture();
    }

    private CandidateHandler handler(String group) {
        ArgAssert.notNull(group, "Group name");

        CandidateHandler handler;

        guard.lockReadWithStateCheck();

        try {
            handler = handlers.get(group);
        } finally {
            guard.unlockRead();
        }

        if (handler == null) {
            throw new IllegalArgumentException("Unknown group [name=" + group + ']');
        }

        return handler;
    }

    private void doRegister(CandidateConfig cfg, Hekate hekate) {
        assert guard.isWriteLocked() : "Thread must hold a write lock.";
        assert guard.isInitialized() : "Service must be initialized.";
        assert cfg != null : "Configuration is null.";
        assert hekate != null : "Hekate is null.";

        if (DEBUG) {
            log.debug("Registering new configuration [config={}]", cfg);
        }

        String group = cfg.getGroup().trim();

        DistributedLock lock = locks.region(LOCK_REGION).get(group);

        ExecutorService worker = Executors.newSingleThreadExecutor(new HekateThreadFactory("Election" + '-' + group));

        CandidateHandler handler = new CandidateHandler(
            group,
            cfg.getCandidate(),
            worker,
            lock,
            hekate.localNode(),
            hekate
        );

        handlers.put(group, handler);
    }

    @Override
    public String toString() {
        return ElectionService.class.getSimpleName() + "[candidates=" + Utils.toString(candidatesConfig, CandidateConfig::getGroup) + ']';
    }
}
