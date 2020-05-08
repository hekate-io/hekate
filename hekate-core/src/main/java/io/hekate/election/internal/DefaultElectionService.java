/*
 * Copyright 2020 The Hekate Project
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
import io.hekate.core.jmx.JmxService;
import io.hekate.core.report.ConfigReporter;
import io.hekate.core.service.ConfigurationContext;
import io.hekate.core.service.CoreService;
import io.hekate.core.service.DependencyContext;
import io.hekate.core.service.InitializationContext;
import io.hekate.election.Candidate;
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

import static io.hekate.core.internal.util.StreamUtils.nullSafe;
import static java.util.stream.Collectors.toList;

public class DefaultElectionService implements ElectionService, CoreService, LockConfigProvider {
    private static final Logger log = LoggerFactory.getLogger(DefaultElectionService.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final String ELECTION_LOCK_REGION = "hekate.election";

    private final StateGuard guard = new StateGuard(ElectionService.class);

    private final List<CandidateConfig> candidateConfigs = new ArrayList<>();

    private final Map<String, CandidateHandler> candidates = new HashMap<>();

    private JmxService jmx;

    private LockService locks;

    public DefaultElectionService(ElectionServiceFactory factory) {
        ArgAssert.notNull(factory, "Factory");

        nullSafe(factory.getCandidates()).forEach(candidateConfigs::add);

        nullSafe(factory.getConfigProviders()).forEach(provider ->
            nullSafe(provider.configureElection()).forEach(candidateConfigs::add)
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
        nullSafe(ctx.findComponents(CandidateConfigProvider.class)).forEach(provider ->
            nullSafe(provider.configureElection()).forEach(candidateConfigs::add)
        );

        // Validate configs.
        ConfigCheck check = ConfigCheck.get(CandidateConfig.class);

        Set<String> uniqueGroups = new HashSet<>();

        candidateConfigs.forEach(cfg -> {
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
        if (candidateConfigs.isEmpty()) {
            return Collections.emptyList();
        }

        return Collections.singletonList(new LockRegionConfig(ELECTION_LOCK_REGION));
    }

    @Override
    public void initialize(InitializationContext ctx) throws HekateException {
        if (DEBUG) {
            log.debug("Initializing...");
        }

        guard.becomeInitialized(() -> {
            if (!candidateConfigs.isEmpty()) {
                // Register candidates.
                candidateConfigs.forEach(cfg ->
                    registerCandidate(cfg, ctx.hekate())
                );

                // Register JMX for candidates.
                if (jmx != null) {
                    for (CandidateHandler candidate : candidates.values()) {
                        jmx.register(candidate, candidate.group());
                    }
                }

                // Initialize handlers after we join the cluster.
                ctx.cluster().addListener(
                    event -> guard.withReadLockIfInitialized(() ->
                        candidates.values().forEach(CandidateHandler::initialize)
                    ),
                    ClusterEventType.JOIN
                );
            }
        });

        if (DEBUG) {
            log.debug("Initialized.");
        }
    }

    @Override
    public void report(ConfigReporter report) {
        guard.withReadLockIfInitialized(() -> {
            if (!candidates.isEmpty()) {
                report.section("election", election ->
                    election.section("candidates", candidates ->
                        this.candidates.values().forEach(h ->
                            candidates.section("candidate", candidate -> {
                                candidate.value("group", h.group());
                                candidate.value("candidate", h.candidate());
                            })
                        )
                    )
                );
            }
        });
    }

    @Override
    public void preTerminate() throws HekateException {
        Waiting done = guard.becomeTerminating(() ->
            candidates.values().stream()
                .map(CandidateHandler::terminate)
                .collect(toList())
        );

        done.awaitUninterruptedly();
    }

    @Override
    public void terminate() throws HekateException {
        // Actual termination of handlers happens in postTerminate() in order to make sure that worker threads are terminated after
        // the lock service termination. Otherwise it can lead to RejectedExecutionException if lock service tries to process async lock
        // events while election service is already terminated.
    }

    @Override
    public void postTerminate() throws HekateException {
        if (DEBUG) {
            log.debug("Terminating...");
        }

        Waiting done = guard.becomeTerminated(() -> {
            List<Waiting> waiting = candidates.values().stream()
                .map(CandidateHandler::shutdown)
                .collect(toList());

            candidates.clear();

            return waiting;
        });

        done.awaitUninterruptedly();

        if (DEBUG) {
            log.debug("Terminated.");
        }
    }

    @Override
    public LeaderFuture leader(String group) {
        return candidate(group).leaderFuture();
    }

    private CandidateHandler candidate(String group) {
        ArgAssert.notNull(group, "Group name");

        return guard.withReadLockAndStateCheck(() ->
            candidates.computeIfAbsent(group, missing -> {
                throw new IllegalArgumentException("Unknown group [name=" + missing + ']');
            })
        );
    }

    private void registerCandidate(CandidateConfig cfg, Hekate hekate) {
        assert guard.isWriteLocked() : "Thread must hold a write lock.";
        assert guard.isInitialized() : "Service must be initialized.";
        assert cfg != null : "Configuration is null.";
        assert hekate != null : "Hekate is null.";

        if (DEBUG) {
            log.debug("Registering new candidate [config={}]", cfg);
        }

        // Prepare configuration values.
        String group = cfg.getGroup().trim();
        Candidate candidate = cfg.getCandidate();

        // Election lock.
        DistributedLock lock = locks.region(ELECTION_LOCK_REGION).get(group);

        // Election thread.
        ExecutorService worker = Executors.newSingleThreadExecutor(new HekateThreadFactory("Election-" + group));

        // Register candidate.
        CandidateHandler handler = new CandidateHandler(
            group,
            candidate,
            worker,
            lock,
            hekate.localNode(),
            hekate
        );

        candidates.put(group, handler);
    }

    @Override
    public String toString() {
        return ElectionService.class.getSimpleName();
    }
}
