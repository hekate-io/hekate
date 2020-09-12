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

package io.hekate.cluster.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.split.SplitBrainAction;
import io.hekate.cluster.split.SplitBrainDetector;
import io.hekate.core.report.ConfigReportSupport;
import io.hekate.core.report.ConfigReporter;
import io.hekate.util.StateGuard;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SplitBrainManager implements ConfigReportSupport {
    interface Callback {
        void rejoin();

        void terminate();

        void kill();

        void error(Throwable t);
    }

    private static final Logger log = LoggerFactory.getLogger(SplitBrainManager.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final long checkInterval;

    private final SplitBrainAction action;

    private final SplitBrainDetector detector;

    @ToStringIgnore
    private final StateGuard guard = new StateGuard(SplitBrainManager.class);

    @ToStringIgnore
    private final AtomicBoolean active = new AtomicBoolean();

    @ToStringIgnore
    private final AtomicBoolean actionApplied = new AtomicBoolean();

    @ToStringIgnore
    private ClusterNode localNode;

    @ToStringIgnore
    private Callback callback;

    @ToStringIgnore
    private Executor async;

    public SplitBrainManager(SplitBrainAction action, long checkInterval, SplitBrainDetector detector) {
        this.detector = detector;
        this.checkInterval = checkInterval;
        this.action = action;
    }

    @Override
    public void report(ConfigReporter report) {
        report.value("action", action);
        report.value("check-interval", checkInterval);
        report.value("detector", detector);
    }

    public long checkInterval() {
        return checkInterval;
    }

    public boolean hasDetector() {
        return detector != null;
    }

    public SplitBrainDetector detector() {
        return detector;
    }

    public SplitBrainAction action() {
        return action;
    }

    public void initialize(ClusterNode localNode, Executor async, Callback callback) {
        guard.becomeInitialized(() -> {
            this.localNode = localNode;
            this.async = async;
            this.callback = callback;
            this.actionApplied.set(false);
        });
    }

    public void terminate() {
        guard.becomeTerminated(() -> {
            this.async = null;
            this.callback = null;
        });
    }

    public boolean check() {
        return guard.withReadLock(() -> {
            if (hasDetector() && localNode != null && active.compareAndSet(false, true)) {
                try {
                    return detector.isValid(localNode);
                } finally {
                    active.compareAndSet(true, false);
                }
            } else {
                return true;
            }
        });
    }

    public void checkAsync() {
        if (hasDetector()) {
            guard.withReadLockIfInitialized(() -> {
                if (active.compareAndSet(false, true)) {
                    runAsync(() -> {
                        try {
                            guard.withReadLockIfInitialized(() -> {
                                if (!actionApplied.get()) {
                                    if (DEBUG) {
                                        log.debug("Checking for cluster split-brain [detector={}]", detector);
                                    }

                                    if (!detector.isValid(localNode)) {
                                        // Make sure that we apply action only once.
                                        if (actionApplied.compareAndSet(false, true)) {
                                            if (log.isWarnEnabled()) {
                                                log.warn("Split-brain detected.");
                                            }

                                            applyAction();
                                        }
                                    }
                                }
                            });
                        } finally {
                            active.compareAndSet(true, false);
                        }
                    });
                }
            });
        }
    }

    public void applyAction() {
        guard.withReadLockIfInitialized(() -> {
            switch (action) {
                case REJOIN: {
                    if (log.isWarnEnabled()) {
                        log.warn("Rejoining due to inconsistency of the cluster state.");
                    }

                    callback.rejoin();

                    break;
                }
                case TERMINATE: {
                    if (log.isErrorEnabled()) {
                        log.error("Terminating due to inconsistency of the cluster state.");
                    }

                    callback.terminate();

                    break;
                }
                case KILL_JVM: {
                    if (log.isErrorEnabled()) {
                        log.error("Killing the JVM due to inconsistency of the cluster state.");
                    }

                    callback.kill();

                    break;
                }
                default: {
                    throw new IllegalArgumentException("Unexpected policy: " + action);
                }
            }
        });
    }

    private void runAsync(Runnable task) {
        async.execute(() -> {
            try {
                task.run();
            } catch (Throwable e) {
                callback.error(e);
            }
        });
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
