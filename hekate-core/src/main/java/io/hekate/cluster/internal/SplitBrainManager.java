package io.hekate.cluster.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.split.SplitBrainAction;
import io.hekate.cluster.split.SplitBrainDetector;
import io.hekate.util.StateGuard;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SplitBrainManager {
    interface Callback {
        void rejoin();

        void terminate();

        void kill();

        void error(Throwable t);
    }

    private static final Logger log = LoggerFactory.getLogger(SplitBrainManager.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final SplitBrainAction action;

    private final SplitBrainDetector detector;

    @ToStringIgnore
    private final StateGuard guard = new StateGuard(SplitBrainManager.class);

    @ToStringIgnore
    private final AtomicBoolean active = new AtomicBoolean();

    @ToStringIgnore
    private ClusterNode localNode;

    @ToStringIgnore
    private Callback callback;

    @ToStringIgnore
    private Executor async;

    public SplitBrainManager(SplitBrainAction action, SplitBrainDetector detector) {
        assert action != null : "Action is null.";

        this.detector = detector;
        this.action = action;
    }

    public SplitBrainDetector detector() {
        return detector;
    }

    public SplitBrainAction action() {
        return action;
    }

    public void initialize(ClusterNode localNode, Executor async, Callback callback) {
        assert localNode != null : "Local node is null.";
        assert async != null : "Executor is null.";
        assert callback != null : "Callback is null.";

        guard.withWriteLock(() -> {
            guard.becomeInitialized();

            this.localNode = localNode;
            this.async = async;
            this.callback = callback;
        });
    }

    public void terminate() {
        guard.withWriteLock(() -> {
            if (guard.becomeTerminated()) {
                this.async = null;
                this.callback = null;
            }
        });
    }

    public boolean check() {
        return guard.withReadLock(() ->
            detector == null || localNode == null || detector.isValid(localNode)
        );
    }

    public void checkAsync() {
        if (detector != null) {
            guard.withReadLockIfInitialized(() -> {
                if (active.compareAndSet(false, true)) {
                    runAsync(() -> {
                        try {
                            guard.withReadLockIfInitialized(() -> {
                                if (DEBUG) {
                                    log.debug("Checking for cluster split-brain [detector={}]", detector);
                                }

                                if (!detector.isValid(localNode)) {
                                    if (log.isWarnEnabled()) {
                                        log.warn("Split-brain detected.");
                                    }

                                    applyAction();
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
            } catch (RuntimeException | Error e) {
                callback.error(e);
            }
        });
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
