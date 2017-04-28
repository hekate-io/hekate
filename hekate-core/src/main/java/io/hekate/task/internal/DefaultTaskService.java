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

package io.hekate.task.internal;

import io.hekate.cluster.ClusterFilter;
import io.hekate.cluster.ClusterNode;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecService;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.Utils;
import io.hekate.core.service.DependencyContext;
import io.hekate.core.service.DependentService;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.InitializingService;
import io.hekate.core.service.TerminatingService;
import io.hekate.failover.FailoverPolicy;
import io.hekate.failover.FailoverPolicyBuilder;
import io.hekate.failover.FailureInfo;
import io.hekate.inject.InjectionService;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingConfigProvider;
import io.hekate.messaging.MessagingService;
import io.hekate.task.ApplicableTask;
import io.hekate.task.CallableTask;
import io.hekate.task.MultiNodeResult;
import io.hekate.task.RunnableTask;
import io.hekate.task.TaskFuture;
import io.hekate.task.TaskService;
import io.hekate.task.TaskServiceFactory;
import io.hekate.task.internal.TaskProtocol.ApplyTask;
import io.hekate.task.internal.TaskProtocol.CallTask;
import io.hekate.task.internal.TaskProtocol.ErrorResult;
import io.hekate.task.internal.TaskProtocol.NullResult;
import io.hekate.task.internal.TaskProtocol.ObjectResult;
import io.hekate.task.internal.TaskProtocol.RunTask;
import io.hekate.util.StateGuard;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.failover.FailoverRoutingPolicy.RE_ROUTE;

public class DefaultTaskService implements TaskService, InitializingService, TerminatingService, DependentService, MessagingConfigProvider {
    private static class ApplyTaskCallback {
        private final int expect;

        private final Message<TaskProtocol> request;

        private final List<Object> results;

        private volatile boolean completed;

        public ApplyTaskCallback(int expect, Message<TaskProtocol> request) {
            this.expect = expect;
            this.request = request;

            results = new ArrayList<>(expect);
        }

        public boolean isCompleted() {
            return completed;
        }

        void onResult(Object result) {
            boolean ready = false;

            synchronized (this) {
                results.add(result);

                if (results.size() == expect) {
                    completed = ready = true;
                }
            }

            if (ready) {
                request.reply(new ObjectResult(results));
            }
        }

        void onError(Throwable error) {
            boolean reply = false;

            synchronized (this) {
                // Make sure that we reply only once.
                if (!completed) {
                    completed = reply = true;
                }
            }

            if (reply) {
                request.reply(new ErrorResult(error));
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DefaultTaskService.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final String CHANNEL_NAME = "hekate.tasks";

    private final boolean localExecutionEnabled;

    private final int workerThreadPoolSize;

    private final int nioThreadPoolSize;

    private final long idleSocketTimeout;

    @ToStringIgnore
    private final StateGuard guard = new StateGuard(TaskService.class);

    @ToStringIgnore
    private CodecFactory<Object> codec;

    @ToStringIgnore
    private InjectionService injector;

    @ToStringIgnore
    private MessagingService messaging;

    @ToStringIgnore
    private FilteredTaskService rootExecutor;

    public DefaultTaskService(TaskServiceFactory factory) {
        ConfigCheck check = ConfigCheck.get(TaskServiceFactory.class);

        check.notNull(factory, "factory");
        check.positive(factory.getWorkerThreads(), "worker thread pool size");

        localExecutionEnabled = factory.isLocalExecutionEnabled();
        workerThreadPoolSize = factory.getWorkerThreads();
        nioThreadPoolSize = factory.getNioThreads();
        idleSocketTimeout = factory.getIdleSocketTimeout();
        codec = factory.getTaskCodec();
    }

    @Override
    public void resolve(DependencyContext ctx) {
        messaging = ctx.require(MessagingService.class);

        if (codec == null) {
            codec = ctx.require(CodecService.class).getCodecFactory();
        }

        injector = ctx.optional(InjectionService.class);
    }

    @Override
    public Collection<MessagingChannelConfig<?>> configureMessaging() {
        MessagingChannelConfig<TaskProtocol> cfg = new MessagingChannelConfig<TaskProtocol>()
            .withName(CHANNEL_NAME)
            .withLogCategory(getClass().getName())
            .withClusterFilter(n -> n.hasService(TaskService.class))
            .withIdleTimeout(idleSocketTimeout)
            .withNioThreads(nioThreadPoolSize)
            .withWorkerThreads(workerThreadPoolSize)
            .withMessageCodec(() -> new TaskProtocolCodec(codec.createCodec()))
            .withLoadBalancer((msg, ctx) -> {
                List<ClusterNode> nodes = ctx.getNodes();

                int size = nodes.size();

                if (size == 1) {
                    return nodes.get(0).getId();
                } else {
                    // Check if this is a new failover attempt and we should re-route task to another node.
                    // In such case we remove all failed nodes from the routing topology.
                    if (ctx.getFailure().isPresent()) {
                        FailureInfo failure = ctx.getFailure().get();

                        if (!failure.getFailedNodes().isEmpty() && failure.getRouting() == RE_ROUTE) {
                            List<ClusterNode> nonFailed = new ArrayList<>(nodes);

                            // Filter out all failed nodes.
                            nonFailed.removeAll(failure.getFailedNodes());

                            if (!nonFailed.isEmpty()) {
                                size = nonFailed.size();
                                nodes = nonFailed;
                            }
                        }
                    }

                    int idx = Utils.mod(ctx.getAffinity(), size);

                    return nodes.get(idx).getId();
                }
            });

        if (localExecutionEnabled) {
            cfg.withReceiver(this::handleMessage);
        }

        return Collections.singleton(cfg);
    }

    @Override
    public void initialize(InitializationContext ctx) throws HekateException {
        guard.lockWrite();

        try {
            guard.becomeInitialized();

            if (DEBUG) {
                log.debug("Initializing...");
            }

            MessagingChannel<TaskProtocol> channel = messaging.channel(CHANNEL_NAME);

            rootExecutor = new FilteredTaskService(this, channel);

            if (DEBUG) {
                log.debug("Initialized.");
            }
        } finally {
            guard.unlockWrite();
        }
    }

    @Override
    public void terminate() throws HekateException {
        guard.lockWrite();

        try {
            if (guard.becomeTerminated()) {
                rootExecutor = null;

                if (DEBUG) {
                    log.debug("Terminated.");
                }
            }
        } finally {
            guard.unlockWrite();
        }
    }

    @Override
    public TaskService filterAll(ClusterFilter filter) {
        return getRootExecutor().filterAll(filter);
    }

    @Override
    public TaskService withFailover(FailoverPolicy policy) {
        return getRootExecutor().withFailover(policy);
    }

    @Override
    public TaskService withFailover(FailoverPolicyBuilder builder) {
        return getRootExecutor().withFailover(builder);
    }

    @Override
    public FailoverPolicy getFailover() {
        return getRootExecutor().getFailover();
    }

    @Override
    public TaskService withTimeout(long timeout, TimeUnit unit) {
        return getRootExecutor().withTimeout(timeout, unit);
    }

    @Override
    public long getTimeout() {
        return getRootExecutor().getTimeout();
    }

    @Override
    public TaskFuture<MultiNodeResult<Void>> broadcast(RunnableTask task) {
        return getRootExecutor().broadcast(task);
    }

    @Override
    public <T> TaskFuture<MultiNodeResult<T>> aggregate(CallableTask<T> task) {
        return getRootExecutor().aggregate(task);
    }

    @Override
    public TaskService withAffinity(Object affinityKey) {
        return getRootExecutor().withAffinity(affinityKey);
    }

    @Override
    public Object getAffinity() {
        return getRootExecutor().getAffinity();
    }

    @Override
    public TaskFuture<?> run(RunnableTask task) {
        return getRootExecutor().run(task);
    }

    @Override
    public <T> TaskFuture<T> call(CallableTask<T> task) {
        return getRootExecutor().call(task);
    }

    @Override
    public <T, V> TaskFuture<V> apply(T arg, ApplicableTask<T, V> task) {
        return getRootExecutor().apply(arg, task);
    }

    @Override
    public <T, V> TaskFuture<Collection<V>> applyAll(Collection<T> args, ApplicableTask<T, V> task) {
        return getRootExecutor().applyAll(args, task);
    }

    @Override
    public <T, V> TaskFuture<Collection<V>> applyAll(T[] args, ApplicableTask<T, V> task) {
        return getRootExecutor().applyAll(args, task);
    }

    private FilteredTaskService getRootExecutor() {
        guard.lockReadWithStateCheck();

        try {
            return rootExecutor;
        } finally {
            guard.unlockRead();
        }
    }

    private void handleMessage(Message<TaskProtocol> msg) {
        TaskProtocol taskMsg = msg.get();

        switch (taskMsg.getType()) {
            case RUN_TASK: {
                RunTask runTask = (RunTask)taskMsg;

                try {
                    Runnable task = runTask.getTask();

                    inject(task);

                    task.run();

                    msg.reply(NullResult.INSTANCE);
                } catch (Throwable t) {
                    msg.reply(new ErrorResult(t));
                }

                break;
            }
            case CALL_TASK: {
                CallTask callTask = (CallTask)taskMsg;

                try {
                    Callable<?> task = callTask.getTask();

                    inject(task);

                    Object result = task.call();

                    if (result == null) {
                        msg.reply(NullResult.INSTANCE);
                    } else {
                        msg.reply(new ObjectResult(result));
                    }
                } catch (Throwable t) {
                    msg.reply(new ErrorResult(t));
                }

                break;
            }
            case APPLY_TASK: {
                ApplyTask applyTask = (ApplyTask)taskMsg;

                List<Object> args = applyTask.getArgs();
                ApplicableTask<Object, Object> task = applyTask.getTask();

                if (args.size() == 1) {
                    // Execute on the same thread if this is a single argument task.
                    try {
                        inject(task);

                        Object result = task.apply(args.get(0));

                        msg.reply(new ObjectResult(Collections.singleton(result)));
                    } catch (Throwable t) {
                        msg.reply(new ErrorResult(t));
                    }
                } else {
                    // Distribute workload among threads.
                    ApplyTaskCallback callback = new ApplyTaskCallback(args.size(), msg);

                    try {
                        MessagingChannel<TaskProtocol> channel = msg.getEndpoint().getChannel();

                        inject(task);

                        for (Object arg : args) {
                            channel.getExecutor().execute(() -> {
                                try {
                                    if (!callback.isCompleted()) {
                                        Object result = task.apply(arg);

                                        callback.onResult(result);
                                    }
                                } catch (Throwable t) {
                                    callback.onError(t);
                                }
                            });
                        }
                    } catch (Throwable t) {
                        callback.onError(t);
                    }
                }

                break;
            }
            case NULL_RESULT:
            case ERROR_RESULT:
            case OBJECT_RESULT:
            default: {
                throw new IllegalArgumentException("Unexpected message type: " + taskMsg);
            }
        }
    }

    private void inject(Object obj) {
        if (injector != null) {
            injector.inject(obj);
        }
    }

    @Override
    public String toString() {
        return ToString.format(TaskService.class, this);
    }
}
