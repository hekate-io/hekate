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
import io.hekate.cluster.ClusterNodeId;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecService;
import io.hekate.core.HekateException;
import io.hekate.core.inject.InjectionService;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.service.DependencyContext;
import io.hekate.core.service.DependentService;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.InitializingService;
import io.hekate.core.service.TerminatingService;
import io.hekate.failover.FailoverPolicy;
import io.hekate.failover.FailoverPolicyBuilder;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessagingBackPressureConfig;
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
import io.hekate.task.internal.TaskProtocol.ApplyBulkTask;
import io.hekate.task.internal.TaskProtocol.ApplySingleTask;
import io.hekate.task.internal.TaskProtocol.CallTask;
import io.hekate.task.internal.TaskProtocol.ErrorResult;
import io.hekate.task.internal.TaskProtocol.NullResult;
import io.hekate.task.internal.TaskProtocol.ObjectResult;
import io.hekate.task.internal.TaskProtocol.RunTask;
import io.hekate.util.StateGuard;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultTaskService implements TaskService, InitializingService, TerminatingService, DependentService, MessagingConfigProvider {
    private static final Logger log = LoggerFactory.getLogger(DefaultTaskService.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final String CHANNEL_NAME = "hekate.tasks";

    private final boolean localExecutionEnabled;

    private final int workerThreadPoolSize;

    private final int nioThreadPoolSize;

    private final long idleSocketTimeout;

    private final MessagingBackPressureConfig backPressure;

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
        backPressure = new MessagingBackPressureConfig(factory.getBackPressure());
        codec = factory.getTaskCodec();
    }

    @Override
    public void resolve(DependencyContext ctx) {
        messaging = ctx.require(MessagingService.class);

        if (codec == null) {
            codec = ctx.require(CodecService.class).codecFactory();
        }

        injector = ctx.optional(InjectionService.class);
    }

    @Override
    public Collection<MessagingChannelConfig<?>> configureMessaging() {
        MessagingChannelConfig<TaskProtocol> cfg = MessagingChannelConfig.of(TaskProtocol.class)
            .withName(CHANNEL_NAME)
            .withLogCategory(getClass().getName())
            .withClusterFilter(n -> n.hasService(TaskService.class))
            .withIdleSocketTimeout(idleSocketTimeout)
            .withNioThreads(nioThreadPoolSize)
            .withWorkerThreads(workerThreadPoolSize)
            .withBackPressure(backPressure)
            .withMessageCodec(() -> new TaskProtocolCodec(codec.createCodec()));

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

            MessagingChannel<TaskProtocol> channel = messaging.channel(CHANNEL_NAME, TaskProtocol.class);

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
        return rootExecutor().filterAll(filter);
    }

    @Override
    public TaskService withFailover(FailoverPolicy policy) {
        return rootExecutor().withFailover(policy);
    }

    @Override
    public TaskService withFailover(FailoverPolicyBuilder builder) {
        return rootExecutor().withFailover(builder);
    }

    @Override
    public FailoverPolicy failover() {
        return rootExecutor().failover();
    }

    @Override
    public TaskService withTimeout(long timeout, TimeUnit unit) {
        return rootExecutor().withTimeout(timeout, unit);
    }

    @Override
    public long timeout() {
        return rootExecutor().timeout();
    }

    @Override
    public TaskFuture<MultiNodeResult<Void>> broadcast(RunnableTask task) {
        return rootExecutor().broadcast(task);
    }

    @Override
    public <T> TaskFuture<MultiNodeResult<T>> aggregate(CallableTask<T> task) {
        return rootExecutor().aggregate(task);
    }

    @Override
    public TaskService withAffinity(Object affinityKey) {
        return rootExecutor().withAffinity(affinityKey);
    }

    @Override
    public Object affinity() {
        return rootExecutor().affinity();
    }

    @Override
    public TaskFuture<?> run(RunnableTask task) {
        return rootExecutor().run(task);
    }

    @Override
    public <T> TaskFuture<T> call(CallableTask<T> task) {
        return rootExecutor().call(task);
    }

    @Override
    public <T, V> TaskFuture<V> apply(T arg, ApplicableTask<T, V> task) {
        return rootExecutor().apply(arg, task);
    }

    @Override
    public <T, V> TaskFuture<Collection<V>> applyToAll(Collection<T> args, ApplicableTask<T, V> task) {
        return rootExecutor().applyToAll(args, task);
    }

    @Override
    public <T, V> TaskFuture<Collection<V>> applyToAll(T[] args, ApplicableTask<T, V> task) {
        return rootExecutor().applyToAll(args, task);
    }

    private FilteredTaskService rootExecutor() {
        guard.lockReadWithStateCheck();

        try {
            return rootExecutor;
        } finally {
            guard.unlockRead();
        }
    }

    private void handleMessage(Message<TaskProtocol> msg) {
        TaskProtocol taskMsg = msg.get();

        ClusterNodeId from = msg.from();

        switch (taskMsg.type()) {
            case RUN_TASK: {
                RunTask runTask = (RunTask)taskMsg;

                Runnable task = runTask.task();

                try {
                    inject(task);

                    task.run();

                    msg.reply(NullResult.INSTANCE);
                } catch (Throwable t) {
                    if (log.isErrorEnabled()) {
                        log.error("Failed to execute runnable task [from-node-id={}, task={}]", from, task, t);
                    }

                    msg.reply(new ErrorResult(t));
                }

                break;
            }
            case CALL_TASK: {
                CallTask callTask = (CallTask)taskMsg;

                Callable<?> task = callTask.task();

                try {

                    inject(task);

                    Object result = task.call();

                    if (result == null) {
                        msg.reply(NullResult.INSTANCE);
                    } else {
                        msg.reply(new ObjectResult(result));
                    }
                } catch (Throwable t) {
                    if (log.isErrorEnabled()) {
                        log.error("Failed to execute callable task [from-node-id={}, task={}]", from, task, t);
                    }

                    msg.reply(new ErrorResult(t));
                }

                break;
            }
            case APPLY_SINGLE_TASK: {
                ApplySingleTask applyBulkTask = (ApplySingleTask)taskMsg;

                ApplicableTask<Object, ?> task = applyBulkTask.task();

                Object arg = applyBulkTask.arg();

                try {
                    inject(task);

                    Object result = task.apply(arg);

                    msg.reply(new ObjectResult(result));
                } catch (Throwable t) {
                    if (log.isErrorEnabled()) {
                        log.error("Failed to execute apply task [from-node-id={}, task={}, arg={}]", from, task, arg, t);
                    }

                    msg.reply(new ErrorResult(t));
                }

                break;
            }
            case APPLY_BULK_TASK: {
                ApplyBulkTask applyBulkTask = (ApplyBulkTask)taskMsg;

                ApplicableTask<Object, Object> task = applyBulkTask.task();

                List<Object> args = applyBulkTask.args();

                if (args.size() == 1) {
                    // Execute on the same thread if this is a single argument task.
                    Object arg = args.get(0);

                    try {
                        inject(task);

                        Object result = task.apply(arg);

                        msg.reply(new ObjectResult(Collections.singleton(result)));
                    } catch (Throwable t) {
                        if (log.isErrorEnabled()) {
                            log.error("Failed to execute apply task [from-node-id={}, task={}, arg={}]", from, task, arg, t);
                        }

                        msg.reply(new ErrorResult(t));
                    }
                } else {
                    // Distribute workload among threads.
                    ApplyTaskCallback callback = new ApplyTaskCallback(args.size(), msg);

                    MessagingChannel<TaskProtocol> channel = msg.endpoint().channel();

                    try {
                        inject(task);

                        for (Object arg : args) {
                            channel.executor().execute(() -> {
                                try {
                                    if (!callback.isCompleted()) {
                                        Object result = task.apply(arg);

                                        callback.onResult(result);
                                    }
                                } catch (Throwable t) {
                                    if (log.isErrorEnabled()) {
                                        log.error("Failed to execute apply task [from-node-id={}, task={}, arg={}]", from, task, arg, t);
                                    }

                                    callback.onError(t);
                                }
                            });
                        }
                    } catch (Throwable t) {
                        if (log.isErrorEnabled()) {
                            log.error("Failed to execute multi-arg apply task [from-node-id={}, task={}]", from, task, t);
                        }

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
