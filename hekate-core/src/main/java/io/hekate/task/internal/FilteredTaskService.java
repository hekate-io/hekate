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
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopology;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.failover.FailoverPolicy;
import io.hekate.failover.FailoverPolicyBuilder;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.UnknownRouteException;
import io.hekate.messaging.broadcast.AggregateCallback;
import io.hekate.messaging.broadcast.AggregateResult;
import io.hekate.messaging.unicast.Reply;
import io.hekate.messaging.unicast.RequestCallback;
import io.hekate.task.ApplicableTask;
import io.hekate.task.CallableTask;
import io.hekate.task.MultiNodeResult;
import io.hekate.task.RunnableTask;
import io.hekate.task.TaskException;
import io.hekate.task.TaskFuture;
import io.hekate.task.TaskService;
import io.hekate.task.internal.TaskProtocol.ApplyTask;
import io.hekate.task.internal.TaskProtocol.ObjectResult;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

class FilteredTaskService implements TaskService {
    private static class SingleTaskFuture<T> extends TaskFuture<T> implements RequestCallback<TaskProtocol> {
        private final Object task;

        public SingleTaskFuture(Object task) {
            this.task = task;
        }

        @Override
        public void onComplete(Throwable err, Reply<TaskProtocol> reply) {
            if (err == null) {
                TaskProtocol taskReply = reply.get();

                TaskProtocol.Type type = taskReply.getType();

                if (type == TaskProtocol.Type.NULL_RESULT) {
                    complete(null);
                } else {
                    ObjectResult resultMsg = (ObjectResult)taskReply;

                    @SuppressWarnings("unchecked")
                    T result = (T)resultMsg.getResult();

                    complete(result);
                }
            } else {
                completeExceptionally(new TaskException("Task execution failed [task=" + task + ']', err));
            }
        }
    }

    private static class MultiNodeTaskFuture<T> extends TaskFuture<MultiNodeResult<T>> implements AggregateCallback<TaskProtocol> {
        private final Object task;

        public MultiNodeTaskFuture(Object task) {
            this.task = task;
        }

        @Override
        public void onComplete(Throwable err, AggregateResult<TaskProtocol> result) {
            if (err == null) {
                Map<ClusterNode, Reply<TaskProtocol>> responses = result.getReplies();

                Map<ClusterNode, Throwable> errors = new HashMap<>(result.getErrors());
                Map<ClusterNode, T> values = new HashMap<>(responses.size(), 1.0f);

                responses.forEach((node, msg) -> {
                    TaskProtocol taskReply = msg.get();

                    TaskProtocol.Type type = taskReply.getType();

                    if (type == TaskProtocol.Type.NULL_RESULT) {
                        values.put(node, null);
                    } else {
                        ObjectResult resultMsg = (ObjectResult)taskReply;

                        @SuppressWarnings("unchecked")
                        T value = (T)resultMsg.getResult();

                        values.put(node, value);
                    }
                });

                complete(new DefaultMultiNodeResult<>(result.getNodes(), errors, values));
            } else {
                completeExceptionally(new TaskException("Task execution failed [task=" + task + ']', err));
            }
        }
    }

    private static class ApplyTaskFuture<V> extends TaskFuture<Collection<V>> implements RequestCallback<TaskProtocol> {
        private final Object task;

        private final List<V> result;

        private final int expected;

        private int received;

        public ApplyTaskFuture(Object task, int expected, int capacity) {
            this.task = task;
            this.expected = expected;
            this.result = new ArrayList<>(capacity);
        }

        @Override
        public void onComplete(Throwable err, Reply<TaskProtocol> reply) {
            if (err == null) {
                if (!isDone()) {
                    @SuppressWarnings("unchecked")
                    Collection<V> part = (Collection<V>)reply.get(ObjectResult.class).getResult();

                    boolean allReceived;

                    synchronized (result) {
                        received++;

                        result.addAll(part);

                        allReceived = received == expected;
                    }

                    if (allReceived) {
                        complete(result);
                    }
                }
            } else {
                completeExceptionally(new TaskException("Task execution failed [task=" + task + ']', err));
            }
        }
    }

    @ToStringIgnore
    private final ClusterFilter filter;

    @ToStringIgnore
    private final TaskService parent;

    @ToStringIgnore
    private final MessagingChannel<TaskProtocol> channel;

    public FilteredTaskService(TaskService parent, MessagingChannel<TaskProtocol> channel) {
        this(parent, channel, null);
    }

    private FilteredTaskService(TaskService parent, MessagingChannel<TaskProtocol> channel, ClusterFilter filter) {
        assert parent != null : "Parent is null.";
        assert channel != null : "Channel is null.";

        this.parent = parent;
        this.filter = filter;
        this.channel = channel;
    }

    @Override
    public TaskService filterAll(ClusterFilter newFilter) {
        ClusterFilter childFilter;

        if (filter == null) {
            childFilter = newFilter;
        } else {
            childFilter = ClusterFilter.and(filter, newFilter);
        }

        return new FilteredTaskService(this, channel.filterAll(childFilter), childFilter);
    }

    @Override
    public boolean hasFilter() {
        return filter != null;
    }

    @Override
    public TaskService getUnfiltered() {
        return parent.getUnfiltered();
    }

    @Override
    public TaskService withFailover(FailoverPolicy policy) {
        return new FilteredTaskService(this, channel.withFailover(policy), filter);
    }

    @Override
    public TaskService withFailover(FailoverPolicyBuilder builder) {
        return new FilteredTaskService(this, channel.withFailover(builder), filter);
    }

    @Override
    public TaskFuture<MultiNodeResult<Void>> broadcast(RunnableTask task) {
        ArgAssert.notNull(task, "Task");

        MultiNodeTaskFuture<Void> future = new MultiNodeTaskFuture<>(task);

        channel.aggregate(new TaskProtocol.RunTask(task), future);

        return future;
    }

    @Override
    public TaskFuture<MultiNodeResult<Void>> broadcast(Object affinityKey, RunnableTask task) {
        ArgAssert.notNull(affinityKey, "Affinity key");
        ArgAssert.notNull(task, "Task");

        MultiNodeTaskFuture<Void> future = new MultiNodeTaskFuture<>(task);

        channel.affinityAggregate(affinityKey, new TaskProtocol.RunTask(task), future);

        return future;
    }

    @Override
    public <T> TaskFuture<MultiNodeResult<T>> aggregate(CallableTask<T> task) {
        ArgAssert.notNull(task, "Task");

        MultiNodeTaskFuture<T> future = new MultiNodeTaskFuture<>(task);

        channel.aggregate(new TaskProtocol.CallTask(task), future);

        return future;
    }

    @Override
    public <T> TaskFuture<MultiNodeResult<T>> aggregate(Object affinityKey, CallableTask<T> task) {
        ArgAssert.notNull(affinityKey, "Affinity key");
        ArgAssert.notNull(task, "Task");

        MultiNodeTaskFuture<T> future = new MultiNodeTaskFuture<>(task);

        channel.affinityAggregate(affinityKey, new TaskProtocol.CallTask(task), future);

        return future;
    }

    @Override
    public TaskFuture<?> run(RunnableTask task) {
        ArgAssert.notNull(task, "Task");

        SingleTaskFuture<Object> future = new SingleTaskFuture<>(task);

        channel.request(new TaskProtocol.RunTask(task), future);

        return future;
    }

    @Override
    public TaskFuture<?> run(Object affinityKey, RunnableTask task) {
        ArgAssert.notNull(affinityKey, "Affinity key");
        ArgAssert.notNull(task, "Task");

        SingleTaskFuture<Object> future = new SingleTaskFuture<>(task);

        channel.affinityRequest(affinityKey, new TaskProtocol.RunTask(task), future);

        return future;
    }

    @Override
    public <T> TaskFuture<T> call(CallableTask<T> task) {
        ArgAssert.notNull(task, "Task");

        SingleTaskFuture<T> future = new SingleTaskFuture<>(task);

        channel.request(new TaskProtocol.CallTask(task), future);

        return future;
    }

    @Override
    public <T> TaskFuture<T> call(Object affinityKey, CallableTask<T> task) {
        ArgAssert.notNull(affinityKey, "Affinity key");
        ArgAssert.notNull(task, "Task");

        SingleTaskFuture<T> future = new SingleTaskFuture<>(task);

        channel.affinityRequest(affinityKey, new TaskProtocol.CallTask(task), future);

        return future;
    }

    @Override
    public <T, V> TaskFuture<V> apply(T arg, ApplicableTask<T, V> task) {
        ArgAssert.notNull(arg, "Argument");
        ArgAssert.notNull(task, "Task");

        return call(() -> task.apply(arg));
    }

    @Override
    public <T, V> TaskFuture<V> apply(Object affinityKey, T arg, ApplicableTask<T, V> task) {
        ArgAssert.notNull(affinityKey, "Affinity key.");
        ArgAssert.notNull(arg, "Argument");
        ArgAssert.notNull(task, "Task");

        return call(affinityKey, () -> task.apply(arg));
    }

    @Override
    public <T, V> TaskFuture<Collection<V>> applyAll(T[] args, ApplicableTask<T, V> task) {
        ArgAssert.notNull(args, "Task arguments list");

        return applyAll(Arrays.asList(args), task);
    }

    @Override
    public <T, V> TaskFuture<Collection<V>> applyAll(Collection<T> args, ApplicableTask<T, V> task) {
        ArgAssert.notNull(args, "Task arguments list");
        ArgAssert.isFalse(args.isEmpty(), "Task arguments");
        ArgAssert.notNull(task, "Task");

        ClusterTopology nodes = channel.getCluster().getTopology();

        if (nodes.isEmpty()) {
            TaskFuture<Collection<V>> future = new TaskFuture<>();

            future.completeExceptionally(new UnknownRouteException("No suitable task executors in the cluster topology."));

            return future;
        }

        @SuppressWarnings("unchecked")
        ApplicableTask<Object, Object> untypedTask = (ApplicableTask<Object, Object>)task;

        int argsSize = args.size();
        int nodesSize = nodes.size();

        int taskCapacity = argsSize / nodesSize + 1;

        Map<ClusterNodeId, ApplyTask> msgByNode = new HashMap<>(Math.min(nodesSize, argsSize), 1.0f);

        for (Iterator<T> argIt = args.iterator(); argIt.hasNext(); ) {
            for (Iterator<ClusterNode> topologyIt = nodes.iterator(); topologyIt.hasNext() && argIt.hasNext(); ) {
                ClusterNodeId nodeId = topologyIt.next().getId();
                T arg = argIt.next();

                ArgAssert.check(arg != null, "Arguments collection contains null value.");

                ApplyTask msg = msgByNode.get(nodeId);

                if (msg == null) {
                    List<Object> batchArgs = new ArrayList<>(taskCapacity);

                    msg = new ApplyTask(untypedTask, batchArgs);

                    msgByNode.put(nodeId, msg);
                }

                msg.getArgs().add(arg);
            }
        }

        ApplyTaskFuture<V> future = new ApplyTaskFuture<>(task, msgByNode.size(), argsSize);

        int affinity = 0;

        for (ApplyTask msg : msgByNode.values()) {
            channel.affinityRequest(affinity, msg, future);

            affinity++;
        }

        return future;
    }

    @Override
    public String toString() {
        return getUnfiltered().toString();
    }
}
