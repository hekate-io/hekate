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

package io.hekate.task;

import io.hekate.cluster.ClusterFilterSupport;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.DefaultServiceFactory;
import io.hekate.core.service.Service;
import io.hekate.failover.FailoverPolicy;
import io.hekate.failover.FailoverPolicyBuilder;
import io.hekate.inject.HekateInject;
import io.hekate.inject.InjectionService;
import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.Callable;

/**
 * <span class="startHere">&laquo; start here</span>Distributed task execution service.
 *
 * <h2>Overview</h2>
 * <p>
 * {@link TaskService} provides support for parallel execution of distributed tasks and closures on a cluster of {@link Hekate} nodes.
 * Tasks can be submitted to a single node or to multiple nodes at once (aka broadcast). Selection of target nodes is based on filtering
 * rules that can be dynamically {@link #filter(ClusterNodeFilter) specified} for each task individually.
 * </p>
 *
 * <h2>Service configuration</h2>
 * <p>
 * {@link TaskService} can be configured and registered in {@link HekateBootstrap} via {@link TaskServiceFactory} class as in the example
 * below.
 * </p>
 *
 * <p>
 * <b>Note:</b> {@link TaskService} must be configured on each node that should participate in tasks execution. Nodes that do not have this
 * service will not be selected as task execution targets.
 * </p>
 *
 * <div class="tabs">
 * <ul>
 * <li><a href="#configure-java">Java</a></li>
 * <li><a href="#configure-xsd">Spring XSD</a></li>
 * <li><a href="#configure-bean">Spring bean</a></li>
 * </ul>
 * <div id="configure-java">
 * ${source: task/TaskServiceJavadocTest.java#configure}
 * </div>
 * <div id="configure-xsd">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: task/service-xsd.xml#example}
 * </div>
 * <div id="configure-bean">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: task/service-bean.xml#example}
 * </div>
 * </div>
 *
 * <h2>Accessing service</h2>
 * <p>
 * {@link TaskService} can be accessed via {@link Hekate#get(Class)} method as in the example below:
 * ${source: task/TaskServiceJavadocTest.java#access}
 * </p>
 *
 * <h2>Runnable tasks</h2>
 * <p>
 * Runnable task represent an operation that can be executed on a cluster with the {@code void} result. Such tasks are represented by
 * {@link RunnableTask} interface (which is merely an extension of Java's {@link Runnable} with {@link Serializable} interface).
 * </p>
 *
 * <p>
 * Runnable tasks can be submitted for execution to a single node via {@link #run(RunnableTask)} method as in the example
 * below:
 * ${source: task/TaskServiceJavadocTest.java#run_task}
 * </p>
 * <p>
 * ... or can be executed on all nodes of an underlying cluster topology via {@link #broadcast(RunnableTask)}:
 * ${source: task/TaskServiceJavadocTest.java#broadcast_task}
 * </p>
 * <p>
 * <b>Notice:</b> In case of partial task failure (i.e. when task execution failed on some or all nodes) the {@link
 * #broadcast(RunnableTask)} method doesn't throw an error. Consider using the {@link MultiNodeResult} object in order to {@link
 * MultiNodeResult#getErrors() inspect failures}.
 * </p>
 *
 * <h2>Callable tasks</h2>
 * <p>
 * Callable task represent an operation that can be executed on a cluster and produces a non-{@code void} result. Such tasks are
 * represented by {@link CallableTask} interface (which is merely an extension of Java's {@link Callable} with {@link Serializable}
 * interface).
 * </p>
 *
 * <p>
 * Callable tasks can be submitted for execution to a single node via {@link #call(CallableTask)} method as in the example
 * below:
 * ${source: task/TaskServiceJavadocTest.java#call_task}
 * </p>
 *
 * <p>
 * ... or can be executed on all nodes of an underlying cluster topology via {@link #aggregate(CallableTask)}:
 * ${source: task/TaskServiceJavadocTest.java#aggregate_task}
 * </p>
 *
 * <p>
 * <b>Notice:</b> In case of partial task failure (i.e. when task execution failed on some or all nodes) the {@link
 * #aggregate(CallableTask)} method doesn't throw an error. Consider using the {@link MultiNodeResult} object in order to {@link
 * MultiNodeResult#getErrors() inspect failures}.
 * </p>
 *
 * <h2>Applicable tasks</h2>
 * <p>
 * If the same business logic should be applied to a set of homogeneous data then {@link ApplicableTask} should be used together with the
 * {@link #applyAll(Collection, ApplicableTask)} method. This method splits the specified data collection into chunks, distributes those
 * chunks
 * among the cluster nodes and {@link ApplicableTask#apply(Object) applies} the task to each data entry in parallel.
 * ${source: task/TaskServiceJavadocTest.java#apply}
 * </p>
 *
 * <h2>Routing and cluster nodes filtering</h2>
 * <p>
 * Each task can be submitted to a single node (via {@link #run(RunnableTask) run(...)}/{@link #call(CallableTask) call(...)}) or to
 * multiple nodes at once (via {@link #broadcast(RunnableTask) broadcast(...)}/{@link #aggregate(CallableTask) aggregate(...)}) for
 * parallel execution. Target nodes are selected based on cluster filtering rules. {@link TaskService} extends the {@link
 * ClusterFilterSupport} interface which provides a general purpose {@link TaskService#filter(ClusterNodeFilter)} method for dynamic
 * filtering as well as several shortcut methods for frequently used cases:
 * </p>
 * <ul>
 * <li>{@link TaskService#forRemotes()}</li>
 * <li>{@link TaskService#forRole(String)}</li>
 * <li>{@link TaskService#forProperty(String)}</li>
 * <li>{@link TaskService#forNode(ClusterNode)}</li>
 * <li>{@link TaskService#forOldest()}</li>
 * <li>{@link TaskService#forYoungest()}</li>
 * <li>...{@link ClusterFilterSupport etc}</li>
 * </ul>
 *
 * <p>
 * <b>Note:</b> if multiple nodes match the cluster filtering criteria then each single task operation (e.g. {@link #run(RunnableTask)
 * run(...)}/{@link #call(CallableTask) call(...)}) will be executed on a randomly selected node.
 * </p>
 *
 * <p>
 * Several examples below illustrate the usage of cluster nodes filtering API:
 * ${source: task/TaskServiceJavadocTest.java#filter_nodes}
 * </p>
 *
 * <h2>Task affinity</h2>
 * <p>
 * By default, all tasks that are submitted to the cluster are executed on randomly selected nodes and on randomly selected threads of
 * those nodes. If it is required to process tasks sequentially based on come criteria then task affinity key should be specified for each
 * such task. For example, if tasks are modifying some user account data then such tasks can use account ID or username as affinity key in
 * order to make sure that all tasks for particular user will always be executed on the same node and on the same thread of that node.
 * </p>
 *
 * <p>
 * The following methods can be used to execute tasks with affinity keys:
 * </p>
 * <ul>
 * <li>{@link #run(Object, RunnableTask)} - runs all tasks with same affinity key on the same node and on the same thread of that
 * node</li>
 * <li>{@link #call(Object, CallableTask)} - calls all tasks with the same affinity key on the same node and on the same thread of that
 * node</li>
 * <li>{@link #broadcast(Object, RunnableTask)} - runs tasks to all nodes and makes sure that all tasks with same affinity key are
 * always executed on the same thread of each node</li>
 * <li>{@link #aggregate(Object, CallableTask)} - calls tasks on all nodes and makes sure that all tasks with same affinity key are
 * always executed on the same thread of each node</li>
 * </ul>
 *
 * <p>
 * The first parameter of those methods is the task affinity key. {@link TaskService} uses the {@link Object#hashCode() hash code} of
 * that key to perform consistent assignment of nodes and threads for task execution.
 * </p>
 *
 *
 * <h2>Notes on tasks serialization</h2>
 * <p>
 * Tasks are serializable {@link FunctionalInterface functional interfaces} that can be specified as lambda expressions, anonymous/inner
 * classes or as regular Java classes. If task is defined as lambda or as anonymous inner class then it is important to make sure that such
 * lambda/class doesn't have references to non-static fields/methods of its enclosing class. Otherwise the enclosing class must also be
 * made
 * {@link Serializable} as it will be serialized and submitted to a remote node together with the task object.
 * </p>
 *
 * <p>
 * If task requires some data from its enclosing class then the following workarounds can me used in order to prevent serialization of
 * enclosing class:
 * </p>
 * <ul>
 * <li>Define task as a top-level or inner <i>static</i> class and keep all required data in fields of that class</li>
 * <li>Use {@link #apply(Object, ApplicableTask)} method and pass all required data as the parameter</li>
 * </ul>
 *
 * <h2>Failover</h2>
 * <p>
 * Tasks failover is controlled by the {@link FailoverPolicy} interface. Its implementations can be registered via {@link
 * #withFailover(FailoverPolicy)} method. In case of a task execution failure this interface will be called by the {@link TaskService} in
 * order to decided on whether another attempt should be performed or task execution should fail.
 * </p>
 *
 * <p>
 * <b>Important:</b> Failover is supported only for single node tasks (like {@link #run(RunnableTask) run(...)} and {@link
 * #call(CallableTask) call(...)}). It is up to the application developers to implement failover logic for broadcast
 * operations ({@link #broadcast(RunnableTask) broadcast(...)} and {@link #aggregate(CallableTask) aggregate(...)}).
 * </p>
 *
 * <p>
 * For more details and usage examples please see the documentation of {@link FailoverPolicy} interface.
 * </p>
 *
 * <h2>Dependency injection</h2>
 * <p>
 * {@link TaskService} can apply dependency injection if task class is annotated with {@link HekateInject} and {@link
 * InjectionService} is enabled. Typically {@link InjectionService} is enabled if {@link Hekate} instance is managed by an <a
 * href="https://en.wikipedia.org/wiki/Inversion_of_control" target="_blank">IoC</a> container (like
 * <a href="http://projects.spring.io/spring-framework" target="_blank">Spring Framework</a>).
 * </p>
 *
 * <p>
 * If dependency injection is enabled then task can be annotated with the framework-specific annotations (f.e. {@code @Autowired} from
 * Spring Framework). The following code show the example of how dependencies can be injected into a task:
 * ${source: task/ExampleTask.java#task}
 * </p>
 * <p>...and run this task...</p>
 * ${source: task/TaskInjectionJavadocTest.java#execute}
 */
@DefaultServiceFactory(TaskServiceFactory.class)
public interface TaskService extends Service, ClusterFilterSupport<TaskService> {
    /**
     * Asynchronously executes the specified runnable task on a randomly selected node from the underlying cluster topology.
     *
     * @param task Task to be executed.
     *
     * @return Future object that can be used to obtain result of this operation.
     */
    TaskFuture<?> run(RunnableTask task);

    /**
     * Asynchronously executes the specified runnable task on a node that is mapped to the specified affinity key.
     *
     * <p>
     * This method guarantees that all tasks with the same affinity key will always be executed on the same node (selected from
     * the {@link #filter(ClusterNodeFilter) filtered } cluster topology) unless topology doesn't change. Moreover it guarantees that
     * the target node will always use the same thread to execute such tasks.
     * </p>
     *
     * @param affinityKey Task affinity key.
     * @param task Task to be executed.
     *
     * @return Future object that can be used to obtain result of this operation.
     */
    TaskFuture<?> run(Object affinityKey, RunnableTask task);

    /**
     * Asynchronously executes the specified callable task on a randomly selected node from the underlying cluster topology.
     *
     * @param task Task to be executed.
     * @param <T> Result type.
     *
     * @return Future object that can be used to obtain result of this operation.
     */
    <T> TaskFuture<T> call(CallableTask<T> task);

    /**
     * Asynchronously executes the specified runnable task on a node that is mapped to the specified affinity key.
     *
     * <p>
     * This method guarantees that all tasks with the same affinity key will always be executed on the same node (selected from
     * the {@link #filter(ClusterNodeFilter) filtered } cluster topology) unless topology doesn't change. Moreover it guarantees that
     * the target node will always use the same thread to execute such tasks.
     * </p>
     *
     * @param affinityKey Task affinity key.
     * @param task Task to be executed.
     * @param <T> Result type.
     *
     * @return Future object that can be used to obtain result of this operation.
     */
    <T> TaskFuture<T> call(Object affinityKey, CallableTask<T> task);

    /**
     * Asynchronously applies the specified task to the single argument on a randomly selected node of the underlying cluster topology.
     *
     * @param arg Single argument.
     * @param task Task.
     * @param <T> Argument type.
     * @param <V> Result type.
     *
     * @return Future object that can be used to obtain result of this operation.
     *
     * @see #applyAll(Collection, ApplicableTask)
     */
    <T, V> TaskFuture<V> apply(T arg, ApplicableTask<T, V> task);

    /**
     * Asynchronously applies the specified task to the single argument on a node that is mapped to the specified affinity key.
     *
     * <p>
     * This method guarantees that all tasks with the same affinity key will always be executed on the same node (selected from
     * the {@link #filter(ClusterNodeFilter) filtered } cluster topology) unless topology doesn't change. Moreover it guarantees that
     * the target node will always use the same thread to execute such tasks.
     * </p>
     *
     * @param affinityKey Task affinity key.
     * @param arg Single argument.
     * @param task Task.
     * @param <T> Argument type.
     * @param <V> Result type.
     *
     * @return Future object that can be used to obtain result of this operation.
     *
     * @see #applyAll(Collection, ApplicableTask)
     */
    <T, V> TaskFuture<V> apply(Object affinityKey, T arg, ApplicableTask<T, V> task);

    /**
     * Asynchronously applies the specified task to each of the specified arguments by distributing the workload among the cluster nodes.
     *
     * @param args Arguments.
     * @param task Task.
     * @param <T> Argument type.
     * @param <V> Result type.
     *
     * @return Future object that can be used to obtain result of this operation.
     *
     * @see #apply(Object, ApplicableTask)
     */
    <T, V> TaskFuture<Collection<V>> applyAll(Collection<T> args, ApplicableTask<T, V> task);

    /**
     * Asynchronously applies the specified task to each of the specified arguments by distributing the workload among the cluster nodes.
     *
     * @param args Arguments.
     * @param task Task.
     * @param <T> Argument type.
     * @param <V> Result type.
     *
     * @return Future object that can be used to obtain result of this operation.
     *
     * @see #apply(Object, ApplicableTask)
     */
    <T, V> TaskFuture<Collection<V>> applyAll(T[] args, ApplicableTask<T, V> task);

    /**
     * Asynchronously executes the specified runnable task on all nodes from the underlying cluster topology.
     *
     * <p>
     * <b>Notice:</b> this method doesn't throw an error in case of partial task failure (i.e. when task execution failed on some nodes).
     * Consider using the {@link MultiNodeResult} object to {@link MultiNodeResult#getErrors() inspect failures}.
     * </p>
     *
     * @param task Task to be executed.
     *
     * @return Future object that can be used to obtain result of this operation.
     */
    TaskFuture<MultiNodeResult<Void>> broadcast(RunnableTask task);

    /**
     * Asynchronously executes the specified runnable task on all nodes from the underlying cluster topology.
     *
     * <p>
     * This method guarantees that every node will always use the same thread to process tasks that were submitted with the same affinity
     * key.
     * </p>
     *
     * <p>
     * <b>Notice:</b> this method doesn't throw an error in case of partial task failure (i.e. when task execution failed on some nodes).
     * Consider using the {@link MultiNodeResult} object to {@link MultiNodeResult#getErrors() inspect failures}.
     * </p>
     *
     * @param affinityKey Task affinity key.
     * @param task Task to be executed.
     *
     * @return Future object that can be used to obtain result of this operation.
     */
    TaskFuture<MultiNodeResult<Void>> broadcast(Object affinityKey, RunnableTask task);

    /**
     * Asynchronously executes the specified callable task on all nodes from the underlying cluster topology.
     *
     * <p>
     * <b>Notice:</b> this method doesn't throw an error in case of partial task failure (i.e. when task execution failed on some nodes).
     * Consider using the {@link MultiNodeResult} object to {@link MultiNodeResult#getErrors() inspect failures}.
     * </p>
     *
     * @param task Task to be executed.
     * @param <T> Result type.
     *
     * @return Future object that can be used to obtain result of this operation.
     */
    <T> TaskFuture<MultiNodeResult<T>> aggregate(CallableTask<T> task);

    /**
     * Asynchronously executes the specified callable task on all nodes from the underlying cluster topology.
     *
     * <p>
     * This method guarantees that every node will always use the same thread to process tasks that were submitted with the same affinity
     * key.
     * </p>
     *
     * <p>
     * <b>Notice:</b> this method doesn't throw an error in case of partial task failure (i.e. when task execution failed on some nodes).
     * Consider using the {@link MultiNodeResult} object to {@link MultiNodeResult#getErrors() inspect failures}.
     * </p>
     *
     * @param affinityKey Task affinity key.
     * @param task Task to be executed.
     * @param <T> Result type.
     *
     * @return Future object that can be used to obtain result of this operation.
     */
    <T> TaskFuture<MultiNodeResult<T>> aggregate(Object affinityKey, CallableTask<T> task);

    /**
     * Returns {@code true} if this service instance has a {@link #filter(ClusterNodeFilter) filter}.
     *
     * @return {@code true} if this service instance has a {@link #filter(ClusterNodeFilter) filter}.
     */
    boolean hasFilter();

    /**
     * Returns a new lightweight wrapper that will use the specified failover policy and will inherit all cluster filtering options from
     * this instance.
     *
     * @param policy Failover policy.
     *
     * @return Wrapper of this instance that will use the specified failover policy.
     */
    TaskService withFailover(FailoverPolicy policy);

    /**
     * Returns a new lightweight wrapper that will use the specified failover policy and will inherit all cluster filtering options from
     * this instance.
     *
     * @param builder Failover policy builder.
     *
     * @return Wrapper of this instance that will use the specified failover policy.
     */
    TaskService withFailover(FailoverPolicyBuilder builder);
}
