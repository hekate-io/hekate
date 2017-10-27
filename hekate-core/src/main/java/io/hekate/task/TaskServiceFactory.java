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

import io.hekate.codec.CodecFactory;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.ServiceFactory;
import io.hekate.network.NetworkService;
import io.hekate.network.NetworkServiceFactory;
import io.hekate.task.internal.DefaultTaskService;
import io.hekate.util.format.ToString;

/**
 * Configurable factory for {@link TaskService}.
 *
 * <p>
 * This class represents a configurable factory for {@link TaskService}. Instances of this class can be
 * {@link HekateBootstrap#withService(ServiceFactory) registered} within the {@link HekateBootstrap} in order to customize options of the
 * {@link TaskService}.
 * </p>
 *
 * <p>
 * For more details about the {@link TaskService} and its capabilities please see the documentation of {@link TaskService} interface.
 * </p>
 *
 * @see TaskService
 */
public class TaskServiceFactory implements ServiceFactory<TaskService> {
    private boolean localExecutionEnabled = true;

    private int workerThreads = Runtime.getRuntime().availableProcessors();

    private int nioThreads;

    private int idleSocketTimeout;

    private CodecFactory<Object> taskCodec;

    /**
     * Returns <tt>true</tt> if local execution of tasks is enabled (see {@link #setLocalExecutionEnabled(boolean)}).
     *
     * @return <tt>true</tt> if local execution of tasks is enabled.
     */
    public boolean isLocalExecutionEnabled() {
        return localExecutionEnabled;
    }

    /**
     * Sets the flag indicating that task execution should be enabled on this node.
     *
     * <p>
     * If execution of tasks is disabled then the service will be able to submit tasks to remote nodes but will not accepts tasks from them
     * (i.e. service will act as a client).
     * </p>
     *
     * <p>
     * Default value of this parameter is {@code true}.
     * </p>
     *
     * @param localExecutionEnabled {@code true} to enable or {@code false} to disable execution of tasks on the local node.
     */
    public void setLocalExecutionEnabled(boolean localExecutionEnabled) {
        this.localExecutionEnabled = localExecutionEnabled;
    }

    /**
     * Fluent-style version of {@link #setLocalExecutionEnabled(boolean)}.
     *
     * @param localExecutionEnabled {@code true} to enable or {@code false} to disable execution of tasks on the local node.
     *
     * @return This instance.
     */
    public TaskServiceFactory withLocalExecutionEnabled(boolean localExecutionEnabled) {
        setLocalExecutionEnabled(localExecutionEnabled);

        return this;
    }

    /**
     * Returns the worker thread pool size (see {@link #setWorkerThreads(int)}).
     *
     * @return Worker thread pool size
     */
    public int getWorkerThreads() {
        return workerThreads;
    }

    /**
     * Sets the worker thread pool size for task execution.
     *
     * <p>
     * The thread pool of this size will be used by the {@link TaskService} to perform all task executions and callback notification.
     * </p>
     *
     * <p>
     * Value of this parameter must be above zero. If not directly specified then the number of available CPUs (see {@link
     * Runtime#availableProcessors()}) will be used as the default value.
     * </p>
     *
     * @param workerThreads Worker thread pool size.
     */
    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    /**
     * Fluent-style version of {@link #setWorkerThreads(int)}.
     *
     * @param workerThreads Worker thread pool size.
     *
     * @return This instance.
     */
    public TaskServiceFactory withWorkerThreads(int workerThreads) {
        setWorkerThreads(workerThreads);

        return this;
    }

    /**
     * Returns the idle socket timeout in milliseconds (see {@link #setIdleSocketTimeout(int)}).
     *
     * @return Timeout in milliseconds.
     */
    public int getIdleSocketTimeout() {
        return idleSocketTimeout;
    }

    /**
     * Sets the idle socket timeout in milliseconds.
     *
     * <p>
     * If there were no task submission to some remote node for the duration of this time interval then sockets connections with such
     * node will be closed in order to save system resource. Connections will be automatically re-establish on the next attempt to submit a
     * task to that node.
     * </p>
     *
     * <p>
     * If value of this parameter is less than or equals to zero (default value) then connections will not be closed will until remote node
     * stays alive.
     * </p>
     *
     * @param idleSocketTimeout Timeout in milliseconds.
     */
    public void setIdleSocketTimeout(int idleSocketTimeout) {
        this.idleSocketTimeout = idleSocketTimeout;
    }

    /**
     * Fluent-style version of {@link #setIdleSocketTimeout(int)}.
     *
     * @param idlePoolTimeout Timeout in milliseconds.
     *
     * @return This instance.
     */
    public TaskServiceFactory withIdleSocketTimeout(int idlePoolTimeout) {
        setIdleSocketTimeout(idlePoolTimeout);

        return this;
    }

    /**
     * Returns the size of a thread pool for handling NIO-based socket connections (see {@link #setNioThreads(int)}).
     *
     * @return Size of a thread pool for handling NIO-based socket connections.
     */
    public int getNioThreads() {
        return nioThreads;
    }

    /**
     * Sets the size of a thread pool for handling NIO-based socket connections.
     *
     * <p>
     * If this parameter is less than or equals to zero (default value) then all socket connections will be handled by the core thread
     * pool of {@link NetworkService} (see {@link NetworkServiceFactory#setNioThreads(int)}).
     * </p>
     *
     * @param nioThreads Size of a thread pool for handling NIO-based socket connections.
     */
    public void setNioThreads(int nioThreads) {
        this.nioThreads = nioThreads;
    }

    /**
     * Fluent-style version of {@link #setNioThreads(int)}.
     *
     * @param nioThreads Size of a thread pool for handling NIO-based socket connections.
     *
     * @return This instance.
     */
    public TaskServiceFactory withNioThreads(int nioThreads) {
        setNioThreads(nioThreads);

        return this;
    }

    /**
     * Returns the task codec factory to be used for task objects serialization (see {@link #setTaskCodec(CodecFactory)}).
     *
     * @return Task codec factory.
     */
    public CodecFactory<Object> getTaskCodec() {
        return taskCodec;
    }

    /**
     * Sets the task codec factory to be used for task objects serialization.
     *
     * <p>
     * This parameter is optional and if not specified then the {@link HekateBootstrap#setDefaultCodec(CodecFactory) default} codec will be
     * used for task objects serialization.
     * </p>
     *
     * @param taskCodec Task codec factory.
     */
    public void setTaskCodec(CodecFactory<Object> taskCodec) {
        this.taskCodec = taskCodec;
    }

    /**
     * Fluent-style version of {@link #setTaskCodec(CodecFactory)}.
     *
     * @param taskCodec Task codec factory.
     *
     * @return This instance.
     */
    public TaskServiceFactory withTaskCodec(CodecFactory<Object> taskCodec) {
        setTaskCodec(taskCodec);

        return this;
    }

    @Override
    public TaskService createService() {
        return new DefaultTaskService(this);
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
