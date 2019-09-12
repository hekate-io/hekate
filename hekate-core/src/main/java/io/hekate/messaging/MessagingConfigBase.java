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

package io.hekate.messaging;

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.network.NetworkService;
import io.hekate.network.NetworkServiceFactory;
import java.util.function.Consumer;

/**
 * Common configuration options for components that provide messaging support.
 *
 * @param <T> Syb-class type.
 */
public abstract class MessagingConfigBase<T extends MessagingConfigBase<T>> {
    /** See {@link #setNioThreads(int)}. */
    private int nioThreads;

    /** See {@link #setIdleSocketTimeout(long)}. */
    private long idleSocketTimeout;

    /** See {@link #setBackPressure(MessagingBackPressureConfig)}. */
    private MessagingBackPressureConfig backPressure = new MessagingBackPressureConfig();

    /**
     * Returns the idle socket timeout in milliseconds (see {@link #setIdleSocketTimeout(long)}).
     *
     * @return Idle socket timeout in milliseconds.
     */
    public long getIdleSocketTimeout() {
        return idleSocketTimeout;
    }

    /**
     * Sets idle socket timeout in milliseconds.
     *
     * <p>
     * If there were no communication with some remote node for the duration of this time interval then all sockets connections with such
     * node will be closed in order to save system resource. Connections will be automatically reestablish on the next attempt to send a
     * message to that node.
     * </p>
     *
     * <p>
     * If value of this parameter is less than or equals to zero (default value) then connections will not be closed while remote node stays
     * alive.
     * </p>
     *
     * @param idleSocketTimeout Timeout in milliseconds.
     */
    public void setIdleSocketTimeout(long idleSocketTimeout) {
        this.idleSocketTimeout = idleSocketTimeout;
    }

    /**
     * Fluent-style version of {@link #setIdleSocketTimeout(long)}.
     *
     * @param idleTimeout Timeout in milliseconds.
     *
     * @return This instance.
     */
    public T withIdleSocketTimeout(long idleTimeout) {
        setIdleSocketTimeout(idleTimeout);

        return self();
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
     * If this parameter is less than or equals to zero (default value) then this channel will use the core thread pool of
     * {@link NetworkService} (see {@link NetworkServiceFactory#setNioThreads(int)}).
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
    public T withNioThreads(int nioThreads) {
        setNioThreads(nioThreads);

        return self();
    }

    /**
     * Returns the back pressure configuration (see {@link #setBackPressure(MessagingBackPressureConfig)}).
     *
     * @return Back pressure configuration.
     */
    public MessagingBackPressureConfig getBackPressure() {
        return backPressure;
    }

    /**
     * Sets the back pressure configuration.
     *
     * <p>
     * If not specified then {@link MessagingBackPressureConfig}'s defaults will be used.
     * </p>
     *
     * @param backPressure Back pressure configuration.
     */
    public void setBackPressure(MessagingBackPressureConfig backPressure) {
        ArgAssert.notNull(backPressure, "Back pressure configuration");

        this.backPressure = backPressure;
    }

    /**
     * Fluent-style version of {@link #setBackPressure(MessagingBackPressureConfig)}.
     *
     * @param backPressure Back pressure configuration.
     *
     * @return This instance.
     */
    public T withBackPressure(MessagingBackPressureConfig backPressure) {
        setBackPressure(backPressure);

        return self();
    }

    /**
     * Applies the specified consumer to the current {@link #getBackPressure()} configuration.
     *
     * @param configurer Configuration Consumer.
     *
     * @return This instance.
     */
    public T withBackPressure(Consumer<MessagingBackPressureConfig> configurer) {
        configurer.accept(getBackPressure());

        return self();
    }

    @SuppressWarnings("unchecked")
    private T self() {
        return (T)this;
    }
}
