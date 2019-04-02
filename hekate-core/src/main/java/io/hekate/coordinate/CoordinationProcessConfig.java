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

package io.hekate.coordinate;

import io.hekate.codec.CodecFactory;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.util.format.ToString;
import java.util.List;

/**
 * Configuration for {@link CoordinationProcess}.
 *
 * <p>
 * Instances of this class can be registered via {@link CoordinationServiceFactory#setProcesses(List)} method.
 * </p>
 *
 * @see CoordinationService
 */
public class CoordinationProcessConfig {
    /** See {@link #setName(String)}. */
    private String name;

    /** See {@link #setAsyncInit(boolean)}. */
    private boolean asyncInit = true;

    /** See {@link #setHandler(CoordinationHandler)}. */
    private CoordinationHandler handler;

    /** See {@link #setMessageCodec(CodecFactory)}. */
    private CodecFactory<Object> messageCodec;

    /**
     * Default constructor.
     */
    public CoordinationProcessConfig() {
        // No-op.
    }

    /**
     * Constructs new instance.
     *
     * @param name Process name (see {@link #setName(String)}).
     */
    public CoordinationProcessConfig(String name) {
        this.name = name;
    }

    /**
     * Returns the coordination process name (see {@link #setName(String)}).
     *
     * @return Coordination process name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the coordination process name. Can contain only alpha-numeric characters and non-repeatable dots/hyphens
     *
     * <p>
     * This name must be the same on all of the cluster nodes that participate in this coordination process.
     * </p>
     *
     * @param name Process name.
     *
     * @see CoordinationService#process(String)
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Fluent-style version of {@link #setName(String)}.
     *
     * @param name Process name.
     *
     * @return This instance.
     */
    public CoordinationProcessConfig withName(String name) {
        setName(name);

        return this;
    }

    /**
     * Returns the flag that controls the timing of initial coordination (see {@link #setAsyncInit(boolean)}).
     *
     * @return {@code true} if {@link Hekate#join()} will be blocked until the initial coordination is complete.
     */
    public boolean isAsyncInit() {
        return asyncInit;
    }

    /**
     * Sets the flag that controls the timing of initial coordination.
     *
     * <ul>
     * <li>If set to {@code true} then initial coordination will be performed asynchronously and will not block {@link Hekate#join()}</li>
     * <li>If set to {@code false} then {@link Hekate#join()} will be blocked until the initial coordination is complete</li>
     * </ul>
     *
     * <p>
     * Default value of this parameter is {@code true} (i.e. {@link Hekate#join()} will not wait for the initial coordination).
     * </p>
     *
     * @param asyncInit {@code true} to block {@link Hekate#join()} until initial coordination is complete.
     */
    public void setAsyncInit(boolean asyncInit) {
        this.asyncInit = asyncInit;
    }

    /**
     * Fluent-style version of {@link #setAsyncInit(boolean)}.
     *
     * @param asyncInit {@code true} to block {@link Hekate#join()} until initial coordination is complete.
     *
     * @return This instance.
     */
    public CoordinationProcessConfig withAsyncInit(boolean asyncInit) {
        setAsyncInit(asyncInit);

        return this;
    }

    /**
     * Returns the coordination process handler (see {@link #setHandler(CoordinationHandler)}).
     *
     * @return Coordination handler.
     */
    public CoordinationHandler getHandler() {
        return handler;
    }

    /**
     * Sets the coordination process handler.
     *
     * @param handler Coordination process handler.
     */
    public void setHandler(CoordinationHandler handler) {
        this.handler = handler;
    }

    /**
     * Fluent-style version of {@link #setHandler(CoordinationHandler)}.
     *
     * @param handler Coordination process handler.
     *
     * @return This instance.
     */
    public CoordinationProcessConfig withHandler(CoordinationHandler handler) {
        setHandler(handler);

        return this;
    }

    /**
     * Returns the codec that should be used for messages serialization (see {@link #setMessageCodec(CodecFactory)}).
     *
     * @return Codec.
     */
    public CodecFactory<Object> getMessageCodec() {
        return messageCodec;
    }

    /**
     * Sets the codec that should be used for messages serialization.
     *
     * <p>
     * This parameter is optional and if not specified then {@link HekateBootstrap#setDefaultCodec(CodecFactory) default codec}
     * will be used.
     * </p>
     *
     * @param messageCodec Codec.
     */
    public void setMessageCodec(CodecFactory<Object> messageCodec) {
        this.messageCodec = messageCodec;
    }

    /**
     * Fluent-style version of {@link #setMessageCodec(CodecFactory)}.
     *
     * @param messageCodec Codec.
     *
     * @return This instance.
     */
    public CoordinationProcessConfig withMessageCodec(CodecFactory<Object> messageCodec) {
        setMessageCodec(messageCodec);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
