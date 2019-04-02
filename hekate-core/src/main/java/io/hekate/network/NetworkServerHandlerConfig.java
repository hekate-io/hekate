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

package io.hekate.network;

import io.hekate.codec.CodecFactory;
import io.hekate.util.format.ToString;

/**
 * Configuration for {@link NetworkServerHandler}.
 *
 * @param <T> Base type of messages that can be processed by {@link NetworkServerHandler}.
 *
 * @see NetworkServerHandler
 */
public class NetworkServerHandlerConfig<T> {
    private String protocol;

    private CodecFactory<T> codecFactory;

    private NetworkServerHandler<T> handler;

    private String loggerCategory;

    /**
     * Returns the protocol identifier (see {@link #setProtocol(String)}).
     *
     * @return Protocol identifier.
     */
    public String getProtocol() {
        return protocol;
    }

    /**
     * Sets the connector protocol identifier. Can contain only alpha-numeric characters and non-repeatable dots/hyphens.
     *
     * <p>
     * Only clients that have the same {@link NetworkConnectorConfig#setProtocol(String) protocol identifier} will be processed by the
     * {@link #setHandler(NetworkServerHandler) configured} {@link NetworkServerHandler}.
     * </p>
     *
     * <p>
     * This parameter is mandatory and doesn't have a default value.
     * </p>
     *
     * <p>
     * <b>Note:</b> Protocol identifier must be unique across all registered {@link NetworkServerHandlerConfig}s within the {@link
     * NetworkServer}.
     * </p>
     *
     * @param protocol Protocol identifier (can contain only alpha-numeric characters and non-repeatable dots/hyphens).
     */
    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    /**
     * Fluent-style version of {@link #setProtocol(String)}.
     *
     * @param protocol Protocol identifier.
     *
     * @return This instance.
     */
    public NetworkServerHandlerConfig<T> withProtocol(String protocol) {
        setProtocol(protocol);

        return this;
    }

    /**
     * Returns the codec factory that should be used for messages serialization (see {@link #setCodecFactory(CodecFactory)}).
     *
     * @return Codec factory.
     */
    public CodecFactory<T> getCodecFactory() {
        return codecFactory;
    }

    /**
     * Sets the codec factory that should be used for messages serialization.
     *
     * <p>
     * Only messages that are supported by the specified {@link CodecFactory} can be sent/received by the configured {@link
     * NetworkServerHandler}. Note that the same factory must be configured on the
     * {@link NetworkConnectorConfig#setMessageCodec(CodecFactory) client side}.
     * </p>
     *
     * <p>
     * This parameter is mandatory and doesn't have a default value.
     * </p>
     *
     * @param codecFactory Codec factory.
     */
    public void setCodecFactory(CodecFactory<T> codecFactory) {
        this.codecFactory = codecFactory;
    }

    /**
     * Fluent-style version of {@link #setCodecFactory(CodecFactory)}.
     *
     * @param codecFactory Codec factory.
     *
     * @return This instance.
     */
    public NetworkServerHandlerConfig<T> withCodecFactory(CodecFactory<T> codecFactory) {
        setCodecFactory(codecFactory);

        return this;
    }

    /**
     * Returns the handler (see {@link #setHandler(NetworkServerHandler)}.
     *
     * @return Handler.
     */
    public NetworkServerHandler<T> getHandler() {
        return handler;
    }

    /**
     * Sets the handler.
     *
     * @param handler Handler.
     */
    public void setHandler(NetworkServerHandler<T> handler) {
        this.handler = handler;
    }

    /**
     * Fluent-style version of {@link #setHandler(NetworkServerHandler)}.
     *
     * @param handler Handler.
     *
     * @return This instance.
     */
    public NetworkServerHandlerConfig<T> withHandler(NetworkServerHandler<T> handler) {
        setHandler(handler);

        return this;
    }

    /**
     * Returns the logger category that should be used by {@link NetworkServerHandler} (see {@link #setLoggerCategory(String)}).
     *
     * @return Logger category.
     */
    public String getLoggerCategory() {
        return loggerCategory;
    }

    /**
     * Sets the logger category that should be used by {@link NetworkServerHandler}.
     *
     * @param loggerCategory Logger category.
     */
    public void setLoggerCategory(String loggerCategory) {
        this.loggerCategory = loggerCategory;
    }

    /**
     * Fluent-style version of {@link #setLoggerCategory(String)}.
     *
     * @param loggerCategory Logger category.
     *
     * @return This instance.
     */
    public NetworkServerHandlerConfig<T> withLoggerCategory(String loggerCategory) {
        setLoggerCategory(loggerCategory);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
