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
import io.hekate.core.HekateBootstrap;
import io.hekate.util.format.ToString;
import java.util.List;

/**
 * Configuration for {@link NetworkConnector}.
 *
 * <p>
 * Instances of this class can be registered within the {@link NetworkService} via {@link NetworkServiceFactory#setConnectors(List)}
 * method.
 * </p>
 *
 * <p>
 * For more details about connectors and networking please see the documentation of {@link NetworkService}.
 * </p>
 *
 * @param <T> Base type of messages that can be supported by this {@link NetworkConnector}.
 *
 * @see NetworkConnector
 * @see NetworkServiceFactory#setConnectors(List)
 */
public class NetworkConnectorConfig<T> {
    private String protocol;

    private long idleSocketTimeout;

    private int nioThreads;

    private CodecFactory<T> messageCodec;

    private NetworkServerHandler<T> serverHandler;

    private String logCategory;

    /**
     * Returns the connector's protocol identifier (see {@link #setProtocol(String)}).
     *
     * @return Protocol identifier.
     */
    public String getProtocol() {
        return protocol;
    }

    /**
     * Sets the connector's protocol identifier. Can contain only alpha-numeric characters and non-repeatable dots/hyphens.
     *
     * <p>
     * This protocol identifier can be used to obtain references to {@link NetworkConnector} instances via {@link
     * NetworkService#connector(String)} method.
     * </p>
     *
     * <p>
     * This parameter is mandatory and doesn't have a default value.
     * </p>
     *
     * <p>
     * <b>Note:</b> Protocol identifier must be unique across all registered {@link NetworkConnector}s within the {@link NetworkService}.
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
    public NetworkConnectorConfig<T> withProtocol(String protocol) {
        setProtocol(protocol);

        return this;
    }

    /**
     * Returns the timeout in milliseconds for {@link NetworkClient}s to stay idle before being automatically disconnected (see {@link
     * #setIdleSocketTimeout(long)}).
     *
     * @return Timeout in milliseconds.
     */
    public long getIdleSocketTimeout() {
        return idleSocketTimeout;
    }

    /**
     * Sets the timeout in milliseconds for {@link NetworkClient}s to stay idle before being automatically disconnected.
     *
     * <p>
     * {@link NetworkClient}s are considered to be idle if no messages are send/received for the specified time interval. When idle client
     * is detected it will be automatically {@link NetworkClient#disconnect() disconnected}.
     * </p>
     *
     * <p>
     * If value of this parameter is less than or equals to zero then idle clients tracking is disabled.
     * </p>
     *
     * <p>
     * Default value of this parameter is 0.
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
    public NetworkConnectorConfig<T> withIdleSocketTimeout(long idleTimeout) {
        setIdleSocketTimeout(idleTimeout);

        return this;
    }

    /**
     * Returns the {@link CodecFactory} that should be used for messages serialization (see {@link
     * #setMessageCodec(CodecFactory)}).
     *
     * @return Codec factory.
     */
    public CodecFactory<T> getMessageCodec() {
        return messageCodec;
    }

    /**
     * Sets codec factory that should be used for messages serialization.
     *
     * <p>
     * Only messages that are supported by the specified {@link CodecFactory} can be sent/received by this {@link NetworkConnector}.
     * </p>
     *
     * <p>
     * If not specified then the default general-purpose codec will be used (see
     * {@link HekateBootstrap#setDefaultCodec(CodecFactory)}).
     * </p>
     *
     * @param messageCodec Codec factory.
     */
    public void setMessageCodec(CodecFactory<T> messageCodec) {
        this.messageCodec = messageCodec;
    }

    /**
     * Fluent-style version of {@link #setMessageCodec(CodecFactory)}.
     *
     * @param codecFactory Codec factory.
     *
     * @return This instance.
     */
    public NetworkConnectorConfig<T> withMessageCodec(CodecFactory<T> codecFactory) {
        setMessageCodec(codecFactory);

        return this;
    }

    /**
     * Returns the thread pool size that should be used for NIO events processing (see {@link #setNioThreads(int)}).
     *
     * @return Thread pool size.
     */
    public int getNioThreads() {
        return nioThreads;
    }

    /**
     * Sets the thread pool size that should be used for NIO events processing.
     *
     * <p>
     * If value of this parameter is above zero then a new thread pool of that size will be initialized and used by the {@link
     * NetworkConnector} to handle all NIO events of its connections. If value of this parameter is less than or equals to zero then {@link
     * NetworkService}'s core thread pool will be used (see {@link NetworkServiceFactory#setNioThreads(int)}).
     * </p>
     *
     * <p>
     * Default value of this parameter is 0.
     * </p>
     *
     * @param nioThreads Thread pool size.
     *
     * @see NetworkServiceFactory#setNioThreads(int)
     */
    public void setNioThreads(int nioThreads) {
        this.nioThreads = nioThreads;
    }

    /**
     * Fluent-style version of {@link #setNioThreads(int)}.
     *
     * @param nioThreads Thread pool size.
     *
     * @return This instance.
     */
    public NetworkConnectorConfig<T> withNioThreads(int nioThreads) {
        setNioThreads(nioThreads);

        return this;
    }

    /**
     * Returns the log category that should be used by the connector (see {@link #setLogCategory(String)}).
     *
     * @return Log category.
     */
    public String getLogCategory() {
        return logCategory;
    }

    /**
     * Sets the log category that should be used by the connector.
     *
     * @param logCategory Log category.
     */
    public void setLogCategory(String logCategory) {
        this.logCategory = logCategory;
    }

    /**
     * Fluent-style version of {@link #setLogCategory(String)}.
     *
     * @param logCategory Log category.
     *
     * @return This instance.
     */
    public NetworkConnectorConfig<T> withLogCategory(String logCategory) {
        setLogCategory(logCategory);

        return this;
    }

    /**
     * Returns the handler that should be used to accept connections and process messages from remote connectors (see {@link
     * #setServerHandler(NetworkServerHandler)}).
     *
     * @return Handler that should be used to accept connections and process messages from remote connectors.
     */
    public NetworkServerHandler<T> getServerHandler() {
        return serverHandler;
    }

    /**
     * Sets the server handler that should be used to accept connections and process messages from remote connectors.
     *
     * <p>
     * This parameter is optional and if not specified then connector will work in client-only mode. In this mode it will be able to create
     * new {@link NetworkClient} connections but will not be able to accept such connections from remote connectors.
     * </p>
     *
     * @param serverHandler Handler that should be used to accept connections and process messages from remote connectors.
     */
    public void setServerHandler(NetworkServerHandler<T> serverHandler) {
        this.serverHandler = serverHandler;
    }

    /**
     * Fluent-style version of {@link #setServerHandler(NetworkServerHandler)}.
     *
     * @param serverHandler Handler that should be used to accept connections and process messages from remote connectors.
     *
     * @return This instance.
     */
    public NetworkConnectorConfig<T> withServerHandler(NetworkServerHandler<T> serverHandler) {
        setServerHandler(serverHandler);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
