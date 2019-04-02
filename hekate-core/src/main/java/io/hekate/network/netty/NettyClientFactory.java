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

package io.hekate.network.netty;

import io.hekate.codec.CodecFactory;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkServerHandlerConfig;
import io.hekate.util.format.ToString;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import java.net.StandardSocketOptions;

/**
 * Configurable factory for Netty-based {@link NetworkClient}s.
 *
 * @param <T> Base type of messages that can be sent/received by {@link NetworkClient}s.
 */
public class NettyClientFactory<T> implements NetworkConnector<T> {
    private String protocol;

    private Integer connectTimeout;

    private long idleTimeout;

    private boolean tcpNoDelay;

    private Integer soReceiveBufferSize;

    private Integer soSendBufferSize;

    private Boolean soReuseAddress;

    private CodecFactory<T> codecFactory;

    private EventLoopGroup eventLoop;

    private NettyMetricsSink metrics;

    private String loggerCategory;

    private SslContext ssl;

    private NettySpy spy;

    /**
     * Returns the event loop group (see {@link #setEventLoop(EventLoopGroup)}).
     *
     * @return Event loop group.
     */
    public EventLoopGroup getEventLoop() {
        return eventLoop;
    }

    /**
     * Sets the event loop group.
     *
     * <p>
     * This parameter is mandatory and doesn't have a default value.
     * </p>
     *
     * @param eventLoop Event loop group.
     */
    public void setEventLoop(EventLoopGroup eventLoop) {
        this.eventLoop = eventLoop;
    }

    /**
     * Fluent-style version of {@link #setEventLoop(EventLoopGroup)}.
     *
     * @param eventLoop Event loop group.
     *
     * @return This instance.
     */
    public NettyClientFactory<T> withEventLoop(EventLoopGroup eventLoop) {
        setEventLoop(eventLoop);

        return this;
    }

    /**
     * Returns the protocol identifier (see {@link #setProtocol(String)}).
     *
     * @return Protocol identifier.
     */
    public String getProtocol() {
        return protocol;
    }

    /**
     * Sets the protocol identifier. Can contain only alpha-numeric characters and non-repeatable dots/hyphens.
     *
     * <p>
     * Must be the same with the server-side protocol identifier (see {@link NetworkServerHandlerConfig#setProtocol(String)}).
     * </p>
     *
     * <p>
     * This parameter is mandatory and doesn't have a default value.
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
    public NettyClientFactory<T> withProtocol(String protocol) {
        setProtocol(protocol);

        return this;
    }

    /**
     * Returns the TCP connect timeout in milliseconds (see {@link #setConnectTimeout(Integer)}).
     *
     * @return Timeout in milliseconds.
     */
    public Integer getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Sets the timeout in milliseconds for establishing a TCP socket connection to a remote node.
     *
     * <p>
     * This parameter is optional and if not specified then no timeout will happen.
     * </p>
     *
     * @param connectTimeout Timeout in milliseconds.
     */
    public void setConnectTimeout(Integer connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /**
     * Fluent-style version of {@link #setConnectTimeout(Integer)}.
     *
     * @param connectTimeout Timeout in milliseconds.
     *
     * @return This instance.
     */
    public NettyClientFactory<T> withConnectTimeout(Integer connectTimeout) {
        setConnectTimeout(connectTimeout);

        return this;
    }

    /**
     * Returns the timeout in milliseconds for {@link NetworkClient}s to stay idle before being automatically disconnected (see {@link
     * #setIdleTimeout(long)}).
     *
     * @return Timeout in milliseconds.
     */
    public long getIdleTimeout() {
        return idleTimeout;
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
     * @param idleTimeout Timeout in milliseconds.
     */
    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    /**
     * Fluent-style version of {@link #setIdleTimeout(long)}.
     *
     * @param idleTimeout Timeout in milliseconds.
     *
     * @return This instance.
     */
    public NettyClientFactory<T> withIdleTimeout(long idleTimeout) {
        setIdleTimeout(idleTimeout);

        return this;
    }

    /**
     * Returns {@code true} if {@link StandardSocketOptions#TCP_NODELAY TCP_NODELAY} option must be set on TCP socket connections
     * (see {@link #setTcpNoDelay(boolean)}).
     *
     * @return {@code true} if {@link StandardSocketOptions#TCP_NODELAY TCP_NODELAY} option must be set.
     */
    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * Sets the flag indicating that {@link StandardSocketOptions#TCP_NODELAY TCP_NODELAY} option  must be set on TCP socket connections.
     *
     * <p>
     * Please see <a href="https://en.wikipedia.org/wiki/Nagle%27s_algorithm" target="_blank">Nagle's algorithm</a> for more info.
     * </p>
     *
     * <p>
     * Default value of this parameter is {@code false}.
     * </p>
     *
     * @param tcpNoDelay {@code true} if {@link StandardSocketOptions#TCP_NODELAY TCP_NODELAY} option  must be set.
     */
    public void setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    /**
     * Fluent-style version of {@link #setTcpNoDelay(boolean)}.
     *
     * @param tcpNoDelay {@code true} if {@link StandardSocketOptions#TCP_NODELAY TCP_NODELAY} option  must be set.
     *
     * @return This instance.
     */
    public NettyClientFactory<T> withTcpNoDelay(boolean tcpNoDelay) {
        setTcpNoDelay(tcpNoDelay);

        return this;
    }

    /**
     * Returns the size of the socket receive buffer in bytes (see {@link #setSoReceiveBufferSize(Integer)}).
     *
     * @return Buffer size in bytes or {@code null} if it wasn't set.
     */
    public Integer getSoReceiveBufferSize() {
        return soReceiveBufferSize;
    }

    /**
     * Sets the size of the socket receive buffer in bytes (see {@link StandardSocketOptions#SO_RCVBUF} for more details).
     *
     * <p>
     * Default value of this parameter is {@code null}.
     * </p>
     *
     * @param soReceiveBufferSize Buffer size in bytes.
     */
    public void setSoReceiveBufferSize(Integer soReceiveBufferSize) {
        this.soReceiveBufferSize = soReceiveBufferSize;
    }

    /**
     * Fluent-style version of {@link #setSoReceiveBufferSize(Integer)}.
     *
     * @param soReceiveBufferSize Buffer size in bytes.
     *
     * @return This instance.
     */
    public NettyClientFactory<T> withSoReceiveBufferSize(Integer soReceiveBufferSize) {
        setSoReceiveBufferSize(soReceiveBufferSize);

        return this;
    }

    /**
     * Returns the size of the socket send buffer in bytes (see {@link #setSoSendBufferSize(Integer)}).
     *
     * @return Buffer size in bytes or {@code null} if it wasn't set.
     */
    public Integer getSoSendBufferSize() {
        return soSendBufferSize;
    }

    /**
     * Sets the size of the socket send buffer in bytes (see {@link StandardSocketOptions#SO_SNDBUF} for more details).
     *
     * <p>
     * Default value of this parameter is {@code null}.
     * </p>
     *
     * @param soSendBufferSize Buffer size in bytes.
     */
    public void setSoSendBufferSize(Integer soSendBufferSize) {
        this.soSendBufferSize = soSendBufferSize;
    }

    /**
     * Fluent-style version of {@link #setSoSendBufferSize(Integer)}.
     *
     * @param soSendBufferSize Buffer size in bytes.
     *
     * @return This instance.
     */
    public NettyClientFactory<T> withSoSendBufferSize(Integer soSendBufferSize) {
        setSoSendBufferSize(soSendBufferSize);

        return this;
    }

    /**
     * Sets flag indicating that socket addresses should be re-used (see {@link #setSoReuseAddress(Boolean)}).
     *
     * @return Flag value or {@code null} if it wasn't set.
     */
    public Boolean getSoReuseAddress() {
        return soReuseAddress;
    }

    /**
     * Sets flag indicating that socket addresses should be re-used (see {@link StandardSocketOptions#SO_REUSEADDR} for more details).
     *
     * <p>
     * Default value of this parameter is {@code null}.
     * </p>
     *
     * @param soReuseAddress Flag indicating that socket addresses should be re-used.
     */
    public void setSoReuseAddress(Boolean soReuseAddress) {
        this.soReuseAddress = soReuseAddress;
    }

    /**
     * Fluent-style version of {@link #setSoReuseAddress(Boolean)}.
     *
     * @param soReuseAddress Flag indicating that socket addresses should be re-used.
     *
     * @return This instance.
     */
    public NettyClientFactory<T> withSoReuseAddress(Boolean soReuseAddress) {
        setSoReuseAddress(soReuseAddress);

        return this;
    }

    /**
     * Returns the {@link CodecFactory} that should be used for messages serialization (see {@link #setCodecFactory(CodecFactory)}).
     *
     * @return Codec factory.
     */
    public CodecFactory<T> getCodecFactory() {
        return codecFactory;
    }

    /**
     * Sets codec factory that should be used for messages serialization.
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
    public NettyClientFactory<T> withCodecFactory(CodecFactory<T> codecFactory) {
        setCodecFactory(codecFactory);

        return this;
    }

    /**
     * Returns the metrics sink (see {@link #setMetrics(NettyMetricsSink)}).
     *
     * @return Metrics sink.
     */
    public NettyMetricsSink getMetrics() {
        return metrics;
    }

    /**
     * Sets the metrics sink.
     *
     * <p>
     * This parameter is optional and if not specified then no metrics will be collected by {@link NetworkClient}s.
     * </p>
     *
     * @param metrics Metrics sink.
     */
    public void setMetrics(NettyMetricsSink metrics) {
        this.metrics = metrics;
    }

    /**
     * Fluent-style version of {@link #setMetrics(NettyMetricsSink)}.
     *
     * @param metrics Metrics sink.
     *
     * @return This instance.
     */
    public NettyClientFactory<T> withMetrics(NettyMetricsSink metrics) {
        setMetrics(metrics);

        return this;
    }

    /**
     * Returns the logger category that should be used by {@link NetworkClient}s (see {@link #setLoggerCategory(String)}).
     *
     * @return Logger category.
     */
    public String getLoggerCategory() {
        return loggerCategory;
    }

    /**
     * Sets the log category that should be used by {@link NetworkClient}s.
     *
     * <p>
     * This parameter is optional and if not specified then internal logger category will be used.
     * </p>
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
    public NettyClientFactory<T> withLoggerCategory(String loggerCategory) {
        setLoggerCategory(loggerCategory);

        return this;
    }

    /**
     * Returns the SSL context (see {@link #setSsl(SslContext)}).
     *
     * @return SSL context.
     */
    public SslContext getSsl() {
        return ssl;
    }

    /**
     * Sets the SSL context that should be used to secure all network communications.
     *
     * <p>
     * This parameter is optional and if not specified then no encryption will be applied.
     * </p>
     *
     * <p>
     * <b>Note:</b> If SSL is configured for the client then it should be configured on the server side too (see {@link
     * NettyServerFactory#setSsl(SslContext)}).
     * </p>
     *
     * @param ssl SSL context.
     */
    public void setSsl(SslContext ssl) {
        ArgAssert.check(ssl == null || ssl.isClient(), "SSL context must be configured in client mode.");

        this.ssl = ssl;
    }

    /**
     * Fluent-style version of {@link #setSsl(SslContext)}.
     *
     * @param ssl SSL context.
     *
     * @return SSL context.
     */
    public NettyClientFactory<T> withSsl(SslContext ssl) {
        setSsl(ssl);

        return this;
    }

    @Override
    public NetworkClient<T> newClient() {
        return new NettyClient<>(this);
    }

    @Override
    public String protocol() {
        return getProtocol();
    }

    // Package level for testing purposes.
    NettySpy getSpy() {
        return spy;
    }

    // Package level for testing purposes.
    void setSpy(NettySpy spy) {
        this.spy = spy;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
