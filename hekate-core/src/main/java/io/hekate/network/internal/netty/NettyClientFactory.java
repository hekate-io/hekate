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

package io.hekate.network.internal.netty;

import io.hekate.codec.CodecFactory;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.network.NetworkClient;
import io.hekate.util.format.ToString;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;

public class NettyClientFactory<T> {
    private String protocol;

    private Integer connectTimeout;

    private long idleTimeout;

    private boolean tcpNoDelay;

    private Integer soReceiveBufferSize;

    private Integer soSendBufferSize;

    private Boolean soReuseAddress;

    private CodecFactory<T> codecFactory;

    private EventLoopGroup eventLoopGroup;

    private NettyMetricsCallback metrics;

    private String loggerCategory;

    private SslContext ssl;

    public EventLoopGroup getEventLoopGroup() {
        return eventLoopGroup;
    }

    public void setEventLoopGroup(EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public Integer getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(Integer connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public boolean getTcpNoDelay() {
        return tcpNoDelay;
    }

    public void setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    public Integer getSoReceiveBufferSize() {
        return soReceiveBufferSize;
    }

    public void setSoReceiveBufferSize(Integer soReceiveBufferSize) {
        this.soReceiveBufferSize = soReceiveBufferSize;
    }

    public Integer getSoSendBufferSize() {
        return soSendBufferSize;
    }

    public void setSoSendBufferSize(Integer soSendBufferSize) {
        this.soSendBufferSize = soSendBufferSize;
    }

    public Boolean getSoReuseAddress() {
        return soReuseAddress;
    }

    public void setSoReuseAddress(Boolean soReuseAddress) {
        this.soReuseAddress = soReuseAddress;
    }

    public CodecFactory<T> getCodecFactory() {
        return codecFactory;
    }

    public void setCodecFactory(CodecFactory<T> codecFactory) {
        this.codecFactory = codecFactory;
    }

    public NettyMetricsCallback getMetrics() {
        return metrics;
    }

    public void setMetrics(NettyMetricsCallback metrics) {
        this.metrics = metrics;
    }

    public String getLoggerCategory() {
        return loggerCategory;
    }

    public void setLoggerCategory(String loggerCategory) {
        this.loggerCategory = loggerCategory;
    }

    public SslContext getSsl() {
        return ssl;
    }

    public void setSsl(SslContext ssl) {
        ArgAssert.check(ssl == null || ssl.isClient(), "SSL context must be configured in client mode.");

        this.ssl = ssl;
    }

    public NetworkClient<T> createClient() {
        return new NettyClient<>(this);
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
