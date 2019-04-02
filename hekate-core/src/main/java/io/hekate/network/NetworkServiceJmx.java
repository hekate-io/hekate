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

import io.hekate.core.jmx.JmxTypeName;
import javax.management.MXBean;

/**
 * JMX interface for {@link NetworkService}.
 */
@MXBean
@JmxTypeName("NetworkService")
public interface NetworkServiceJmx {
    /**
     * Returns the value of {@link NetworkServiceFactory#setConnectTimeout(int)}.
     *
     * @return Value of {@link NetworkServiceFactory#setConnectTimeout(int)}.
     */
    int getConnectTimeout();

    /**
     * Returns the value of {@link NetworkServiceFactory#setHeartbeatInterval(int)}.
     *
     * @return Value of {@link NetworkServiceFactory#setHeartbeatInterval(int)}.
     */
    int getHeartbeatInterval();

    /**
     * Returns the value of {@link NetworkServiceFactory#setHeartbeatLossThreshold(int)}.
     *
     * @return Value of {@link NetworkServiceFactory#setHeartbeatLossThreshold(int)}.
     */
    int getHeartbeatLossThreshold();

    /**
     * Returns the value of {@link NetworkServiceFactory#setNioThreads(int)}.
     *
     * @return Value of {@link NetworkServiceFactory#setNioThreads(int)}.
     */
    int getNioThreads();

    /**
     * Returns the value of {@link NetworkServiceFactory#setTransport(NetworkTransportType)}.
     *
     * @return Value of {@link NetworkServiceFactory#setTransport(NetworkTransportType)}.
     */
    NetworkTransportType getTransport();

    /**
     * Returns {@code true} if SSL is enabled.
     *
     * @return {@code true} if SSL is enabled.
     *
     * @see NetworkServiceFactory#setSsl(NetworkSslConfig)
     */
    boolean isSsl();

    /**
     * Returns the value of {@link NetworkServiceFactory#setTcpNoDelay(boolean)}.
     *
     * @return Value of {@link NetworkServiceFactory#setTcpNoDelay(boolean)}.
     */
    boolean isTcpNoDelay();

    /**
     * Returns the value of {@link NetworkServiceFactory#setTcpReceiveBufferSize(Integer)}.
     *
     * @return Value of {@link NetworkServiceFactory#setTcpReceiveBufferSize(Integer)}.
     */
    Integer getTcpReceiveBufferSize();

    /**
     * Returns the value of {@link NetworkServiceFactory#setTcpSendBufferSize(Integer)}.
     *
     * @return Value of {@link NetworkServiceFactory#setTcpSendBufferSize(Integer)}.
     */
    Integer getTcpSendBufferSize();

    /**
     * Returns the value of {@link NetworkServiceFactory#setTcpReuseAddress(Boolean)}.
     *
     * @return Value of {@link NetworkServiceFactory#setTcpReuseAddress(Boolean)}.
     */
    Boolean getTcpReuseAddress();
}
