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

import io.hekate.util.format.ToString;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;

/**
 * Abstract base class for configurable {@link NetworkServer} factories.
 *
 * @see NetworkServer
 */
public abstract class NetworkServerFactoryBase {
    /** Default value (={@value}) for {@link #setHeartbeatInterval(int)}. */
    public static final int DEFAULT_HB_INTERVAL = NetworkServiceFactory.DEFAULT_HB_INTERVAL;

    /** Default value (={@value}) for {@link #setHeartbeatLossThreshold(int)}. */
    public static final int DEFAULT_HB_LOSS_THRESHOLD = NetworkServiceFactory.DEFAULT_HB_LOSS_THRESHOLD;

    /** Default value (={@value}) for {@link #setTcpNoDelay(boolean)}. */
    public static final boolean DEFAULT_TCP_NO_DELAY = NetworkServiceFactory.DEFAULT_TCP_NO_DELAY;

    /** Default value (={@value}) for {@link #setAutoAccept(boolean)}. */
    public static final boolean DEFAULT_AUTO_ACCEPT = true;

    private boolean autoAccept = DEFAULT_AUTO_ACCEPT;

    private int heartbeatInterval = DEFAULT_HB_INTERVAL;

    private int heartbeatLossThreshold = DEFAULT_HB_LOSS_THRESHOLD;

    private boolean tcpNoDelay = DEFAULT_TCP_NO_DELAY;

    private Integer soReceiveBufferSize;

    private Integer soSendBufferSize;

    private Boolean soReuseAddress;

    private Integer soBacklog;

    /**
     * Creates a new network server based on the configuration options of this factory.
     *
     * @return New network server.
     */
    public abstract NetworkServer createServer();

    /**
     * Returns {@code true} if server should automatically start accepting client connections (see {@link #setAutoAccept(boolean)}).
     *
     * @return {@code true} if server should automatically start accepting client connections.
     */
    public boolean isAutoAccept() {
        return autoAccept;
    }

    /**
     * Sets the flag indicating whether server should automatically start accepting client connections or if it should be done {@link
     * NetworkServer#startAccepting() manually}.
     *
     * <p>
     * if this flag is set to {@code false} then server will bind to its port but will block all new client connections unless {@link
     * NetworkServer#startAccepting()} method is called.
     * </p>
     *
     * <p>
     * Default value of this parameter is {@value #DEFAULT_AUTO_ACCEPT}.
     * </p>
     *
     * @param autoAccept {@code true} if server should automatically start accepting new client connections.
     */
    public void setAutoAccept(boolean autoAccept) {
        this.autoAccept = autoAccept;
    }

    /**
     * Fluent-style version of {@link #setAutoAccept(boolean)}.
     *
     * @param autoAccept {@code true} if server should automatically start accepting new client connections.
     *
     * @return This instance.
     */
    public NetworkServerFactoryBase withAutoAccept(boolean autoAccept) {
        setAutoAccept(autoAccept);

        return this;
    }

    /**
     * Returns the heartbeat sending interval in milliseconds (see {@link #setHeartbeatInterval(int)}).
     *
     * @return Heartbeat interval in milliseconds.
     */
    public int getHeartbeatInterval() {
        return heartbeatInterval;
    }

    /**
     * Sets the heartbeat sending interval in milliseconds for keeping a TCP socket connection alive.
     *
     * <p>
     * This setting is used by a TCP connection peer to notify other peer that it is alive. This is archived by sending a small
     * dummy message if there was no other activity (i.e. no other messages sent) for the specified time interval. Also please see {@link
     * #setHeartbeatLossThreshold(int)} for details of how this parameter is used for connection failure detection.
     * </p>
     *
     * <p>
     * Value of this parameter must be above zero.
     * </p>
     *
     * <p>
     * Default value of this parameter is {@value #DEFAULT_HB_INTERVAL}.
     * </p>
     *
     * @param heartbeatInterval Heartbeat interval in milliseconds.
     *
     * @see #setHeartbeatLossThreshold(int)
     */
    public void setHeartbeatInterval(int heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    /**
     * Fluent-style version of {@link #setHeartbeatInterval(int)}.
     *
     * @param heartbeatInterval Heartbeat interval in milliseconds.
     *
     * @return This instance.
     */
    public NetworkServerFactoryBase withHeartbeatInterval(int heartbeatInterval) {
        setHeartbeatInterval(heartbeatInterval);

        return this;
    }

    /**
     * Returns the maximum number of lost heartbeats (see {@link #setHeartbeatLossThreshold(int)}).
     *
     * @return Maximum number of lost heartbeats.
     */
    public int getHeartbeatLossThreshold() {
        return heartbeatLossThreshold;
    }

    /**
     * Sets the maximum number of lost heartbeats before considering a TCP connection to be failed.
     *
     * <p>
     * This setting works in conjunction with {@link #setHeartbeatInterval(int)} to calculate the total amount of time for a TCP connection
     * to stay inactive (i.e. do not send any heartbeats or application data). When this timeout elapses then connection is considered to
     * be failed and will be closed with a {@link SocketTimeoutException}.
     * </p>
     *
     * <p>
     * For example: if heartbeat interval is 500 and loss threshold is 3 then a TCP connection will be closed with and error after 1500
     * milliseconds of inactivity.
     * </p>
     *
     * <p>
     * Value of this parameter must be above zero.
     * </p>
     *
     * <p>
     * Default value of this parameter is {@value #DEFAULT_HB_LOSS_THRESHOLD}.
     * </p>
     *
     * @param heartbeatLossThreshold Maximum number of lost heartbeats.
     */
    public void setHeartbeatLossThreshold(int heartbeatLossThreshold) {
        this.heartbeatLossThreshold = heartbeatLossThreshold;
    }

    /**
     * Fluent-style version of {@link #setHeartbeatLossThreshold(int)}.
     *
     * @param heartbeatLossThreshold Maximum number of lost heartbeats.
     *
     * @return This instance.
     */
    public NetworkServerFactoryBase withHeartbeatLossThreshold(int heartbeatLossThreshold) {
        setHeartbeatLossThreshold(heartbeatLossThreshold);

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
     * Default value of this parameter is {@value #DEFAULT_TCP_NO_DELAY}.
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
    public NetworkServerFactoryBase withTcpNoDelay(boolean tcpNoDelay) {
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
    public NetworkServerFactoryBase withSoReceiveBufferSize(Integer soReceiveBufferSize) {
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
    public NetworkServerFactoryBase withSoSendBufferSize(Integer soSendBufferSize) {
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
    public NetworkServerFactoryBase withSoReuseAddress(Boolean soReuseAddress) {
        setSoReuseAddress(soReuseAddress);

        return this;
    }

    /**
     * Returns the maximum number of pending connections that can be queued on the server socket channel (see {@link
     * #setSoBacklog(Integer)}).
     *
     * @return The maximum number of pending connections or {@code null} if it wasn't set.
     */
    public Integer getSoBacklog() {
        return soBacklog;
    }

    /**
     * Sets the maximum number of pending connections that can be queued on the server socket channel
     * (see {@link ServerSocketChannel#bind(SocketAddress, int)} for more details).
     *
     * <p>
     * Default value of this parameter is {@code null}.
     * </p>
     *
     * @param soBacklog The maximum number of pending connections.
     */
    public void setSoBacklog(Integer soBacklog) {
        this.soBacklog = soBacklog;
    }

    /**
     * Fluent-style version of {@link #setSoBacklog(Integer)}.
     *
     * @param soBacklog The maximum number of pending connections.
     *
     * @return This instance.
     */
    public NetworkServerFactoryBase withSoBacklog(Integer soBacklog) {
        setSoBacklog(soBacklog);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
