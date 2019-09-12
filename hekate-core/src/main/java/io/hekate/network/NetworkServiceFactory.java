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

import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.service.ServiceFactory;
import io.hekate.network.address.AddressPattern;
import io.hekate.network.address.AddressSelector;
import io.hekate.network.internal.NettyNetworkService;
import io.hekate.util.format.ToString;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Factory for {@link NetworkService}.
 *
 * <p>
 * This class represents a configurable factory for {@link NetworkService}. Instances of this class can be
 * {@link HekateBootstrap#withService(ServiceFactory) registered} within the {@link HekateBootstrap} in order to customize options of the
 * {@link NetworkService}.
 * </p>
 *
 * <p>
 * For more details about the {@link NetworkService} and its capabilities please see the documentation of {@link NetworkService} interface.
 * </p>
 */
public class NetworkServiceFactory implements ServiceFactory<NetworkService> {
    /** Default value (={@value}) for {@link #setPort(int)}. */
    public static final int DEFAULT_PORT = 10012;

    /** Default value (={@value}) for {@link #setPortRange(int)}. */
    public static final int DEFAULT_PORT_RANGE = 100;

    /** Default value (={@value}) for {@link #setHeartbeatInterval(int)}. */
    public static final int DEFAULT_HB_INTERVAL = 1000;

    /** Default value (={@value}) for {@link #setHeartbeatLossThreshold(int)}. */
    public static final int DEFAULT_HB_LOSS_THRESHOLD = 3;

    /** Default value (={@value}) for {@link #setAcceptRetryInterval(long)}. */
    public static final int DEFAULT_ACCEPT_RETRY_INTERVAL = 1000;

    /** Default value (={@value}) for {@link #setConnectTimeout(int)}. */
    public static final int DEFAULT_CONNECT_TIMEOUT = 3000;

    /** Default value (={@value}) for {@link #setTcpNoDelay(boolean)}. */
    public static final boolean DEFAULT_TCP_NO_DELAY = true;

    private AddressSelector hostSelector = new AddressPattern();

    private int port = DEFAULT_PORT;

    private int portRange = DEFAULT_PORT_RANGE;

    private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;

    private long acceptRetryInterval = DEFAULT_ACCEPT_RETRY_INTERVAL;

    private int heartbeatInterval = DEFAULT_HB_INTERVAL;

    private int heartbeatLossThreshold = DEFAULT_HB_LOSS_THRESHOLD;

    private int nioThreads = Runtime.getRuntime().availableProcessors();

    private NetworkTransportType transport = NetworkTransportType.AUTO;

    private boolean tcpNoDelay = DEFAULT_TCP_NO_DELAY;

    private Integer tcpReceiveBufferSize;

    private Integer tcpSendBufferSize;

    private Boolean tcpReuseAddress;

    private Integer tcpBacklog;

    private NetworkSslConfig ssl;

    private List<NetworkConnectorConfig<?>> connectors;

    private List<NetworkConfigProvider> configProviders;

    /**
     * Returns host address selector of the local node (see {@link #setHostSelector(AddressSelector)}).
     *
     * @return Host address.
     */
    public AddressSelector getHostSelector() {
        return hostSelector;
    }

    /**
     * Sets the host address selector that will be used to select an address of the local cluster node.
     *
     * @param hostSelector Address selector.
     *
     * @see #setHost(String)
     */
    public void setHostSelector(AddressSelector hostSelector) {
        ArgAssert.notNull(hostSelector, "host");

        this.hostSelector = hostSelector;
    }

    /**
     * Fluent-style version of {@link #setHostSelector(AddressSelector)}.
     *
     * @param host IP address or host name.
     *
     * @return This instance.
     */
    public NetworkServiceFactory withHostSelector(AddressSelector host) {
        setHostSelector(host);

        return this;
    }

    /**
     * Supplementary getter for {@link #setHost(String)}. Returns the host selection pattern if this factory is configured with {@link
     * AddressPattern}; returns {@code null} otherwise.
     *
     * @return host selection pattern if this factory is configured with {@link
     * AddressPattern}; returns {@code null} otherwise.
     */
    public String getHost() {
        if (getHostSelector() instanceof AddressPattern) {
            AddressPattern pattern = (AddressPattern)getHostSelector();

            return pattern.pattern();
        } else {
            return null;
        }
    }

    /**
     * Sets the host address selection pattern (see {@link AddressPattern}).
     *
     * <p>
     * The following patterns can be specified:
     * </p>
     *
     * <ul>
     * <li><b>any</b> - any non-loopback address</li>
     * <li><b>any-ip4</b> - any IPv6 non-loopback address</li>
     *
     * <li><b>ip~<i>regex</i></b> - any IP address that matches the specified regular expression</li>
     * <li><b>ip4~<i>regex</i></b> - any IPv4 address that matches the specified regular expression</li>
     * <li><b>ip6~<i>regex</i></b> - any IPv6 address that matches the specified regular expression</li>
     *
     * <li><b>!ip~<i>regex</i></b> - any IP address that does NOT match the specified regular expression</li>
     * <li><b>!ip4~<i>regex</i></b> - any IPv4 address that does NOT match the specified regular expression</li>
     * <li><b>!ip6~<i>regex</i></b> - any IPv6 address that does NOT match the specified regular expression</li>
     *
     * <li><b>net~<i>regex</i></b> - any IP address of a network interface who's {@link NetworkInterface#getName() name} matches the
     * specified regular expression</li>
     * <li><b>net4~<i>regex</i></b> - IPv4 address of a network interface who's {@link NetworkInterface#getName() name} matches the
     * specified regular expression</li>
     * <li><b>net6~<i>regex</i></b> - IPv6 address of a network interface who's {@link NetworkInterface#getName() name} matches the
     * specified regular expression</li>
     *
     * <li><b>!net~<i>regex</i></b> - any IP address of a network interface who's {@link NetworkInterface#getName() name} does NOT match
     * the specified regular expression</li>
     * <li><b>!net4~<i>regex</i></b> - IPv4 address of a network interface who's {@link NetworkInterface#getName() name} does NOT match the
     * specified regular expression</li>
     * <li><b>!net6~<i>regex</i></b> - IPv6 address of a network interface who's {@link NetworkInterface#getName() name} does NOT match the
     * specified regular expression</li>
     *
     * <li>...all other values will be treated as a directly specified address (see {@link InetAddress#getByName(String)})</li>
     * </ul>
     *
     * <p>
     * If multiple addresses match the specified pattern then the first one will be selected (order is not guaranteed).
     * </p>
     *
     * @param pattern Address selection pattern (see {@link AddressPattern}).
     *
     * @see #setHostSelector(AddressSelector)
     * @see AddressPattern
     */
    public void setHost(String pattern) {
        setHostSelector(new AddressPattern(pattern));
    }

    /**
     * Fluent-style version of {@link #setHost(String)}.
     *
     * @param pattern Address selection pattern (see {@link AddressPattern}).
     *
     * @return This instance.
     *
     * @see AddressPattern
     */
    public NetworkServiceFactory withHost(String pattern) {
        setHost(pattern);

        return this;
    }

    /**
     * Returns communications port of the local node (see {@link #setPort(int)}).
     *
     * @return The port number.
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets port number that will be used by the local node to accept network connections.
     *
     * <p>
     * If specified port number is {@code 0} then <a href="https://en.wikipedia.org/wiki/Ephemeral_port" target="_blank">ephemeral port</a>
     * will be assigned by the underlying OS.
     * </p>
     *
     * <p>
     * Note that it is possible to specify port ranges via {@link #setPortRange(int)} method.
     * </p>
     *
     * <p>
     * Default value of this parameter is {@value #DEFAULT_PORT}
     * </p>
     *
     * @param port The port number.
     *
     * @see #setPortRange(int)
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Fluent-style version of {@link #setPort(int)}.
     *
     * @param port The port number.
     *
     * @return This instance.
     */
    public NetworkServiceFactory withPort(int port) {
        setPort(port);

        return this;
    }

    /**
     * Returns the port range (see {@link #setPortRange(int)}).
     *
     * @return Port range.
     */
    public int getPortRange() {
        return portRange;
    }

    /**
     * Sets maximum value that can be used for {@link #setPort(int) port} auto-incrementing.
     *
     * <p>
     * Local node will try to bind on the first available port starting with {@link #getPort()} up to {@link #getPort()} + {@link
     * #getPortRange()}. If value of this property is less than or equals to zero then port auto-incrementing will be disabled.
     * </p>
     *
     * <p>
     * Default value of this parameter is {@value #DEFAULT_PORT_RANGE}
     * </p>
     *
     * @param portRange Port range.
     */
    public void setPortRange(int portRange) {
        this.portRange = portRange;
    }

    /**
     * Fluent-style version of {@link #setPortRange(int)}.
     *
     * @param portRange Port range.
     *
     * @return This instance.
     */
    public NetworkServiceFactory withPortRange(int portRange) {
        setPortRange(portRange);

        return this;
    }

    /**
     * Returns the TCP connect timeout in milliseconds (see {@link #setConnectTimeout(int)}).
     *
     * @return Timeout in milliseconds.
     */
    public int getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Sets the timeout in milliseconds for establishing a TCP socket connection to a remote node.
     *
     * <p>
     * Negative value or zero means that no timeout should happen.
     * </p>
     *
     * <p>
     * The default value of this parameter is {@value #DEFAULT_CONNECT_TIMEOUT}.
     * </p>
     *
     * @param connectTimeout Timeout in milliseconds.
     */
    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /**
     * Fluent-style version of {@link #setConnectTimeout(int)}.
     *
     * @param connectTimeout Timeout in milliseconds.
     *
     * @return This instance.
     */
    public NetworkServiceFactory withConnectTimeout(int connectTimeout) {
        setConnectTimeout(connectTimeout);

        return this;
    }

    /**
     * Returns the number of milliseconds to wait before trying to re-create a TCP socket acceptor in case of a failure (see {@link
     * #setAcceptRetryInterval(long)}).
     *
     * @return Number of milliseconds.
     */
    public long getAcceptRetryInterval() {
        return acceptRetryInterval;
    }

    /**
     * Sets the number of milliseconds to wait before trying to re-create a TCP socket acceptor in case of its failure.
     *
     * <p>
     * Negative value or zero means that retrying should not take place and {@link Hekate} instance must stop with an error.
     * </p>
     *
     * <p>
     * The default value of this parameter is {@value #DEFAULT_ACCEPT_RETRY_INTERVAL}.
     * </p>
     *
     * @param acceptRetryInterval Number of milliseconds to wait.
     */
    public void setAcceptRetryInterval(long acceptRetryInterval) {
        this.acceptRetryInterval = acceptRetryInterval;
    }

    /**
     * Fluent-style version of {@link #setAcceptRetryInterval(long)}.
     *
     * @param acceptRetryInterval Number of milliseconds to wait.
     *
     * @return This instance.
     */
    public NetworkServiceFactory withAcceptRetryInterval(long acceptRetryInterval) {
        setAcceptRetryInterval(acceptRetryInterval);

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
    public NetworkServiceFactory withHeartbeatInterval(int heartbeatInterval) {
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
    public NetworkServiceFactory withHeartbeatLossThreshold(int heartbeatLossThreshold) {
        setHeartbeatLossThreshold(heartbeatLossThreshold);

        return this;
    }

    /**
     * Returns the number of threads to be used for NIO event processing (see {@link #setNioThreads(int)}).
     *
     * @return Number of threads for NIO event processing.
     */
    public int getNioThreads() {
        return nioThreads;
    }

    /**
     * Sets the number of threads to be used for NIO event processing.
     *
     * <p>
     * Value of this parameter must be above zero.
     * </p>
     *
     * <p>
     * Default value of this parameter is the number of CPUs available to the JVM (see {@link Runtime#availableProcessors()}).
     * </p>
     *
     * @param nioThreads Number of threads for NIO event processing.
     */
    public void setNioThreads(int nioThreads) {
        this.nioThreads = nioThreads;
    }

    /**
     * Fluent-style version of {@link #setNioThreads(int)}.
     *
     * @param nioThreads Number of threads for NIO event processing.
     *
     * @return This instance.
     */
    public NetworkServiceFactory withNioThreads(int nioThreads) {
        setNioThreads(nioThreads);

        return this;
    }

    /**
     * Returns the transport type (see {@link #setTransport(NetworkTransportType)}).
     *
     * @return Transport type.
     */
    public NetworkTransportType getTransport() {
        return transport;
    }

    /**
     * Sets the transport type that controls which implementation of NIO API should be used.
     *
     * <p>
     * Possible values are:
     * </p>
     * <ul>
     * <li>{@link NetworkTransportType#NIO} - Use the default implementation that is provided by the JVM.</li>
     * <li>{@link NetworkTransportType#EPOLL} - Use the optimized <a href="https://en.wikipedia.org/wiki/Epoll"
     * target="_blank">Epoll</a>-based implementation (for Linux environments only). Note that this implementation requires Netty's
     * <a href="http://netty.io/wiki/native-transports.html" target="_blank">'netty-transport-native-epoll'</a> module to be on the
     * classpath.</li>
     * <li>{@link NetworkTransportType#AUTO} - Try to autodetect which implementation to use depending on the runtime environment.</li>
     * </ul>
     *
     * <p>
     * This parameter is mandatory and can't be {@code null}. Default value of this parameter is {@link NetworkTransportType#AUTO}
     * </p>
     *
     * @param transport Transport type.
     */
    public void setTransport(NetworkTransportType transport) {
        this.transport = transport;
    }

    /**
     * Fluent-style version of {@link #setTransport(NetworkTransportType)}.
     *
     * @param transport Transport type.
     *
     * @return This instance.
     */
    public NetworkServiceFactory withTransport(NetworkTransportType transport) {
        setTransport(transport);

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
    public NetworkServiceFactory withTcpNoDelay(Boolean tcpNoDelay) {
        setTcpNoDelay(tcpNoDelay);

        return this;
    }

    /**
     * Returns the size of the socket receive buffer in bytes (see {@link #setTcpReceiveBufferSize(Integer)}).
     *
     * @return Buffer size in bytes or {@code null} if it wasn't set.
     */
    public Integer getTcpReceiveBufferSize() {
        return tcpReceiveBufferSize;
    }

    /**
     * Sets the size of the socket receive buffer in bytes (see {@link StandardSocketOptions#SO_RCVBUF} for more details).
     *
     * <p>
     * Default value of this parameter is {@code null}.
     * </p>
     *
     * @param tcpReceiveBufferSize Buffer size in bytes.
     */
    public void setTcpReceiveBufferSize(Integer tcpReceiveBufferSize) {
        this.tcpReceiveBufferSize = tcpReceiveBufferSize;
    }

    /**
     * Fluent-style version of {@link #setTcpReceiveBufferSize(Integer)}.
     *
     * @param tcpReceiveBufferSize Buffer size in bytes.
     *
     * @return This instance.
     */
    public NetworkServiceFactory withTcpReceiveBufferSize(Integer tcpReceiveBufferSize) {
        setTcpReceiveBufferSize(tcpReceiveBufferSize);

        return this;
    }

    /**
     * Returns the size of the socket send buffer in bytes (see {@link #setTcpSendBufferSize(Integer)}).
     *
     * @return Buffer size in bytes or {@code null} if it wasn't set.
     */
    public Integer getTcpSendBufferSize() {
        return tcpSendBufferSize;
    }

    /**
     * Sets the size of the socket send buffer in bytes (see {@link StandardSocketOptions#SO_SNDBUF} for more details).
     *
     * <p>
     * Default value of this parameter is {@code null}.
     * </p>
     *
     * @param tcpSendBufferSize Buffer size in bytes.
     */
    public void setTcpSendBufferSize(Integer tcpSendBufferSize) {
        this.tcpSendBufferSize = tcpSendBufferSize;
    }

    /**
     * Fluent-style version of {@link #setTcpSendBufferSize(Integer)}.
     *
     * @param soSendBufferSize Buffer size in bytes.
     *
     * @return This instance.
     */
    public NetworkServiceFactory withTcpSendBufferSize(Integer soSendBufferSize) {
        setTcpSendBufferSize(soSendBufferSize);

        return this;
    }

    /**
     * Sets flag indicating that socket addresses should be re-used (see {@link #setTcpReuseAddress(Boolean)}).
     *
     * @return Flag value or {@code null} if it wasn't set.
     */
    public Boolean getTcpReuseAddress() {
        return tcpReuseAddress;
    }

    /**
     * Sets flag indicating that socket addresses should be re-used (see {@link StandardSocketOptions#SO_REUSEADDR} for more details).
     *
     * <p>
     * Default value of this parameter is {@code null}.
     * </p>
     *
     * @param tcpReuseAddress Flag indicating that socket addresses should be re-used.
     */
    public void setTcpReuseAddress(Boolean tcpReuseAddress) {
        this.tcpReuseAddress = tcpReuseAddress;
    }

    /**
     * Fluent-style version of {@link #setTcpReuseAddress(Boolean)}.
     *
     * @param soReuseAddress Flag indicating that socket addresses should be re-used.
     *
     * @return This instance.
     */
    public NetworkServiceFactory withTcpReuseAddress(Boolean soReuseAddress) {
        setTcpReuseAddress(soReuseAddress);

        return this;
    }

    /**
     * Returns the maximum number of pending connections that can be queued on the server socket channel (see {@link
     * #setTcpBacklog(Integer)}).
     *
     * @return The maximum number of pending connections or {@code null} if it wasn't set.
     */
    public Integer getTcpBacklog() {
        return tcpBacklog;
    }

    /**
     * Sets the maximum number of pending connections that can be queued on the server socket channel
     * (see {@link ServerSocketChannel#bind(SocketAddress, int)} for more details).
     *
     * <p>
     * Default value of this parameter is {@code null}.
     * </p>
     *
     * @param tcpBacklog The maximum number of pending connections.
     */
    public void setTcpBacklog(Integer tcpBacklog) {
        this.tcpBacklog = tcpBacklog;
    }

    /**
     * Fluent-style version of {@link #setTcpBacklog(Integer)}.
     *
     * @param soBacklog The maximum number of pending connections.
     *
     * @return This instance.
     */
    public NetworkServiceFactory withTcpBacklog(Integer soBacklog) {
        setTcpBacklog(soBacklog);

        return this;
    }

    /**
     * Returns the list of connector configurations (see {@link #setConnectors(List)}).
     *
     * @return List of connector configurations.
     */
    public List<NetworkConnectorConfig<?>> getConnectors() {
        return connectors;
    }

    /**
     * Sets the list of connector configurations that should be registered within the {@link NetworkService}.
     *
     * @param connectors Connector configurations.
     *
     * @see NetworkService#connector(String)
     */
    public void setConnectors(List<NetworkConnectorConfig<?>> connectors) {
        this.connectors = connectors;
    }

    /**
     * Fluent-style version of {@link #setConnectors(List)}.
     *
     * @param connector Connector configuration.
     *
     * @return This instance.
     */
    public NetworkServiceFactory withConnector(NetworkConnectorConfig<?> connector) {
        if (connectors == null) {
            connectors = new ArrayList<>();
        }

        connectors.add(connector);

        return this;
    }

    /**
     * Returns the list of connector configuration providers (see {@link #setConfigProviders(List)}).
     *
     * @return Connector configuration providers.
     */
    public List<NetworkConfigProvider> getConfigProviders() {
        return configProviders;
    }

    /**
     * Sets the list of connector configuration providers.
     *
     * @param configProviders Connector configuration providers.
     *
     * @see NetworkConfigProvider
     */
    public void setConfigProviders(List<NetworkConfigProvider> configProviders) {
        this.configProviders = configProviders;
    }

    /**
     * Fluent-style version of {@link #setConfigProviders(List)}.
     *
     * @param configProvider Connector configuration provider.
     *
     * @return This instance.
     */
    public NetworkServiceFactory withConfigProvider(NetworkConfigProvider configProvider) {
        if (configProviders == null) {
            configProviders = new ArrayList<>();
        }

        configProviders.add(configProvider);

        return this;
    }

    /**
     * Returns the SSL configuration (see {@link #setSsl(NetworkSslConfig)}).
     *
     * @return SSL configuration.
     */
    public NetworkSslConfig getSsl() {
        return ssl;
    }

    /**
     * Sets the SSL configuration.
     *
     * <p>
     * If the specified configuration is not {@code null} then SSL encryption will be applied to all network communications.
     * Note that SSL must be enabled/disabled on all nodes in the cluster. <b>Mixed mode</b> where some nodes have SSL enabled and some
     * nodes have SSL disabled <b>is not supported</b>.
     * </p>
     *
     * @param ssl SSL configuration.
     */
    public void setSsl(NetworkSslConfig ssl) {
        this.ssl = ssl;
    }

    /**
     * Fluent-style version of {@link #setSsl(NetworkSslConfig)}.
     *
     * @param ssl SSL configuration.
     *
     * @return This instance.
     */
    public NetworkServiceFactory withSsl(NetworkSslConfig ssl) {
        setSsl(ssl);

        return this;
    }

    @Override
    public NetworkService createService() {
        return new NettyNetworkService(this);
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
