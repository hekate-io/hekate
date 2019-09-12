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

import io.hekate.codec.Codec;
import io.hekate.codec.CodecFactory;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.DefaultServiceFactory;
import io.hekate.core.service.Service;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * <span class="startHere">&laquo; start here</span>Entry point to TCP-based client/server communication API.
 *
 * <h2>Overview</h2>
 * <p>
 * {@link NetworkService} provides an abstraction layer on top of sockets API for building connection-oriented communication protocols.
 * </p>
 *
 * <ul>
 * <li><a href="#service_configuration">Service Configuration</a></li>
 * <li><a href="#connectors">Connectors</a></li>
 * <li><a href="#connectors_configuration">Connectors Configuration</a></li>
 * <li><a href="#protocol_identifier">Protocol Identifier</a></li>
 * <li><a href="#ssl_encryption">SSL Encryption</a></li>
 * <li><a href="#data_serialization">Data Serialization</a></li>
 * <li><a href="#thread_management">Thread Management</a></li>
 * <li><a href="#example">Example</a></li>
 * </ul>
 *
 * <a name="service_configuration"></a>
 * <h2>Service Configuration</h2>
 * <p>
 * {@link NetworkService} can be configured and registered within the {@link HekateBootstrap} via the {@link NetworkServiceFactory} class
 * as in the example below:
 * </p>
 *
 * <div class="tabs">
 * <ul>
 * <li><a href="#configure-java">Java</a></li>
 * <li><a href="#configure-xsd">Spring XSD</a></li>
 * <li><a href="#configure-bean">Spring bean</a></li>
 * </ul>
 * <div id="configure-java">
 * ${source: network/NetworkServiceJavadocTest.java#config}
 * </div>
 * <div id="configure-xsd">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: network/service-xsd.xml#example}
 * </div>
 * <div id="configure-bean">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: network/service-bean.xml#example}
 * </div>
 * </div>
 *
 * <p>
 * Please see the documentation of {@link NetworkServiceFactory} class for more details about the available configuration options.
 * </p>
 *
 * <a name="connectors"></a>
 * <h2>Connectors</h2>
 * <p>
 * Communication units in the {@link NetworkService} are represented by the {@link NetworkConnector} interface. This interface provides API
 * for creating client connections based on the connector options as well as accepting connections from remote clients (optional).
 * </p>
 *
 * <p>
 * The client side of a connector API is represented by the {@link NetworkClient} interface. Each client manages a single socket
 * connection and provides API for connecting to remote endpoints and sending/receiving messages to/from them. Instances if this interface
 * can be be obtained from {@link NetworkConnector} as illustrated in the <a href="#client_example">example below</a>.
 * </p>
 *
 * <p>
 * The server side of a connector API is represented by the {@link NetworkServerHandler} interface. This interface provides callback
 * methods that get notified upon various events of remote clients (connects, disconnects, new messages, etc). Instances of this
 * interface can be registered via {@link NetworkConnectorConfig#setServerHandler(NetworkServerHandler)} method as illustrated in the
 * <a href="#server_example">example below</a>.
 * </p>
 *
 * <p>
 * <b>Note:</b> {@link NetworkServerHandler} is optional and if not specified for particular {@link NetworkConnector} then such connector
 * will act in a pure client mode and will not be able to accept connections from remote addresses.
 * </p>
 *
 * <a name="connectors_configuration"></a>
 * <h2>Connectors Configuration</h2>
 * <p>
 * {@link NetworkConnector} configuration is represented by the {@link NetworkConnectorConfig} class. Please see its documentation for the
 * complete list of all available configuration options.
 * </p>
 *
 * <p>
 * Instances of this class can be registered within the {@link NetworkService} via {@link NetworkServiceFactory#setConnectors(List)}
 * method.
 * </p>
 *
 * <a name="protocol_identifier"></a>
 * <h2>Protocol Identifier</h2>
 * <p>
 * Each connector must have a protocol identifier. This identifier is used by the {@link NetworkService} to select which {@link
 * NetworkConnector} should be responsible for processing each particular connection from a remote {@link NetworkClient}. When {@link
 * NetworkClient} established a new connection to a remote {@link NetworkService} it submits its protocol identifier as part of an initial
 * handshake message. This identifier is used by the remote {@link NetworkService} to select a {@link NetworkConnector} instance that is
 * configured with exactly the same protocol identifier. If such instance can be found then all subsequent communication events will be
 * handled by its {@link NetworkServerHandler}. If such instance can't be found then connection will be rejected.
 * </p>
 *
 * <a name="ssl_encryption"></a>
 * <h2>SSL Encryption</h2>
 * <p>
 * It is possible to configure {@link NetworkService} to use secure communications by setting {@link
 * NetworkServiceFactory#setSsl(NetworkSslConfig) SSL configuration}. Please see the documentation of the {@link NetworkSslConfig}
 * class for available configuration options.
 * </p>
 *
 * <p>
 * Note that SSL encryption will be applied to all network communications at the cluster node level, thus it is important to make sure that
 * all nodes in the cluster are configured to use SSL encryption. Mixed mode, when some nodes do use SSL and some do not use it, is not
 * supported. In such case non-SSL nodes will not be able to connect to SSL-enabled nodes and vice versa.
 * </p>
 *
 * <p>
 * Protocol identifier must be specified within the {@link NetworkConnector} configuration via {@link
 * NetworkConnectorConfig#setProtocol(String)} method.
 * </p>
 *
 * <a name="data_serialization"></a>
 * <h2>Data Serialization</h2>
 * <p>
 * Data serialization and deserialization within connectors is handled by the {@link Codec} interface. Instances of this interface can
 * be specified for each connector independently via {@link NetworkConnectorConfig#setMessageCodec(CodecFactory)} method. If not
 * specified the the default general purpose codec of a {@link Hekate} instance will be used (see {@link
 * HekateBootstrap#setDefaultCodec(CodecFactory)}).
 * </p>
 *
 * <p>
 * Please see the documentation of {@link Codec} interface for more details about data serialization.
 * </p>
 *
 * <a name="thread_management"></a>
 * <h2>Thread Management</h2>
 * <p>
 * {@link NetworkService} manages a core NIO thread pool of {@link NetworkServiceFactory#setNioThreads(int)} size. This thread pools
 * is used to process all incoming and outgoing connections by default.
 * </p>
 *
 * <p>
 * It is also possible to configure each {@link NetworkConnector} to use its own thread pool via {@link
 * NetworkConnectorConfig#setNioThreads(int)} option. In such case all incoming and outgoing connections of that connector will be
 * handled by a dedicated thread pool of the specified size and will not interfere with {@link NetworkService}'s core thread nor with
 * thread pool of any other connector.
 * </p>
 *
 * <p>
 * Whenever a new connection is created by the connector (either {@link NetworkClient client connection} or {@link
 * NetworkServerHandler#onConnect(Object, NetworkEndpoint)} server connection}) it obtains a worker thread from the {@link
 * NetworkConnector}'s thread pool and uses this thread to process all of the NIO events. Due to the event-based nature of NIO each thread
 * can handle multiple connections and doesn't require a one-to-one relationship between the pool size and the amount of active
 * connections. Typically thread pool size must be much less than the number of active connections. When connection gets closed it
 * unregisters itself from its worker thread.
 * </p>
 *
 * <a name="example"></a>
 * <h2>Example</h2>
 * <p>
 * The code example below shows how {@link NetworkService} can be used to implement client/server communications. For the sake of brevity
 * this example uses the default Java serialization and messages of {@link String} type. For real world applications it is recommended to
 * implement custom message classes and provide a more optimized implementation of {@link Codec} in order to increase communication
 * speed and to support a more complex application logic.
 * </p>
 *
 * <a name="server_example"></a>
 * <h3>Server Example</h3>
 * <p>
 * 1) Prepare server handler.
 * ${source: network/NetworkServiceJavadocTest.java#server_handler_example}
 * </p>
 *
 * <a name="server_example_connector"></a>
 * <p>
 * 2) Prepare connector configuration.
 * ${source: network/NetworkServiceJavadocTest.java#server_handler_config_example}
 * </p>
 *
 * <p>
 * 3) Start new node.
 * ${source: network/NetworkServiceJavadocTest.java#server_example}
 * </p>
 *
 * <a name="client_example"></a>
 * <h3>Client Example</h3>
 * <p>
 * <b>Note:</b> This example uses the same connector configuration as in the <a href="#server_example_connector">server example.</a>
 * </p>
 *
 * <p>
 * 1) Instantiate a new client and connect to the server.
 * ${source: network/NetworkServiceJavadocTest.java#client_connect_example}
 * </p>
 *
 * <p>
 * 2) Start sending messages (can be done even if connection establishment is still in progress).
 * ${source: network/NetworkServiceJavadocTest.java#client_send_example}
 * </p>
 *
 * @see NetworkServiceFactory
 */
@DefaultServiceFactory(NetworkServiceFactory.class)
public interface NetworkService extends Service {
    /**
     * Returns a connector instance for the specified {@link NetworkConnectorConfig#setProtocol(String) protocol name}.
     *
     * <p>
     * Please see the overview section of this class for more details about connectors.
     * </p>
     *
     * @param protocol Protocol name (see {@link NetworkConnectorConfig#setProtocol(String)}).
     * @param <T> Base type of connector protocol messages.
     *
     * @return TCP connector instance.
     *
     * @throws IllegalArgumentException If there is no such connector with the specified protocol name.
     */
    <T> NetworkConnector<T> connector(String protocol) throws IllegalArgumentException;

    /**
     * Returns {@code true} if this service has a connector with the specified protocol name.
     *
     * @param protocol Protocol name (see {@link NetworkConnectorConfig#setProtocol(String)}).
     *
     * @return {@code true} if connector exists.
     */
    boolean hasConnector(String protocol);

    /**
     * Asynchronously checks if connection can be established with a {@link NetworkService} at the specified address and notifies the
     * provided callback on operation result.
     *
     * <p>
     * Example:
     * ${source: network/NetworkServiceJavadocTest.java#ping}
     * </p>
     *
     * @param address Address.
     * @param callback Callback to be notified.
     *
     * @see NetworkPingResult
     */
    void ping(InetSocketAddress address, NetworkPingCallback callback);
}
