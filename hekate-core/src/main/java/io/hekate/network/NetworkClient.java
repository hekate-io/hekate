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
import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.Optional;

/**
 * Asynchronous network client connection.
 *
 * <p>
 * <b>Note:</b> for generic overview of networking and usage example please see the documentation of {@link NetworkService}.
 * </p>
 *
 * <h2>Instantiation</h2>
 * <p>
 * Instances of this interface can be obtained via {@link NetworkConnector#newClient()} method.
 * </p>
 *
 * <h2>Connecting and Disconnecting</h2>
 * <p>
 * Each {@link NetworkClient} instance manages a single NIO channel that is created when {@link #connect(InetSocketAddress,
 * NetworkClientCallback) connect(...)} method is called and destroyed when {@link #disconnect()} is called. Both of those  methods are
 * asynchronous and return a {@link NetworkFuture future} object that can be used get asynchronous operation results.
 * </p>
 *
 * <p>
 * {@link #connect(InetSocketAddress, NetworkClientCallback) connect(...)} method accepts an instance of {@link NetworkClientCallback}
 * interface that can be used to get notified upon {@link #state()}  connection state} changes and listen for incoming messages from
 * server.
 * </p>
 *
 * <p>
 * When {@link NetworkClient} instance is not needed anymore it must be disconnected by calling its {@link #disconnect()} method in order
 * to release system resources.
 * </p>
 *
 * <p>
 * Its is possible to re-connect the same {@link NetworkClient} instance (even to a different address), assuming that such client is in
 * {@link #disconnect() disconnected} state before trying to establish a new connection.
 * </p>
 *
 * <h2>Sending and Receiving Messages</h2>
 * <p>
 * Messages sending can be done by calling {@link #send(Object, NetworkSendCallback)} method that accepts a message to be sent and a
 * callback that will be notified upon operation completion. Note that message sending operation is asynchronous and doesn't block the
 * caller thread.
 * </p>
 *
 * <p>
 * <b>Note:</b> After {@link #connect(InetSocketAddress, NetworkClientCallback) connect(...)} method is called it is possible to start
 * {@link #send(Object, NetworkSendCallback) sending} messages immediately without awaiting for operation completion. Such messages will be
 * kept in the internal buffer and will be asynchronously sent once the TCP connection is established (or will fail in case of connection
 * error).
 * </p>
 *
 * <p>
 * Receiving messages from server can be done by providing an instance of {@link NetworkClientCallback} while {@link
 * #connect(InetSocketAddress, Object, NetworkClientCallback) connecting} and implementing its {@link
 * NetworkClientCallback#onMessage(NetworkMessage, NetworkClient)} method. This method gets notified on every message that gets received by
 * the {@link NetworkClient} instance from a server.
 * </p>
 *
 * <h2>Messages Serialization</h2>
 * <p>
 * Messages serialization and deserialization is handled by the {@link CodecFactory} interface. Implementations of this interface can
 * be registered via {@link NetworkConnectorConfig#setMessageCodec(CodecFactory)} method. Every time when {@link NetworkClient}
 * establishes a new connection it uses this factory to create an instance of {@link Codec} that will performs actual data encoding and
 * decoding.
 * </p>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * {@link NetworkClient} is thread safe and can be called by multiple threads in parallel.
 * </p>
 *
 * @param <T> Base type of messages that can be sent/received by this {@link NetworkClient}. Note that {@link NetworkClient} must be {@link
 * NetworkConnectorConfig#setMessageCodec(CodecFactory) configured} to use a {@link Codec} which is capable of serializing messages
 * of this type.
 *
 * @see NetworkService
 * @see NetworkConnector
 */
public interface NetworkClient<T> extends NetworkEndpoint<T>, Closeable {
    /**
     * State of {@link NetworkClient} connection lifecycle.
     */
    enum State {
        /**
         * Client is disconnected.
         */
        DISCONNECTED,

        /**
         * Connection establishment is in process.
         */
        CONNECTING,

        /**
         * Client is connected.
         */
        CONNECTED,

        /**
         * Client is disconnecting.
         */
        DISCONNECTING
    }

    /**
     * Asynchronously established a new socket connection to a {@link NetworkService} at the specified address.
     *
     * <p>
     * If operation completes successfully then {@link NetworkClientCallback#onConnect(NetworkClient)} will be notified. If operation
     * completes with an error then {@link NetworkClientCallback#onDisconnect(NetworkClient, Optional)} will be notified.
     * </p>
     *
     * <p>
     * Note that it is possible to start {@link #send(Object, NetworkSendCallback) sending} messages without awaiting for completion of
     * this operation. Such messages will be buffered and submitted once this operation completes.
     * </p>
     *
     * @param address Socket address where {@link NetworkService} is running.
     * @param callback Connection callback.
     *
     * @return Future object that can be used to obtain result of this operation.
     *
     * @throws IllegalStateException Thrown if this {@link NetworkClient} is already in {@link NetworkClient.State#CONNECTING} or {@link
     * NetworkClient.State#CONNECTED} state.
     */
    NetworkFuture<T> connect(InetSocketAddress address, NetworkClientCallback<T> callback) throws IllegalStateException;

    /**
     * Asynchronously established a new socket connection to a {@link NetworkService} at the specified address and submits an optional
     * login message. This message will be passed to the {@link NetworkServerHandler#onConnect(Object, NetworkEndpoint)} method at the
     * received side.
     *
     * <p>
     * If operation completes successfully then {@link NetworkClientCallback#onConnect(NetworkClient)} will be notified. If operation
     * completes with an error then {@link NetworkClientCallback#onConnect(NetworkClient)} will be notified.
     * </p>
     *
     * @param address Socket address where {@link NetworkService} is running.
     * @param login Login message that should be passed to the {@link NetworkServerHandler#onConnect(Object, NetworkEndpoint)} method.
     * Can be {@code null}.
     * @param callback Connection callback.
     *
     * @return Future object that can be used to obtain result of this operation.
     *
     * @throws IllegalStateException Thrown if this {@link NetworkClient} is already in {@link NetworkClient.State#CONNECTING} or {@link
     * NetworkClient.State#CONNECTED} state.
     */
    NetworkFuture<T> connect(InetSocketAddress address, T login, NetworkClientCallback<T> callback) throws IllegalStateException;

    /**
     * Returns the connection state.
     *
     * @return Connection state.
     */
    State state();

    /**
     * Support method for {@link Closeable} interface (equivalent of calling {@link #disconnect()} and {@link NetworkFuture#join()}).
     */
    @Override
    void close();
}
