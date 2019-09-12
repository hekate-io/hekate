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

import java.net.InetSocketAddress;
import java.util.function.Consumer;

/**
 * Base interface for network communication endpoints.
 *
 * @param <T> Base type of messages that can be supported by this endpoint.
 */
public interface NetworkEndpoint<T> {
    /**
     * Returns the protocol name of this endpoint.
     *
     * @return Protocol name.
     *
     * @see NetworkConnectorConfig#setProtocol(String)
     */
    String protocol();

    /**
     * Returns the remote address of this endpoint. Returns {@code null} if this endpoint is not connected.
     *
     * @return Remote address or {@code null} if endpoint is not connected.
     */
    InetSocketAddress remoteAddress();

    /**
     * Returns the local address of this endpoint. Returns {@code null} if this endpoint is not connected.
     *
     * @return Local address or {@code null} if this endpoint is not connected.
     */
    InetSocketAddress localAddress();

    /**
     * Returns {@code true} if SSL is enabled (see {@link NetworkServiceFactory#setSsl(NetworkSslConfig)}).
     *
     * @return {@code true} if SSL.
     */
    boolean isSecure();

    /**
     * Returns the custom user context object that was set via {@link #setContext(Object)}.
     *
     * @return Context object.
     */
    Object getContext();

    /**
     * Sets the custom user context object that should be associated with this endpoint.
     *
     * @param ctx Context object.
     */
    void setContext(Object ctx);

    /**
     * Asynchronously sends the specified message.
     *
     * <p>
     * Note that this methods doesn't provide any feedback on whether operation completes successfully or with an error. Consider using
     * {@link #send(Object, NetworkSendCallback)} method if such feedback is required.
     * </p>
     *
     * @param msg Message.
     */
    void send(T msg);

    /**
     * Asynchronously sends the specified message and notifies the specified callback on operation result.
     *
     * <p>
     * Note that the specified callback will be called on the {@link NetworkEndpoint}'s NIO thread. It must be executed as fast as possible
     * in order to prevent blocking other endpoints that can be associated with the same thread. In case of long/heavy computations
     * consider scheduling such tasks to another thread.
     * </p>
     *
     * @param msg Message.
     * @param callback Callback.
     */
    void send(T msg, NetworkSendCallback<T> callback);

    /**
     * Pauses receiving of messages from a remote peer. Does nothing if receiving is already paused or is disconnected.
     *
     * <p>
     * Note that pausing is an asynchronous operation and some messages, that were buffered prior to this method call, can still be
     * received.
     * </p>
     *
     * @param callback Optional callback to be notified once pausing takes effect (can be {@code null}).
     *
     * @see #resumeReceiving(Consumer)
     * @see #isReceiving()
     */
    void pauseReceiving(Consumer<NetworkEndpoint<T>> callback);

    /**
     * Resumes receiving of messages from a remote peer. Does nothing if receiving is not paused or is disconnected.
     *
     * @param callback Optional callback to be notified once resuming takes effect (can be {@code null}).
     *
     * @see #pauseReceiving(Consumer)
     * @see #isReceiving()
     */
    void resumeReceiving(Consumer<NetworkEndpoint<T>> callback);

    /**
     * Returns {@code false} if receiving of messages from a remote peer is {@link #pauseReceiving(Consumer)} paused} or if this endpoint
     * is disconnected.
     *
     * @return {@code false} if receiving is paused via {@link #pauseReceiving(Consumer)}.
     *
     * @see #pauseReceiving(Consumer)
     * @see #resumeReceiving(Consumer)
     */
    boolean isReceiving();

    /**
     * Asynchronously disconnects this endpoint.
     *
     * @return Future object that can be used to obtain result of this operation.
     */
    NetworkFuture<T> disconnect();
}
