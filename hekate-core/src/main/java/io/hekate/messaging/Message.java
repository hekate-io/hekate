/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.messaging;

import io.hekate.failover.FailoverPolicy;
import io.hekate.messaging.unicast.Response;
import io.hekate.messaging.unicast.ResponseCallback;
import io.hekate.messaging.unicast.SendCallback;

/**
 * Message with arbitrary payload.
 *
 * <p>
 * This interface represents a message that was received by a {@link MessagingChannel} and provides methods for sending responses.
 * </p>
 *
 * <p>
 * For more information about messaging please see the {@link MessagingService} documentation.
 * </p>
 *
 * @param <T> Base type of the message payload.
 *
 * @see MessageReceiver
 */
public interface Message<T> extends MessageBase<T> {
    /**
     * Returns {@code true} if the sender is waiting for a response message.
     *
     * <p>
     * Responses can be sent back via the following methods:
     * </p>
     * <ul>
     * <li>{@link #reply(Object)}</li>
     * <li>{@link #reply(Object, SendCallback)} </li>
     * </ul>
     *
     * <p>
     * <b>Note:</b> Not responding on messages that have this flag set to {@code true} can result in resource leaks, since the sending side
     * keeps track of all outstanding requests in its memory queue and releases resources only when a response is received.
     * </p>
     *
     * @return {@code true} if the sender is waiting for a response message.
     *
     * @see #reply(Object, SendCallback)
     */
    boolean mustReply();

    /**
     * Asynchronously send back a reply and ignores the operation result.
     *
     * <p>
     * Calling this method will put the message into the 'replied' state and its {@link #mustReply()} method will start returning {@code
     * false}. Note that each message can be responded only once and all subsequent attempts to call this method will result in {@link
     * IllegalStateException}.
     * </p>
     *
     * <p>
     * Any attempt to call this method on a message that doesn't support responses (i.e. message not a {@link #isRequest() request}) will
     * result in {@link IllegalStateException}.
     * </p>
     *
     * @param response Response.
     *
     * @throws UnsupportedOperationException If message doesn't support responses.
     * @throws IllegalStateException If message had already been responded (see {@link #mustReply()}).
     */
    void reply(T response) throws UnsupportedOperationException, IllegalStateException;

    /**
     * Asynchronously sends a response and uses the specified callback for notification of the operation result.
     *
     * <p>
     * Calling this method will put the message into the 'replied' state and its {@link #mustReply()} method will start returning {@code
     * false}. Note that each message can be responded only once and all subsequent attempts to call this method will result in {@link
     * IllegalStateException}.
     * </p>
     *
     * <p>
     * Any attempt to call this method on a message that doesn't support responses (i.e. message not a {@link #isRequest() request}) will
     * result in {@link IllegalStateException}.
     * </p>
     *
     * @param response Response.
     * @param callback Callback for tracking completion of an operation.
     *
     * @throws UnsupportedOperationException If message doesn't support responses.
     * @throws IllegalStateException If message had already been responded (see {@link #mustReply()}).
     */
    void reply(T response, SendCallback callback) throws UnsupportedOperationException, IllegalStateException;

    /**
     * Asynchronously sends a partial reply in response to a
     * {@link MessagingChannel#subscribe(Object, ResponseCallback) subscription request}.
     *
     * <p>
     * This method provides support for streaming messages from the recipient back to the sender.
     * For example, the sender can subscribe to a topic using the {@link MessagingChannel#subscribe(Object, ResponseCallback)} method,
     * and then the recipient of such a request can start to continuously stream updates back to the sender. The subscriber will receive
     * those updates via the {@link ResponseCallback#onComplete(Throwable, Response)} method. After the streaming is complete, the final
     * response must be sent using the {@link #reply(Object)} method.
     * </p>
     *
     * <p>
     * <b>Note:</b> This method can only be called for messages that were sent using the
     * {@link MessagingChannel#subscribe(Object, ResponseCallback)} method (or its overloaded versions). If the message was sent by some
     * other method, then all attempts to call this method will throw {@link UnsupportedOperationException}.
     * </p>
     *
     * <p>
     * The verification of whether a particular message supports partial replies can be performed using the {@link #isSubscription()}
     * method.
     * </p>
     *
     * @param response Response.
     *
     * @throws UnsupportedOperationException If message doesn't support partial responses (see {@link #isSubscription()}).
     */
    void partialReply(T response) throws UnsupportedOperationException;

    /**
     * Asynchronously sends a partial reply in response to a
     * {@link MessagingChannel#subscribe(Object, ResponseCallback) subscription request}.
     *
     * <p>
     * This method provides support for streaming messages from the recipient back to the sender.
     * For example, the sender can subscribe to a topic using the {@link MessagingChannel#subscribe(Object, ResponseCallback)} method,
     * and then the recipient of such a request can start to continuously stream updates back to the sender. The subscriber will receive
     * these updates via the {@link ResponseCallback#onComplete(Throwable, Response)} method. After the streaming is complete, the final
     * response must be sent using the {@link #reply(Object)} method.
     * </p>
     *
     * <p>
     * <b>Note:</b> This method can only be called for messages that were sent using the
     * {@link MessagingChannel#subscribe(Object, ResponseCallback)} method (or its overloaded versions). If the message was sent by some
     * other method, then all attempts to call this method will throw {@link UnsupportedOperationException}.
     * </p>
     *
     * <p>
     * The verification of whether a particular message supports partial replies can be performed using the {@link #isSubscription()}
     * method.
     * </p>
     *
     * @param response Response.
     * @param callback Callback for tracking completion of an operation.
     *
     * @throws UnsupportedOperationException If message doesn't support partial responses (see {@link #isSubscription()}).
     */
    void partialReply(T response, SendCallback callback) throws UnsupportedOperationException;

    /**
     * Returns {@code true} if this message is a possible duplicate of another message that was received earlier and then was
     * retransmitted by the {@link MessagingChannel#withFailover(FailoverPolicy) failover} logic.
     *
     * @return {@code true} if this message is a possible duplicate of a previously received message.
     */
    boolean isRetransmit();

    /**
     * Returns {@code true} if this message represents a {@link MessagingChannel#subscribe(Object, ResponseCallback) subscription request}
     * and supports {@link #partialReply(Object, SendCallback) partial responses}.
     *
     * @return {@code true} if this message represents a {@link MessagingChannel#subscribe(Object, ResponseCallback) subscription request}.
     */
    boolean isSubscription();

    /**
     * Returns {@code true} if this message represents a request and can be {@link #reply(Object) replied}.
     *
     * @return {@code true} if this message represents a request and can be {@link #reply(Object) replied}.
     */
    boolean isRequest();
}
