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

package io.hekate.messaging;

import io.hekate.messaging.unicast.Reply;
import io.hekate.messaging.unicast.RequestCallback;
import io.hekate.messaging.unicast.SendCallback;

/**
 * Message with arbitrary payload.
 *
 * <p>
 * This interface represents a message that was received from a {@link MessagingChannel} and provides methods for sending responses in
 * order to support request-response (or even more complex) communication pattern.
 * </p>
 *
 * <p>
 * Please see the documentation of {@link MessagingService} for more information about messaging and supported communication patterns.
 * </p>
 *
 * @param <T> Base type of message's payload.
 *
 * @see MessageReceiver
 */
public interface Message<T> {
    /**
     * Returns the payload of this message.
     *
     * @return Payload.
     */
    T get();

    /**
     * Casts the payload of this message ot the specified type.
     *
     * @param type Payload type.
     * @param <P> Payload type.
     *
     * @return Payload.
     *
     * @throws ClassCastException If payload can't be cast to the specified type.
     */
    <P extends T> P get(Class<P> type);

    /**
     * Returns {@code true} if this message has a {@link #get() payload} of the specified type.
     *
     * @param type Payload type.
     *
     * @return {@code true} if this message has a {@link #get() payload} of the specified type.
     */
    boolean is(Class<? extends T> type);

    /**
     * Returns {@code true} if the sending side expects a response message to be send back.
     *
     * <p>
     * Responses are required if the sending side used {@link MessagingChannel#request(Object) request(...)} or {@link
     * MessagingChannel#aggregate(Object) aggregate(...)} methods for the message submission. Responses can be sent back via the following
     * methods:
     * </p>
     * <ul>
     * <li>{@link #reply(Object)}</li>
     * <li>{@link #reply(Object, SendCallback)} </li>
     * <li>{@link #replyWithRequest(Object, RequestCallback)}</li>
     * </ul>
     *
     * <p>
     * <b>Note:</b> Not sending a response when this method returns {@code true} can lead to resource leaks since the sending side
     * keeps track of messages in its in-memory queues and releases resources only when responses are received.
     * </p>
     *
     * @return {@code true} if sending side expects a response message to be send back.
     *
     * @see #reply(Object, SendCallback)
     * @see #replyWithRequest(Object, RequestCallback)
     */
    boolean mustReply();

    /**
     * Asynchronously send back a reply and ignores the operation result.
     *
     *
     * <p>
     * Calling this method will put the message into the 'replied' state and its {@link #mustReply()} method will start returning {@code
     * false}.
     * </p>
     *
     * <p>
     * Calling this method on a message that doesn't support responses (i.e. {@link #mustReply()} returns {@code false}) will result in an
     * {@link UnsupportedOperationException}.
     * </p>
     *
     * @param response Response.
     *
     * @throws UnsupportedOperationException If message doesn't support responses (see {@link #mustReply()}).
     * @see #reply(Object, SendCallback)
     */
    void reply(T response) throws UnsupportedOperationException;

    /**
     * Asynchronously send back a reply and uses the specified callback to notify on operation result.
     *
     * <p>
     * Calling this method will put the message into the 'replied' state and its {@link #mustReply()} method will start returning {@code
     * false}.
     * </p>
     *
     * <p>
     * Calling this method on a message that doesn't support responses (i.e. {@link #mustReply()} returns {@code false}) will result in
     * {@link UnsupportedOperationException}.
     * </p>
     *
     * @param response Response.
     * @param callback Callback for tracking the operation completion results.
     *
     * @throws UnsupportedOperationException If message doesn't support responses (see {@link #mustReply()}).
     */
    void reply(T response, SendCallback callback) throws UnsupportedOperationException;

    /**
     * Asynchronously sends a request back to the sender of this message and notifies the provided callback on results.
     *
     * <p>
     * This method provides support for implementing complex conversation patterns that go beyond the simple request-response approach
     * and require multi-step messages exchange between the messaging peers.
     * </p>
     *
     * <p>
     * Calling this method will put the message into the 'replied' state and its {@link #mustReply()} method will start returning {@code
     * false}.
     * </p>
     *
     * <p>
     * Calling this method on a message that doesn't support responses (i.e. {@link #mustReply()} returns {@code false}) will result in
     * {@link UnsupportedOperationException}.
     * </p>
     *
     * @param request Response.
     * @param callback Callback for response listening.
     *
     * @throws UnsupportedOperationException If message doesn't support responses (see {@link #mustReply()}).
     */
    void replyWithRequest(T request, RequestCallback<T> callback) throws UnsupportedOperationException;

    /**
     * Asynchronously sends back a partial reply.
     *
     * <p>
     * This method provides support for subscription-based messages streaming from receiver back to sender. For example, sender can
     * subscribes to some topic via {@link MessagingChannel#request(Object, RequestCallback)} method and receiver of such request starts
     * continuously pushing updates back the sender. Sender will receive those updates via {@link RequestCallback#onComplete(Throwable,
     * Reply)}. Once messages streaming is finished then final response should be sent via {@link #reply(Object)} or
     * {@link #replyWithRequest(Object, RequestCallback)} methods.
     * </p>
     *
     * <p>
     * Calling this method on a message that doesn't support responses (i.e. {@link #mustReply()} returns {@code false}) will result in
     * {@link UnsupportedOperationException}.
     * </p>
     *
     * @param response Response.
     *
     * @throws UnsupportedOperationException If message doesn't support responses (see {@link #mustReply()}).
     * @see Reply#isPartial()
     */
    void replyPartial(T response) throws UnsupportedOperationException;

    /**
     * Asynchronously sends back the partial reply.
     *
     * <p>
     * This method provides support for subscription-based messages streaming from receiver back to sender. For example, sender can
     * subscribes to some topic via {@link MessagingChannel#request(Object, RequestCallback)} method and receiver of such request starts
     * continuously pushing updates back the sender. Sender will receive those updates via {@link RequestCallback#onComplete(Throwable,
     * Reply)}. Once messages streaming is finished then final response should be sent via {@link #reply(Object)} or
     * {@link #replyWithRequest(Object, RequestCallback)} methods.
     * </p>
     *
     * <p>
     * Calling this method on a message that doesn't support responses (i.e. {@link #mustReply()} returns {@code false}) will result in
     * {@link UnsupportedOperationException}.
     * </p>
     *
     * @param response Response.
     * @param callback Callback for tracking the operation completion results.
     *
     * @throws UnsupportedOperationException If message doesn't support responses (see {@link #mustReply()}).
     * @see Reply#isPartial()
     */
    void replyPartial(T response, SendCallback callback) throws UnsupportedOperationException;

    /**
     * Returns the messaging endpoint of this message..
     *
     * @return Messaging endpoint.
     */
    MessagingEndpoint<T> getEndpoint();

    /**
     * Returns the messaging channel of this message.
     *
     * @return Messaging channel.
     */
    MessagingChannel<T> getChannel();
}
