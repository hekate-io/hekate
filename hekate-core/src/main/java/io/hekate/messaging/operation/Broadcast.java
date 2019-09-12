package io.hekate.messaging.operation;

import io.hekate.messaging.Message;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessageTimeoutException;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.retry.GenericRetryConfigurer;
import java.util.concurrent.TimeUnit;

/**
 * <b>Broadcast operation.</b>
 *
 * <p>
 * This interface represents a one-way message broadcast operation of a {@link MessagingChannel}.
 * This operation submits the same message to multiple remote nodes.
 * </p>
 *
 * <h2>Acknowledgements</h2>
 * <p>
 * Messages can be broadcast with or without acknowledgements:
 * </p>
 * <ul>
 * <li>If acknowledgements are enabled then this operation will be completed only when acknowledgements are received from all receiver</li>
 * <li>If acknowledgements are disabled then this operation will be completed immediately once it is flushed to the network buffer</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <p>
 * Typical use of this interface is:
 * </p>
 * <ol>
 * <li>Obtain an instance of this interface via the {@link MessagingChannel#newBroadcast(Object)} method call</li>
 * <li>Set options (if needed):
 * <ul>
 * <li>{@link #withAckMode(AckMode) Acknowledgememt Mode}</li>
 * <li>{@link #withTimeout(long, TimeUnit) Operation Timeout}</li>
 * <li>{@link #withAffinity(Object) Affinity Key}</li>
 * <li>{@link #withRetry(BroadcastRetryConfigurer) Retry Policy}</li>
 * </ul>
 * </li>
 * <li>Execute this operation via the {@link #submit()} method</li>
 * <li>Await for the execution result, if needed</li>
 * </ol>
 *
 * <p>
 * ${source: messaging/MessagingServiceJavadocTest.java#broadcast_operation}
 * </p>
 *
 * <h2>Shortcut Methods</h2>
 * <p>
 * {@link MessagingChannel} interface provides a set of synchronous and asynchronous shortcut methods for common use cases:
 * </p>
 * <ul>
 * <li>{@link MessagingChannel#broadcast(Object)}</li>
 * <li>{@link MessagingChannel#broadcast(Object, AckMode)}</li>
 * <li>{@link MessagingChannel#broadcast(Object, Object)}</li>
 * <li>{@link MessagingChannel#broadcast(Object, Object, AckMode)}</li>
 * <li>{@link MessagingChannel#broadcastAsync(Object)}</li>
 * <li>{@link MessagingChannel#broadcastAsync(Object, AckMode)}</li>
 * <li>{@link MessagingChannel#broadcastAsync(Object, Object)}</li>
 * <li>{@link MessagingChannel#broadcastAsync(Object, Object, AckMode)}</li>
 * </ul>
 *
 * @param <T> Message type.
 */
public interface Broadcast<T> {
    /**
     * Affinity key.
     *
     * <p>
     * Specifying an affinity key ensures that all operation with the same key will always be transmitted over the same network
     * connection and will always be processed by the same thread (if the cluster topology doesn't change).
     * </p>
     *
     * @param affinity Affinity key.
     *
     * @return This instance.
     */
    Broadcast<T> withAffinity(Object affinity);

    /**
     * Overrides the channel's default timeout value for this operation.
     *
     * <p>
     * If this operation can not complete at the specified timeout then this operation will end up in a {@link MessageTimeoutException}.
     * </p>
     *
     * <p>
     * Specifying a negative or zero value disables the timeout check.
     * </p>
     *
     * @param timeout Timeout.
     * @param unit Unit.
     *
     * @return This instance.
     *
     * @see MessagingChannelConfig#setMessagingTimeout(long)
     */
    Broadcast<T> withTimeout(long timeout, TimeUnit unit);

    /**
     * Acknowledgement mode.
     *
     * <p>
     * If this option is set to {@link AckMode#REQUIRED} then the receiver of this operation will send back an acknowledgement to
     * indicate that this operation was successfully {@link MessageReceiver#receive(Message) received}. In such case the operation's
     * callback/future will be notified only when such acknowledgement is received from all nodes (or if operation fails).
     * </p>
     *
     * <p>
     * If this option is set to {@link AckMode#NOT_NEEDED} then operation will be assumed to be successful once the message gets
     * flushed to the network buffer without any additional acknowledgements from receivers.
     * </p>
     *
     * <p>
     * Default value of this option is {@link AckMode#NOT_NEEDED}.
     * </p>
     *
     * @param ackMode Acknowledgement mode.
     *
     * @return This instance.
     */
    Broadcast<T> withAckMode(AckMode ackMode);

    /**
     * Retry policy.
     *
     * @param retry Retry policy.
     *
     * @return This instance.
     *
     * @see MessagingChannelConfig#setRetryPolicy(GenericRetryConfigurer)
     */
    Broadcast<T> withRetry(BroadcastRetryConfigurer retry);

    /**
     * Asynchronously executes this operation.
     *
     * @return Future result of this operation.
     */
    BroadcastFuture<T> submit();

    /**
     * Asynchronously executes this operation and notifies the specified callback upon completion.
     *
     * @param callback Callback.
     */
    default void submit(BroadcastCallback<T> callback) {
        submit().whenComplete((result, error) ->
            callback.onComplete(error, result)
        );
    }

    /**
     * Synchronously executes this operation and returns the result.
     *
     * @return Result.
     *
     * @throws MessagingFutureException If operations fails.
     * @throws InterruptedException If thread got interrupted while awaiting for this operation to complete.
     */
    default BroadcastResult<T> sync() throws MessagingFutureException, InterruptedException {
        return submit().get();
    }

    /**
     * Sets acknowledgement mode to {@link AckMode#REQUIRED}.
     *
     * @return This instance.
     *
     * @see #withAckMode(AckMode)
     */
    default Broadcast<T> withAck() {
        return withAckMode(AckMode.REQUIRED);
    }

    /**
     * Sets acknowledgement mode to {@link AckMode#NOT_NEEDED}.
     *
     * @return This instance.
     *
     * @see #withAckMode(AckMode)
     */
    default Broadcast<T> withNoAck() {
        return withAckMode(AckMode.NOT_NEEDED);
    }
}
