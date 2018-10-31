package io.hekate.messaging.broadcast;

import io.hekate.messaging.Message;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingFutureException;

/**
 * Broadcast operation.
 *
 * <p>
 * This interface represents a unidirectional broadcast operation. Typical use of this interface is:
 * </p>
 * <ol>
 * <li>Obtain an instance of this interface via the {@link MessagingChannel#broadcast(Object)} method call</li>
 * <li>Set options (f.e. {@link #withConfirmReceive(boolean) confirmation mode} or {@link #withAffinity(Object) affinity key})</li>
 * <li>Execute this operation via the {@link #execute()} method</li>
 * <li>Await for the execution result, if needed</li>
 * </ol>
 * <h3>Example:</h3>
 * ${source: messaging/MessagingServiceJavadocTest.java#broadcast_operation}
 *
 * @param <T> Message type.
 */
public interface Broadcast<T> {
    /**
     * Affinity key.
     *
     * <p>
     * Specifying an affinity key ensures that all operation with the same key will always be transmitted over the same network
     * connection and will always be processed by the same thread.
     * </p>
     *
     * @param affinity Affinity key.
     *
     * @return This instance.
     */
    Broadcast<T> withAffinity(Object affinity);

    /**
     * Confirmation mode.
     *
     * <p>
     * If this option is set to {@code true} then the receiver of this operation will send back a confirmation to indicate that this
     * operation was successfully {@link MessageReceiver#receive(Message) received}. In such case the operation's callback/future will be
     * notified only when such confirmation is received from all nodes (or if operation fails).
     * </p>
     *
     * <p>
     * If this option is set to {@code false} then operation will be assumed to be successful once the message gets flushed to the network
     * buffer without any additional confirmations from receivers.
     * </p>
     *
     * <p>
     * Default value of this option is {@code false} (i.e. confirmations are disabled by default).
     * </p>
     *
     * @param confirmReceive Confirmation mode.
     *
     * @return This instance.
     */
    Broadcast<T> withConfirmReceive(boolean confirmReceive);

    /**
     * Asynchronously executes this operation.
     *
     * @return Future result of this operation.
     */
    BroadcastFuture<T> execute();

    /**
     * Synchronously executes this operation and returns the result.
     *
     * @return Result.
     *
     * @throws MessagingFutureException If operations fails.
     * @throws InterruptedException If thread got interrupted while awaiting for this operation to complete.
     */
    default BroadcastResult<T> sync() throws MessagingFutureException, InterruptedException {
        return execute().get();
    }

    /**
     * Asynchronously executes this operation and notifies the specified callback upon completion.
     *
     * @param callback Callback.
     */
    default void async(BroadcastCallback<T> callback) {
        execute().whenComplete((result, error) ->
            callback.onComplete(error, result)
        );
    }
}
