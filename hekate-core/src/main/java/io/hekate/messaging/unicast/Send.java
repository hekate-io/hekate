package io.hekate.messaging.unicast;

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.loadbalance.LoadBalancer;

/**
 * Send operation.
 *
 * <p>
 * This interface represents a unidirectional send operation. Typical use of this interface is:
 * </p>
 * <ol>
 * <li>Obtain an instance of this interface via the {@link MessagingChannel#send(Object)} method call</li>
 * <li>Set options (f.e. {@link #withAckMode(SendAckMode) acknowledgement mode}  or {@link #withAffinity(Object) affinity key})</li>
 * <li>Execute this operation via the {@link #execute()} method</li>
 * <li>Await for the execution result, if needed</li>
 * </ol>
 * <h3>Example:</h3>
 * ${source: messaging/MessagingServiceJavadocTest.java#send_operation}
 *
 * @param <T> Message type.
 */
public interface Send<T> {
    /**
     * Affinity key.
     *
     * <p>
     * Specifying an affinity key ensures that all operation with the same key will always be transmitted over the same network
     * connection and will always be processed by the same thread.
     * </p>
     *
     * <p>
     * {@link LoadBalancer} can also make use of the affinity key in order to perform consistent routing of messages among the cluster
     * node. For example, the default load balancer makes sure that all messages, having the same key, are always routed to the same node
     * (unless the cluster topology doesn't change).
     * </p>
     *
     * @param affinity Affinity key.
     *
     * @return This instance.
     */
    Send<T> withAffinity(Object affinity);

    /**
     * Acknowledgement mode.
     *
     * <p>
     * If this option is set to {@link SendAckMode#REQUIRED} then the receiver of this operation will send back an acknowledgement to
     * indicate that this operation was successfully {@link MessageReceiver#receive(Message) received}. In such case the operation's
     * callback/future will be notified only when such acknowledgement is received from all nodes (or if operation fails).
     * </p>
     *
     * <p>
     * If this option is set to {@link SendAckMode#NOT_NEEDED} then operation will be assumed to be successful once the message gets
     * flushed to the network buffer without any additional acknowledgements from receivers.
     * </p>
     *
     * <p>
     * Default value of this option is {@link SendAckMode#NOT_NEEDED}.
     * </p>
     *
     * @param ackMode Acknowledgement mode.
     *
     * @return This instance.
     */
    Send<T> withAckMode(SendAckMode ackMode);

    /**
     * Asynchronously executes this operation.
     *
     * @return Future result of this operation.
     */
    SendFuture execute();

    /**
     * Sets acknowledgement mode to {@link SendAckMode#REQUIRED}.
     *
     * @return This instance.
     *
     * @see #withAckMode(SendAckMode)
     */
    default Send<T> withAck() {
        return withAckMode(SendAckMode.REQUIRED);
    }

    /**
     * Sets acknowledgement mode to {@link SendAckMode#NOT_NEEDED}.
     *
     * @return This instance.
     *
     * @see #withAckMode(SendAckMode)
     */
    default Send<T> withNoAck() {
        return withAckMode(SendAckMode.NOT_NEEDED);
    }

    /**
     * Synchronously executes this operation.
     *
     * @throws MessagingFutureException if operations fails.
     * @throws InterruptedException if thread got interrupted while awaiting for this operation to complete.
     */
    default void sync() throws MessagingFutureException, InterruptedException {
        execute().get();
    }

    /**
     * Asynchronously executes this operation and notifies the specified callback upon completion.
     *
     * @param callback Callback.
     */
    default void async(SendCallback callback) {
        ArgAssert.notNull(callback, "Callback");

        execute().whenComplete((ignore, err) ->
            callback.onComplete(err)
        );
    }
}
