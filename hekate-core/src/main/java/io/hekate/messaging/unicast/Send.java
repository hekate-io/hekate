package io.hekate.messaging.unicast;

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.loadbalance.LoadBalancer;

/**
 * Send operation.
 *
 * <p>
 * This interface represents a unidirectional send operation. Typical use of this interface is:
 * </p>
 * <ol>
 * <li>Obtain an instance of this interface via the {@link MessagingChannel#newSend(Object)} method call</li>
 * <li>Set options (f.e. {@link #withConfirmReceive(boolean) confirmation mode} or {@link #withAffinity(Object) affinity key})</li>
 * <li>Execute this operation via the {@link #submit()} method</li>
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
     * Confirmation mode.
     *
     * <p>
     * If this option is set to {@code true} then the receiver of this operation will send back a confirmation to indicate that this
     * operation was successfully {@link MessageReceiver#receive(Message) received}. In such case the operation's callback/future will be
     * notified only when such confirmation is received (or if operation fails).
     * </p>
     *
     * <p>
     * If this option is set to {@code false} then operation will be assumed to be successful once the message gets flushed to the network
     * buffer without any additional confirmations from the receiver side.
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
    Send<T> withConfirmReceive(boolean confirmReceive);

    /**
     * Asynchronously executes this operation.
     *
     * @return Future result of this operation.
     */
    SendFuture submit();

    /**
     * Asynchronously executes this operation and notifies the specified callback upon completion.
     *
     * @param callback Callback.
     *
     * @return Future result of this operation.
     */
    default SendFuture submit(SendCallback callback) {
        ArgAssert.notNull(callback, "Callback");

        SendFuture future = submit();

        future.whenComplete((ignore, err) ->
            callback.onComplete(err)
        );

        return future;
    }
}
