package io.hekate.messaging.intercept;

import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessagingChannel;

/**
 * Server-side message interceptor.
 *
 * <p>
 * Please see the documentation of {@link MessageInterceptor} for details of message interception logic.
 * </p>
 *
 * @param <T> Base type fo messages that can be handled by this interceptor.
 */
public interface ServerMessageInterceptor<T> extends MessageInterceptor<T> {
    /**
     * Intercepts an incoming message from a remote client.
     *
     * <p>
     * This method gets called by the {@link MessagingChannel} when it receives a message from a client right before invoking
     * the {@link MessageReceiver}. Implementations of this method can decide to transform the message into another message based on some
     * criteria of the {@link ServerReceiveContext}.
     * </p>
     *
     * @param msg Message.
     * @param rcvCtx Context.
     *
     * @return Transformed message or the original message if transformation is not required. Returning {@code null} will be interpreted as
     * if no transformation had been applied and the original message should be send to the target node.
     */
    default T beforeServerReceive(T msg, ServerReceiveContext rcvCtx) {
        return null;
    }

    /**
     * Notifies on server completes processing of a message.
     *
     * <p>
     * This method gets called right after the server's {@link MessageReceiver} completes processing of a message.
     * </p>
     *
     * @param msg Message.
     * @param rcvCtx Context of a request message (same object as in {@link #beforeServerReceive(Object, ServerReceiveContext)}).
     */
    default void onServerReceiveComplete(T msg, ServerReceiveContext rcvCtx) {
        // No-op.
    }

    /**
     * Intercepts a response right before sending it back to a client.
     *
     * <p>
     * This method gets called by the {@link MessagingChannel} right before sending a response to a client. Implementations of this
     * method can decide to transform the message into another message based on some criteria of the {@link ServerReceiveContext}.
     * </p>
     *
     * @param rsp Response.
     * @param sndCtx Response context.
     * @param rcvCtx Context of a request message (same object as in {@link #beforeServerReceive(Object, ServerReceiveContext)}).
     *
     * @return Transformed message or the original message if transformation is not required. Returning {@code null} will be interpreted as
     * if no transformation had been applied and the original message should be send to the target node.
     */
    default T beforeServerSend(T rsp, ServerSendContext sndCtx, ServerReceiveContext rcvCtx) {
        return null;
    }
}
