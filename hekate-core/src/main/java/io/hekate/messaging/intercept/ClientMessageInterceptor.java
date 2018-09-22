package io.hekate.messaging.intercept;

import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.loadbalance.LoadBalancer;

/**
 * Client-side message interceptor.
 *
 * <p>
 * Please see the documentation of {@link MessageInterceptor} for details of message interception logic.
 * </p>
 *
 * @param <T> Base type fo messages that can be handled by this interceptor.
 */
public interface ClientMessageInterceptor<T> extends MessageInterceptor<T> {
    /**
     * Intercepts a message right before sending it to a server.
     *
     * <p>
     * This method gets called by the {@link MessagingChannel} after the target server had been selected by the {@link LoadBalancer} for the
     * specified outbound message. Implementations of this method can decide to transform the message into another message based on some
     * criteria of the {@link ClientSendContext}.
     * </p>
     *
     * @param msg Message.
     * @param sndCtx Context.
     *
     * @return Transformed message or the original message if transformation is not required. Returning {@code null} will be interpreted as
     * if no transformation had been applied and the original message should be send to the target node.
     *
     * @see #beforeClientReceiveResponse(Object, ClientReceiveContext, ClientSendContext)
     */
    default T beforeClientSend(T msg, ClientSendContext sndCtx) {
        return null;
    }

    /**
     * Intercepts a response from a server.
     *
     * <p>
     * This method gets called by the {@link MessagingChannel} when it receives a response from a server for a message of
     * {@link OutboundType#REQUEST} or {@link OutboundType#SUBSCRIBE} type. Implementations of this method can decide to transform the
     * message into another message based on some criteria of the {@link ClientSendContext}.
     * </p>
     *
     * @param rsp Response.
     * @param rcvCtx Response context.
     * @param sndCtx Context of a sent message (same object as in {@link #beforeClientSend(Object, ClientSendContext)}).
     *
     * @return Transformed message or the original message if transformation is not required. Returning {@code null} will be interpreted as
     * if no transformation had been applied and the original message should be send to the target node.
     *
     * @see #beforeClientSend(Object, ClientSendContext)
     */
    default T beforeClientReceiveResponse(T rsp, ClientReceiveContext rcvCtx, ClientSendContext sndCtx) {
        return null;
    }

    /**
     * Notifies on an acknowledgement from a server.
     *
     * <p>
     * This method gets called by the {@link MessagingChannel} when it receives an acknowledgement from a server for a message of
     * {@link OutboundType#SEND_WITH_ACK} type.
     * </p>
     *
     * @param sndCtx Context of a sent message (same object as in {@link #beforeClientSend(Object, ClientSendContext)}).
     *
     * @see MessagingChannel#withConfirmReceive(boolean)
     */
    default void onClientReceiveConfirmation(ClientSendContext sndCtx) {
        // No-op.
    }

    /**
     * Notifies on a failure to receive a response from server.
     *
     * @param err Error
     * @param sndCtx Context of a message (same object as in {@link #beforeClientSend(Object, ClientSendContext)}).
     */
    default void onClientReceiveError(Throwable err, ClientSendContext sndCtx) {
        // No-op.
    }
}
