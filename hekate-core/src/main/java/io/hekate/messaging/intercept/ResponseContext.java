package io.hekate.messaging.intercept;

/**
 * Response context.
 *
 * @param <T> Message type.
 */
public interface ResponseContext<T> {
    /**
     * Returns the original (untransformed) message.
     *
     * @return Message.
     */
    T message();

    /**
     * Type of this response.
     *
     * @return Type of this response.
     */
    ResponseType type();
}
