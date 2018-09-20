package io.hekate.messaging.intercept;

/**
 * Response context.
 */
public interface ResponseContext {
    /**
     * Type of this response.
     *
     * @return Type of this response.
     */
    InboundType type();
}
