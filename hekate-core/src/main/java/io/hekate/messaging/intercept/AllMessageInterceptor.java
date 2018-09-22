package io.hekate.messaging.intercept;

/**
 * Union of {@link ClientMessageInterceptor} and {@link ServerMessageInterceptor}.
 *
 * <p>
 * Please see the documentation of {@link MessageInterceptor} for details of message interception logic.
 * </p>
 *
 * @param <T> Base type fo messages that can be handled by this interceptor.
 */
public interface AllMessageInterceptor<T> extends ClientMessageInterceptor<T>, ServerMessageInterceptor<T> {
    // No-op.
}
