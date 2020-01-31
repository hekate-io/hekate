package io.hekate.messaging.operation;

/**
 * Condition to repeat an {@link Broadcast} operation.
 *
 * @param <T> Message type.
 *
 * @see Broadcast#withRepeat(BroadcastRepeatCondition)
 */
@FunctionalInterface
public interface BroadcastRepeatCondition<T> {
    /**
     * Returns {@code true} if the whole {@link Broadcast} operation should be repeated.
     *
     * @param result Current aggregation result.
     *
     * @return {@code true} if the whole {@link Broadcast} operation should be repeated.
     */
    boolean shouldRepeat(BroadcastResult<T> result);
}
