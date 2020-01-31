package io.hekate.messaging.operation;

/**
 * Condition to repeat an {@link Aggregate} operation.
 *
 * @param <T> Aggregation result type.
 *
 * @see Aggregate#withRepeat(AggregateRepeatCondition)
 */
@FunctionalInterface
public interface AggregateRepeatCondition<T> {
    /**
     * Returns {@code true} if the whole {@link Aggregate} operation should be repeated.
     *
     * @param result Current aggregation result.
     *
     * @return {@code true} if the whole {@link Aggregate} operation should be repeated.
     */
    boolean shouldRepeat(AggregateResult<T> result);
}
