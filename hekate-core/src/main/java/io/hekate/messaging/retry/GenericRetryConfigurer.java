package io.hekate.messaging.retry;

import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.operation.Aggregate;
import io.hekate.messaging.operation.AggregateRetryConfigurer;
import io.hekate.messaging.operation.Broadcast;
import io.hekate.messaging.operation.BroadcastRetryConfigurer;
import io.hekate.messaging.operation.Request;
import io.hekate.messaging.operation.RequestRetryConfigurer;
import io.hekate.messaging.operation.Send;
import io.hekate.messaging.operation.SendRetryConfigurer;

/**
 * Generic retry policy for a messaging channel.
 *
 * <p>
 * Instances of this interface are responsible for providing a default retry policy for messaging operations of a {@link MessagingChannel}
 * and can be registered via the {@link MessagingChannelConfig#setRetryPolicy(GenericRetryConfigurer)}.
 * </p>
 *
 * <p>
 * This policy can be overridden for each individual operation via the following methods:
 * </p>
 * <ul>
 * <li>{@link Request#withRetry(RequestRetryConfigurer)}</li>
 * <li>{@link Send#withRetry(SendRetryConfigurer)}</li>
 * <li>{@link Broadcast#withRetry(BroadcastRetryConfigurer)}</li>
 * <li>{@link Aggregate#withRetry(AggregateRetryConfigurer)}</li>
 * </ul>
 *
 * @see MessagingChannelConfig#setRetryPolicy(GenericRetryConfigurer)
 */
@FunctionalInterface
public interface GenericRetryConfigurer {
    /**
     * Configures the retry policy.
     *
     * @param retry Retry policy.
     */
    void configure(RetryPolicy<?> retry);

    /**
     * Do not retry.
     *
     * @return Policy that doesn't retry failed operations.
     */
    static GenericRetryConfigurer noRetries() {
        return retry -> {
            // No-op.
        };
    }
}
