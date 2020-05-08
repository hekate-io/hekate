/*
 * Copyright 2020 The Hekate Project
 *
 * The Hekate Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.hekate.rpc;

import io.hekate.core.inject.PlaceholderResolver;
import io.hekate.messaging.retry.GenericRetryConfigurer;
import io.hekate.messaging.retry.RetryPolicy;
import io.hekate.util.format.ToString;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

/**
 * Meta-information about RPC retry setting.
 *
 * @see RpcRetry
 */
public final class RpcRetryInfo implements GenericRetryConfigurer {
    private final OptionalInt maxAttempts;

    private final OptionalLong delay;

    private final OptionalLong maxDelay;

    private final List<Class<? extends Throwable>> errors;

    private RpcRetryInfo(
        OptionalInt maxAttempts,
        OptionalLong delay,
        OptionalLong maxDelay,
        List<Class<? extends Throwable>> errors
    ) {
        this.errors = errors;
        this.maxAttempts = maxAttempts;
        this.delay = delay;
        this.maxDelay = maxDelay;
    }

    /**
     * Parses the specified {@link RpcRetry} annotation.
     *
     * @param retry Annotation.
     * @param resolver String value resolver.
     *
     * @return Meta-information about RPC retry settings.
     */
    public static RpcRetryInfo parse(RpcRetry retry, PlaceholderResolver resolver) {
        OptionalInt maxAttempts = tryParseInt("maxAttempts", retry.maxAttempts(), resolver);
        OptionalLong delay = tryParseLong("delay", retry.delay(), resolver);
        OptionalLong maxDelay = tryParseLong("maxDelay", retry.maxDelay(), resolver);
        List<Class<? extends Throwable>> errors = unmodifiableList(asList(retry.errors()));

        return new RpcRetryInfo(maxAttempts, delay, maxDelay, errors);
    }

    /**
     * Returns the parsed value of {@link RpcRetry#maxAttempts()}.
     *
     * @return Parsed value of {@link RpcRetry#maxAttempts()}.
     */
    public OptionalInt maxAttempts() {
        return maxAttempts;
    }

    /**
     * Returns the parsed value of {@link RpcRetry#delay()}.
     *
     * @return Parsed value of {@link RpcRetry#delay()}.
     */
    public OptionalLong delay() {
        return delay;
    }

    /**
     * Returns the parsed value of {@link RpcRetry#maxDelay()}.
     *
     * @return Parsed value of {@link RpcRetry#maxDelay()}.
     */
    public OptionalLong maxDelay() {
        return maxDelay;
    }

    /**
     * Returns the parsed value of {@link RpcRetry#errors()}.
     *
     * @return Parsed value of {@link RpcRetry#errors()}.
     */
    public List<Class<? extends Throwable>> errors() {
        return errors;
    }

    @Override
    public void configure(RetryPolicy<?> retry) {
        if (!errors.isEmpty()) {
            retry.whileError(err -> errors.stream().anyMatch(err::isCausedBy));
        }

        if (maxAttempts.isPresent()) {
            retry.maxAttempts(maxAttempts.getAsInt());
        }

        if (delay.isPresent()) {
            if (maxDelay.isPresent()) {
                retry.withExponentialDelay(delay.getAsLong(), maxDelay.getAsLong());
            } else {
                retry.withFixedDelay(delay.getAsLong());
            }
        }
    }

    private static OptionalInt tryParseInt(String field, String value, PlaceholderResolver resolver) {
        value = resolver.resolvePlaceholders(value).trim();

        if (value.isEmpty()) {
            return OptionalInt.empty();
        } else {
            try {
                return OptionalInt.of(Integer.parseInt(value));
            } catch (NumberFormatException e) {
                throw invalidValue(field, value, e);
            }
        }
    }

    private static OptionalLong tryParseLong(String field, String value, PlaceholderResolver resolver) {
        value = resolver.resolvePlaceholders(value).trim();

        if (value.isEmpty()) {
            return OptionalLong.empty();
        } else {
            try {
                return OptionalLong.of(Long.parseLong(value));
            } catch (NumberFormatException e) {
                throw invalidValue(field, value, e);
            }
        }
    }

    private static IllegalArgumentException invalidValue(String field, String value, NumberFormatException e) {
        String msg = "Failed to parse the value of @" + RpcRetry.class.getSimpleName() + "#" + field + "() attribute "
            + "[value=" + value + ']';

        return new IllegalArgumentException(msg, e);
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
