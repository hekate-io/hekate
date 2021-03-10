/*
 * Copyright 2021 The Hekate Project
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

package io.hekate.messaging.retry;

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ErrorUtils;
import java.util.function.Predicate;

/**
 * Failed attempt.
 */
public interface FailedAttempt extends Attempt {
    /**
     * Returns the routing policy.
     *
     * @return Routing policy.
     */
    RetryRoutingPolicy routing();

    /**
     * Returns the cause of ths failure.
     *
     * @return Cause of this failure.
     */
    Throwable error();

    /**
     * Returns {@code true} if this failure is caused by an error of the specified type.
     *
     * @param type Error type.
     *
     * @return {@code true} if this failure is caused by an error of the specified type.
     */
    default boolean isCausedBy(Class<? extends Throwable> type) {
        ArgAssert.notNull(type, "Error type");

        return ErrorUtils.isCausedBy(type, error());
    }

    /**
     * Returns {@code true} if this failure is caused by an error of the specified type.
     *
     * @param type Error type.
     * @param predicate Error predicate.
     * @param <T> Error type.
     *
     * @return {@code true} if this failure is caused by an error of the specified type.
     */
    default <T extends Throwable> boolean isCausedBy(Class<T> type, Predicate<T> predicate) {
        ArgAssert.notNull(type, "Error type");
        ArgAssert.notNull(predicate, "Error predicate");

        T cause = ErrorUtils.findCause(type, error());

        return cause != null && predicate.test(cause);
    }
}
