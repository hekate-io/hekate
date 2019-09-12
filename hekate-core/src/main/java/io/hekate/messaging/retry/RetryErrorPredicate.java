/*
 * Copyright 2019 The Hekate Project
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

/**
 * Predicate to test if particular error should be retried.
 */
@FunctionalInterface
public interface RetryErrorPredicate {
    /**
     * Decides on whether the specified error should be retried.
     *
     * @param err Failure.
     *
     * @return {@code true} if operation should be retried.
     */
    boolean shouldRetry(FailedAttempt err);

    /**
     * Never retry.
     *
     * @return Policy that rejects all errors.
     */
    static RetryErrorPredicate rejectAll() {
        return err -> false;
    }

    /**
     * Always retry.
     *
     * @return Policy that always retries.
     */
    static RetryErrorPredicate acceptAll() {
        return err -> true;
    }
}
