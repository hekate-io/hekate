/*
 * Copyright 2022 The Hekate Project
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

import io.hekate.util.format.ToString;

/**
 * Fixed delay policy.
 */
public class FixedBackoffPolicy implements RetryBackoffPolicy {
    /** Default delay (={@value}) in milliseconds (see {@link #defaultPolicy()}). */
    public static final long DEFAULT_DELAY = 100;

    private static final FixedBackoffPolicy DEFAULT_POLICY = new FixedBackoffPolicy(DEFAULT_DELAY);

    /** Delay. */
    private final long delay;

    /**
     * Constructs a new instance with the default delay (={@value #DEFAULT_DELAY}).
     */
    public FixedBackoffPolicy() {
        this(DEFAULT_DELAY);
    }

    /**
     * Constructs a new instance.
     *
     * @param delay Delay.
     */
    public FixedBackoffPolicy(long delay) {
        this.delay = delay;
    }

    /**
     * Returns the default policy with {@value #DEFAULT_DELAY}ms interval.
     *
     * @return Default backoff policy.
     */
    public static FixedBackoffPolicy defaultPolicy() {
        return DEFAULT_POLICY;
    }

    @Override
    public long delayBeforeRetry(int attempt) {
        return delay;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
