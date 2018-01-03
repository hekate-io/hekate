/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.failover;

import io.hekate.util.format.ToString;

/**
 * Implementation of {@link FailoverCondition} that performs failover actions with a limited number of attempts.
 *
 * <p>
 * Maximum number of attempts can be specified via {@link #MaxFailoverAttempts(int)} constructor. For every failover attempt it will be
 * compared with the current attempt (see {@link FailureInfo#attempt()}) and will succeed only if the current value is less than the
 * maximum number of attempts.
 * </p>
 *
 * @see FailoverCondition
 */
public class MaxFailoverAttempts implements FailoverCondition {
    private final int maxAttempts;

    /**
     * Constructs new instance.
     *
     * @param maxAttempts Maximum number of failover attempts.
     */
    public MaxFailoverAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    @Override
    public boolean test(FailureInfo failover) {
        return failover.attempt() < maxAttempts;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
