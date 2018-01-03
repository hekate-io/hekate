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
 * Implementation of {@link FailoverDelaySupplier} that always returns the same value.
 *
 * @see FailoverDelaySupplier
 */
public class ConstantFailoverDelay implements FailoverDelaySupplier {
    private final long delay;

    /**
     * Constructs new instance.
     *
     * @param delay Value that should be returned by {@link #delayOf(FailureInfo)} method.
     */
    public ConstantFailoverDelay(long delay) {
        this.delay = delay;
    }

    @Override
    public long delayOf(FailureInfo failure) {
        return delay;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
