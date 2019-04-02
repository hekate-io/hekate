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

package io.hekate.messaging;

/**
 * Policy that defines how {@link MessagingChannel} should behave in case of its send queue overflow.
 *
 * @see MessagingBackPressureConfig#setOutOverflowPolicy(MessagingOverflowPolicy)
 * @see MessagingBackPressureConfig#setOutLowWatermark(int)
 * @see MessagingBackPressureConfig#setOutHighWatermark(int)
 */
public enum MessagingOverflowPolicy {
    /**
     * Block the caller thread unless the {@link MessagingChannel}'s outbound queue goes down to its
     * {@link MessagingBackPressureConfig#setOutLowWatermark(int) low watermark} or unless the caller thread gets interrupted. If the caller
     * thread gets interrupted then the messaging operation will fail with {@link InterruptedException}. Note that the caller thread's
     * {@link Thread#isInterrupted() interrupted} flag will not be reset (i.e. {@link Thread#isInterrupted()} will return {@code false}).
     */
    BLOCK,

    /**
     * Blocks the caller thread unless the {@link MessagingChannel}'s outbound queue goes down to its
     * {@link MessagingBackPressureConfig#setOutLowWatermark(int) low watermark}. If the caller thread gets interrupted then such
     * interruption will be ignored.
     */
    BLOCK_UNINTERRUPTEDLY,

    /**
     * Rejects operation with {@link MessageQueueOverflowException} error.
     */
    FAIL,

    /**
     * Completely ignore queue size restrictions.
     */
    IGNORE
}
