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
 * Signals {@link MessagingChannel}'s send queue timeout.
 *
 * <p>
 * This error is thrown by a {@link MessagingChannel} when its send queue is full (above the {@link
 * MessagingBackPressureConfig#setOutHighWatermark(int) high watermark}) and messaging operation
 * {@link MessagingChannelConfig#withMessagingTimeout(long)} timed out}.
 * </p>
 *
 * @see MessagingBackPressureConfig
 * @see MessagingOverflowPolicy#BLOCK
 */
public class MessageQueueTimeoutException extends MessageTimeoutException {
    private static final long serialVersionUID = 1;

    /**
     * Constructs a new instance with the specified error message.
     *
     * @param message The error message.
     */
    public MessageQueueTimeoutException(String message) {
        super(message);
    }
}
