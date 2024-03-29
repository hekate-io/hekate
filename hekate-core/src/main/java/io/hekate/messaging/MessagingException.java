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

package io.hekate.messaging;

import io.hekate.core.HekateException;

/**
 * Failure of a messaging operation.
 *
 * @see MessagingService
 */
public class MessagingException extends HekateException {
    private static final long serialVersionUID = 1;

    /**
     * Constructs new instance.
     *
     * @param message Error message.
     */
    public MessagingException(String message) {
        super(message);
    }

    /**
     * Constructs new instance.
     *
     * @param message Error message.
     * @param cause Error cause.
     */
    public MessagingException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new instance.
     *
     * @param message Error message.
     * @param cause Cause.
     * @param enableSuppression Enabled/disabled suppression.
     * @param writableStackTrace Whether or not the stack trace should be writable.
     */
    protected MessagingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    @Override
    public HekateException forkFromAsync() {
        return new MessagingException(getMessage(), this);
    }
}
