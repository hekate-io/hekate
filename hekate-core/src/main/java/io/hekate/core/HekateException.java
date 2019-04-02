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

package io.hekate.core;

import io.hekate.core.internal.util.ErrorUtils;

/**
 * Generic base class for checked errors.
 */
public class HekateException extends Exception {
    private static final long serialVersionUID = 1;

    /**
     * Constructs a new instance.
     *
     * @param message Error message.
     */
    public HekateException(String message) {
        super(message);
    }

    /**
     * Constructs a new instance.
     *
     * @param message Error message.
     * @param cause Cause.
     */
    public HekateException(String message, Throwable cause) {
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
    protected HekateException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    /**
     * Returns {@code true} if this exception is caused by an error of the specified type.
     *
     * @param causeType Error type.
     *
     * @return {@code true} if this exception is caused by an error of the specified type.
     */
    public boolean isCausedBy(Class<? extends Throwable> causeType) {
        return ErrorUtils.isCausedBy(causeType, this);
    }
}
