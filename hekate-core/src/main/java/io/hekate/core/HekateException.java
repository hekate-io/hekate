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

package io.hekate.core;

import io.hekate.core.internal.util.ErrorUtils;
import java.util.concurrent.Future;

/**
 * Generic base class for Hekate errors.
 */
public class HekateException extends RuntimeException {
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
     * Forks this exception by creating a new exception that has this instance as its cause.
     *
     * <p>
     * The primary use case of this method is to re-throw exceptions from asynchronous contexts
     * (f.e. exceptions thrown by {@link Future#get()} method).
     * </p>
     *
     * @return Fork of this exception.
     */
    public HekateException forkFromAsync() {
        return new HekateException(getMessage(), this);
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

    /**
     * Searches for an error cause of the specified type. Returns {@code null} if there is no such error.
     *
     * @param type Error type.
     * @param <T> Error type.
     *
     * @return Error or {@code null}.
     */
    public <T extends Throwable> T findCause(Class<T> type) {
        return ErrorUtils.findCause(type, this);
    }
}
