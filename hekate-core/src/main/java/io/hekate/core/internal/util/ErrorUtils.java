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

package io.hekate.core.internal.util;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Utilities for working with {@link Exception}s.
 */
public final class ErrorUtils {
    private ErrorUtils() {
        // No-op.
    }

    /**
     * Returns {@link Throwable#getStackTrace() stackt race} as string.
     *
     * @param error Error.
     *
     * @return Stack trace as a string.
     */
    public static String stackTrace(Throwable error) {
        ArgAssert.notNull(error, "error");

        StringWriter out = new StringWriter();

        error.printStackTrace(new PrintWriter(out));

        return out.toString();
    }

    /**
     * Returns {@code true} if the specified exception {@link Class#isAssignableFrom(Class) is} an error of the specified type or is caused
     * by an error of the specified type.
     *
     * <p>
     * This method recursively scans the whole {@link Throwable#getCause() cause} tree unless an error the specified type is found the
     * bottom of the tree is reached.
     * </p>
     *
     * @param type Error type to check for.
     * @param error Error (can be {@code null}).
     *
     * @return {@code true} if the specified exception {@link Class#isAssignableFrom(Class) is} an error of the specified type or is caused
     * by an error of the specified type.
     */
    public static boolean isCausedBy(Class<? extends Throwable> type, Throwable error) {
        return findCause(type, error) != null;
    }

    /**
     * Returns an error instance of the specified type by scanning the {@link Throwable#getCause() cause} tree.
     *
     * @param <T> Error type to search for.
     * @param type Error type to search for.
     * @param error Error (can be {@code null}).
     *
     * @return Error or {@code null} if the specified error is not of the specified type and is not {@link Throwable#getCause() caused} by
     * an error of the specified type.
     */
    public static <T extends Throwable> T findCause(Class<T> type, Throwable error) {
        ArgAssert.notNull(type, "error type");

        Throwable cause = error;

        while (cause != null) {
            if (type.isAssignableFrom(cause.getClass())) {
                return type.cast(cause);
            }

            cause = cause.getCause();
        }

        return null;
    }
}
