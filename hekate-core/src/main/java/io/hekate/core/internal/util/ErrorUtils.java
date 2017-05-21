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
     * @param error Error (can be {@code null}).
     * @param type Error type to check for.
     *
     * @return {@code true} if the specified exception {@link Class#isAssignableFrom(Class) is} an error of the specified type or is caused
     * by an error of the specified type.
     */
    public static boolean isCausedBy(Throwable error, Class<? extends Throwable> type) {
        return findCause(error, type) != null;
    }

    /**
     * Returns an error instance of the specified type by scanning the {@link Throwable#getCause() cause} tree.
     *
     * @param error Error (can be {@code null}).
     * @param type Error type to search for.
     * @param <T> Error type to search for.
     *
     * @return Error or {@code null} if the specified error is not of the specified type and is not {@link Throwable#getCause() caused} by
     * an error of the specified type.
     */
    public static <T extends Throwable> T findCause(Throwable error, Class<T> type) {
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