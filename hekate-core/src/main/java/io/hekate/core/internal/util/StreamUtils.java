package io.hekate.core.internal.util;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Utilities for working with {@link Stream}s.
 */
public final class StreamUtils {
    private StreamUtils() {
        // No-op.
    }

    /**
     * Returns a {@link Stream} that filters out all {@code null} values.
     *
     * @param collection Collection (can be {@code null}, in such {@link Stream#empty()} will be returned).
     * @param <T> Stream type.
     *
     * @return Null-safe stream.
     */
    public static <T> Stream<T> nullSafe(Collection<T> collection) {
        if (collection == null || collection.isEmpty()) {
            return Stream.empty();
        }

        return collection.stream().filter(Objects::nonNull);
    }
}
