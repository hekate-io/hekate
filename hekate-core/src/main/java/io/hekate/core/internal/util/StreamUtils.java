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
