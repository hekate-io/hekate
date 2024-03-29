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

package io.hekate.core.internal.util;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

/**
 * Common utilities.
 */
public final class Utils {
    /** Shortcut for {@link System#lineSeparator()}. */
    public static final String NL = System.lineSeparator();

    /** {@link Charset} for UTF-8. */
    public static final Charset UTF_8 = StandardCharsets.UTF_8;

    /** Magic bytes that should be appended to the first packet of a network connection. */
    public static final int MAGIC_BYTES = 19_800_124;

    private static final DecimalFormatSymbols NUMBER_FORMAT_SYMBOLS;

    static {
        // Formatting options.
        NUMBER_FORMAT_SYMBOLS = new DecimalFormatSymbols(Locale.getDefault());

        NUMBER_FORMAT_SYMBOLS.setDecimalSeparator('.');
    }

    private Utils() {
        // No-op.
    }

    public static String numberFormat(String pattern, Number number) {
        return new DecimalFormat(pattern, NUMBER_FORMAT_SYMBOLS).format(number);
    }

    public static String byteSizeFormat(long bytes) {
        long mb = bytes / 1024 / 1024;

        double gb = mb / 1024D;

        if (gb >= 1) {
            return numberFormat("###.##Gb", gb);
        } else {
            return mb + "Mb";
        }
    }

    public static int mod(int hash, int size) {
        if (hash < 0) {
            return -hash % size;
        } else {
            return hash % size;
        }
    }

    /**
     * Returns {@code true} if the specified value is a power of two.
     *
     * @param n Number to check.
     *
     * @return {@code true} if the specified value is a power of two.
     */
    public static boolean isPowerOfTwo(int n) {
        return (n & n - 1) == 0 && n > 0;
    }

    /**
     * Converts the specified collection to a comma-separated string by applying the specified mapper to each element and wrapping the
     * resulting string with {@code {...}}.
     *
     * @param collection Collection.
     * @param mapper Element mapper.
     * @param <T> Collection type.
     *
     * @return String that has the following format: {@code {element1, element2, ..., elementN}}
     */
    public static <T> String toString(Collection<T> collection, Function<? super T, String> mapper) {
        return collection.stream().map(mapper).sorted().collect(Collectors.joining(", ", "{", "}"));
    }

    /**
     * Returns {@code null} if the specified string is {@code null} or is an empty string after {@link String#trim() trimming}; returns a
     * {@link String#trim() trimmed} string otherwise.
     *
     * @param str String.
     *
     * @return {@code null} or {@link String#trim() trimmed} string.
     */
    public static String nullOrTrim(String str) {
        if (str == null) {
            return null;
        }

        str = str.trim();

        return str.isEmpty() ? null : str;
    }

    /**
     * Returns {@code defaultVal} if the specified string is {@code null} or is an empty string after {@link String#trim() trimming};
     * returns a {@link String#trim() trimmed} string otherwise.
     *
     * @param str String.
     * @param defaultVal Default value.
     *
     * @return {@code null} or {@link String#trim() trimmed} string.
     */
    public static String nullOrTrim(String str, String defaultVal) {
        String trimmed = nullOrTrim(str);

        return trimmed == null ? defaultVal : trimmed;
    }

    public static boolean hasText(String str) {
        return str != null
            && !str.isEmpty()
            && containsText(str);
    }

    public static String camelCase(CharSequence str) {
        StringBuilder buf = new StringBuilder();

        boolean capitalize = true;

        for (int i = 0, len = str.length(); i < len; ++i) {
            char c = str.charAt(i);

            switch (c) {
                case '-':
                case '.':
                case '_': {
                    capitalize = true;

                    break;
                }
                default: {
                    if (capitalize) {
                        buf.append(Character.toUpperCase(c));

                        capitalize = false;
                    } else {
                        buf.append(c);
                    }
                }
            }
        }

        return buf.toString();
    }

    /**
     * Returns an immutable copy of the specified list by filtering out all {@code null} elements.
     *
     * @param source Source list, can be {@code null} (in such case an empty list will be returned by this method).
     * @param <T> Element type.
     *
     * @return Immutable copy of the specified list.
     */
    public static <T> List<T> nullSafeImmutableCopy(List<T> source) {
        if (source == null || source.isEmpty()) {
            return emptyList();
        } else {
            return unmodifiableList(source.stream().filter(Objects::nonNull).collect(toList()));
        }
    }

    private static boolean containsText(CharSequence str) {
        for (int i = 0, len = str.length(); i < len; ++i) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return true;
            }
        }

        return false;
    }
}
