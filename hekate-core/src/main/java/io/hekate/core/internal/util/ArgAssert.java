/*
 * Copyright 2018 The Hekate Project
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

public final class ArgAssert {
    private ArgAssert() {
        // No-op.
    }

    public static void check(boolean that, String msg) throws IllegalArgumentException {
        doCheck(that, null, msg);
    }

    public static <T> T notNull(T obj, String component) {
        doCheck(obj != null, component, " must be not null.");

        return obj;
    }

    public static String notEmpty(String str, String component) {
        String trimmed = notNull(str, component).trim();

        doCheck(!trimmed.isEmpty(), component, " must have non-whitespace characters.");

        return trimmed;
    }

    public static void isFalse(boolean condition, String msg) {
        isTrue(!condition, msg);
    }

    public static void isTrue(boolean condition, String msg) {
        check(condition, msg);
    }

    private static void doCheck(boolean that, String component, String msg) throws IllegalArgumentException {
        if (!that) {
            throw new IllegalArgumentException(component == null ? msg : component + msg);
        }
    }
}
