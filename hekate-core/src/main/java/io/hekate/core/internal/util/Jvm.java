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

import java.lang.management.ManagementFactory;
import java.util.Optional;

/**
 * JVM utilities.
 */
public final class Jvm {
    /**
     * JVM exit handler.
     *
     * @see System#exit(int)
     */
    public interface ExitHandler {
        /**
         * Exits the JVM.
         *
         * @param code Exit code.
         */
        void exit(int code);
    }

    private static final String PID;

    private static volatile ExitHandler exitHandler;

    static {
        String jvmName = ManagementFactory.getRuntimeMXBean().getName();

        int index = jvmName.indexOf('@');

        if (index < 0) {
            PID = "";
        } else {
            PID = jvmName.substring(0, index);
        }
    }

    private Jvm() {
        // No-op.
    }

    /**
     * Returns the PID of the JVM process.
     *
     * @return PID or an empty string if PID couldn't be resolved.
     */
    public static String pid() {
        return PID;
    }

    /**
     * Exits the JVM by calling the {@link #setExitHandler(ExitHandler) pre-configured} {@link ExitHandler} or by calling the {@link
     * System#exit(int)} method (if handler is not defined).
     *
     * @param code Exit code.
     */
    public static void exit(int code) {
        ExitHandler handler = exitHandler().orElse(System::exit);

        handler.exit(code);
    }

    /**
     * Configures the exit handler.
     *
     * @param handler Exit handler.
     */
    public static void setExitHandler(ExitHandler handler) {
        exitHandler = handler;
    }

    /**
     * Returns an instance of {@link ExitHandler} if it was defined via {@link #setExitHandler(ExitHandler)}.
     *
     * @return Exit handler.
     */
    public static Optional<ExitHandler> exitHandler() {
        return Optional.ofNullable(exitHandler);
    }
}
