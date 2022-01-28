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

import io.hekate.core.internal.util.Jvm;
import org.slf4j.LoggerFactory;

/**
 * Fatal error handling policy.
 *
 * <p>
 * Implementations of this interface can be registered via {@link HekateBootstrap#setFatalErrorPolicy(HekateFatalErrorPolicy)} method in
 * order to perform actions upon fatal errors if {@link Hekate} node.
 * </p>
 */
public interface HekateFatalErrorPolicy {
    /** JVM exit code (={@value}) for {@link #exitJvm()} policy. */
    int JVM_EXIT_CODE = 250;

    /**
     * Handles the fatal error.
     *
     * <p>
     * Implementations can use the supplied {@link HekateFatalErrorContext} object to perform operations on {@link Hekate} node.
     * </p>
     *
     * @param err Fatal error.
     * @param ctx Error processing context.
     */
    void handleFatalError(Throwable err, HekateFatalErrorContext ctx);

    /**
     * Rejoin upon any fatal error.
     *
     * @return Policy.
     */
    static HekateFatalErrorPolicy rejoin() {
        return (err, ctx) -> {
            LoggerFactory.getLogger(HekateFatalErrorPolicy.class).error("Re-joining Hekate node due to a fatal error.", err);

            ctx.rejoin();
        };
    }

    /**
     * Terminate upon any fatal error.
     *
     * @return Policy.
     */
    static HekateFatalErrorPolicy terminate() {
        return (err, ctx) -> ctx.terminate();
    }

    /**
     * Call {@link System#exit(int)} with {@value #JVM_EXIT_CODE} exit code upon any fatal error.
     *
     * @return Policy.
     */
    static HekateFatalErrorPolicy exitJvm() {
        return exitJvm(JVM_EXIT_CODE);
    }

    /**
     * Call {@link System#exit(int)} with the specified exit code upon any fatal error.
     *
     * @param exitCode JVM exit code.
     *
     * @return Policy.
     */
    static HekateFatalErrorPolicy exitJvm(int exitCode) {
        return (err, ctx) -> {
            LoggerFactory.getLogger(HekateFatalErrorPolicy.class).error("Exiting JVM due to a fatal error in Hekate node.", err);

            Jvm.exit(exitCode);
        };
    }
}
