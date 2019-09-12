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

package io.hekate.util.async;

import java.util.Collection;

/**
 * Interface to await for the completion of an asynchronous task.
 */
public interface Waiting {
    /**
     * Do not wait.
     */
    Waiting NO_WAIT = () -> {
        // N-op.
    };

    /**
     * Waits for this task to complete.
     *
     * @throws InterruptedException If the current thread was interrupted while waiting.
     */
    void await() throws InterruptedException;

    /**
     * Returns a single {@link Waiting} that will {@link #await() await} for all of the specified tasks to complete.
     *
     * @param all Tasks.
     *
     * @return single {@link Waiting} that will {@link #await() await} for all of the specified tasks to complete.
     */
    static Waiting awaitAll(Collection<Waiting> all) {
        if (all == null || all.isEmpty()) {
            return NO_WAIT;
        }

        return () -> {
            for (Waiting w : all) {
                if (w != null) {
                    w.await();
                }
            }
        };
    }

    /**
     * Uninterruptedly waits for this task to complete.
     */
    default void awaitUninterruptedly() {
        boolean interrupted = false;

        try {
            while (true) {
                try {
                    await();

                    break;
                } catch (InterruptedException e) {
                    interrupted = true;

                    // Make sure that interrupted flag is reset to 'false'.
                    Thread.interrupted();
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
