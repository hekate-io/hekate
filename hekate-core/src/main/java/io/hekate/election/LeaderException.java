/*
 * Copyright 2017 The Hekate Project
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

package io.hekate.election;

import io.hekate.core.HekateFutureException;

/**
 * Failure of a leader election.
 *
 * @see ElectionService
 */
public class LeaderException extends HekateFutureException {
    private static final long serialVersionUID = 1L;

    /**
     * Constructs new instance.
     *
     * @param message Error message.
     * @param cause Error cause.
     */
    public LeaderException(String message, Throwable cause) {
        super(message, cause);
    }
}
