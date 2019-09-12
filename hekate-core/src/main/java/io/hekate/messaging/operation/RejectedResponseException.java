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

package io.hekate.messaging.operation;

import io.hekate.core.HekateException;
import io.hekate.messaging.retry.RetryResponsePredicate;

/**
 * Signals that the result of a request operation was rejected by a {@link RetryResponsePredicate}.
 *
 * @see RetryResponsePredicate#shouldRetry(Response)
 */
public class RejectedResponseException extends HekateException {
    private static final long serialVersionUID = 1;

    private final Object response;

    /**
     * Constructs new instance.
     *
     * @param message Error message.
     * @param response Rejected response.
     */
    public RejectedResponseException(String message, Object response) {
        super(message, null, true, false);

        this.response = response;
    }

    /**
     * Returns the rejected reply message.
     *
     * @return Rejected reply.
     */
    public Object response() {
        return response;
    }

    @Override
    public String getMessage() {
        return super.getMessage() + " [response=" + response + ']';
    }
}
