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

package io.hekate.messaging.unicast;

import io.hekate.core.HekateException;
import java.util.Optional;

/**
 * Signals that the result of a request operation was {@link ReplyDecision#REJECT rejected} by a {@link ResponseCallback}.
 *
 * @see ResponseCallback#accept(Throwable, Response)
 */
public class RejectedReplyException extends HekateException {
    private static final long serialVersionUID = 1;

    private final Optional<Object> reply;

    /**
     * Constructs new instance with an error message and a reply that was rejected.
     *
     * @param message Error message.
     * @param reply Rejected reply (if reply was successfully received but rejected by a {@link ResponseCallback}).
     * @param cause Error cause (if request operation failed and {@link ResponseCallback} decided to {@link ReplyDecision#REJECT} such
     * failure).
     */
    public RejectedReplyException(String message, Object reply, Throwable cause) {
        super(message, cause, true, false);

        this.reply = Optional.ofNullable(reply);
    }

    /**
     * Returns the rejected reply message. Note that message can be absent if request operation failed with an error.
     *
     * @return Rejected reply.
     */
    public Optional<Object> reply() {
        return reply;
    }

    @Override
    public String getMessage() {
        return super.getMessage() + " [reply=" + reply.orElse(null) + ']';
    }
}
