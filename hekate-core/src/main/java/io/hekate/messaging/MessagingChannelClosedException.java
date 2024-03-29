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

package io.hekate.messaging;

import io.hekate.core.HekateException;

/**
 * Signals that messaging operation can't be completed since messaging channel is already closed.
 */
public class MessagingChannelClosedException extends MessagingException {
    private static final long serialVersionUID = 1;

    /**
     * Constructs new instance.
     *
     * @param message Error message.
     * @param cause Error cause.
     */
    public MessagingChannelClosedException(String message, Throwable cause) {
        super(message, cause);
    }

    private MessagingChannelClosedException(MessagingChannelClosedException cause) {
        super(cause.getMessage(), cause);
    }

    @Override
    public HekateException forkFromAsync() {
        return new MessagingChannelClosedException(this);
    }
}
