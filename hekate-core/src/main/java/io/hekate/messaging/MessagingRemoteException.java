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

import static io.hekate.core.internal.util.Utils.NL;

/**
 * Signals that response message couldn't be received due to an error on a remote node.
 *
 * @see #remoteStackTrace()
 */
public class MessagingRemoteException extends MessagingException {
    private static final long serialVersionUID = 1L;

    private final String remoteStackTrace;

    /**
     * Constructs new instance.
     *
     * @param message Error message.
     * @param remoteStackTrace Stack trace of a remote failure (see {@link #remoteStackTrace()}).
     */
    public MessagingRemoteException(String message, String remoteStackTrace) {
        super(format(message, remoteStackTrace), null, true, false);

        this.remoteStackTrace = remoteStackTrace;
    }

    private MessagingRemoteException(MessagingRemoteException cause) {
        super("Messaging operation failed on remote node (see cause for details).", cause);

        this.remoteStackTrace = cause.remoteStackTrace();
    }

    /**
     * Returns the stack trace of a remote failure.
     *
     * @return Stack trace of a remote failure.
     */
    public String remoteStackTrace() {
        return remoteStackTrace;
    }

    @Override
    public HekateException forkFromAsync() {
        return new MessagingRemoteException(this);
    }

    private static String format(String message, String remoteStackTrace) {
        return message + NL
            + "[--- Remote stack trace start ---]" + NL
            + remoteStackTrace
            + "[--- Remote stack trace end ---]";
    }
}
