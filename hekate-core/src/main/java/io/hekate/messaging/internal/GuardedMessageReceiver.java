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

package io.hekate.messaging.internal;

import io.hekate.messaging.Message;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessagingEndpoint;
import io.hekate.util.StateGuard;

class GuardedMessageReceiver<T> implements MessageReceiver<T> {
    private final StateGuard guard;

    private final MessageReceiver<T> receiver;

    public GuardedMessageReceiver(StateGuard guard, MessageReceiver<T> receiver) {
        this.guard = guard;
        this.receiver = receiver;
    }

    @Override
    public void receive(Message<T> msg) {
        guard.lockRead();

        try {
            // Do not notify original receiver if node is shutting down.
            if (guard.isInitialized()) {
                receiver.receive(msg);
            }
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public void onConnect(MessagingEndpoint<T> endpoint) {
        guard.lockRead();

        try {
            if (guard.isInitialized()) {
                receiver.onConnect(endpoint);
            }
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public void onDisconnect(MessagingEndpoint<T> endpoint) {
        guard.lockRead();

        try {
            if (guard.isInitialized()) {
                receiver.onDisconnect(endpoint);
            }
        } finally {
            guard.unlockRead();
        }
    }
}
