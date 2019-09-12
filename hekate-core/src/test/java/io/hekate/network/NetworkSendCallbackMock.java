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

package io.hekate.network;

import io.hekate.HekateTestBase;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NetworkSendCallbackMock<T> implements NetworkSendCallback<T> {
    private static class Failure<T> {
        private final T message;

        private final Throwable cause;

        public Failure(T message, Throwable cause) {
            this.message = message;
            this.cause = cause;
        }
    }

    private final List<T> sentMessages = new CopyOnWriteArrayList<>();

    private final List<Failure<T>> failedMessages = new CopyOnWriteArrayList<>();

    @Override
    public void onComplete(T msg, Throwable error) {
        if (error == null) {
            sentMessages.add(msg);
        } else {
            failedMessages.add(new Failure<>(msg, error));
        }
    }

    public void awaitForSentOrFailed(int expected) throws Exception {
        HekateTestBase.busyWait("messages sending/failing [expected=" + expected + ']', () ->
            sentMessages.size() + failedMessages.size() >= expected
        );
    }

    @SafeVarargs
    public final void awaitForSent(T... messages) throws Exception {
        List<T> expected = new ArrayList<>(Arrays.asList(messages));

        HekateTestBase.busyWait("messages sending [expected=" + expected + ']', () -> {
            for (Iterator<T> it = expected.iterator(); it.hasNext(); ) {
                T msg = it.next();

                if (sentMessages.contains(msg)) {
                    it.remove();
                } else {
                    Throwable err = getFailure(msg);

                    if (err != null) {
                        throw new AssertionError("Failed to await for message sending due to an error [message=" + msg + ']', err);
                    }
                }
            }

            return expected.isEmpty();
        });
    }

    @SafeVarargs
    public final void awaitForErrors(T... messages) throws Exception {
        List<T> expected = Arrays.asList(messages);

        HekateTestBase.busyWait("messages failure [expected=" + expected + ']', () ->
            failedMessages.stream().map(f -> f.message).collect(Collectors.toSet()).containsAll(expected)
        );
    }

    public Throwable getFailure(T message) {
        return failedMessages.stream().filter(m -> m.message.equals(message)).map(m -> m.cause).findFirst().orElse(null);
    }

    public void assertSent(int n) {
        assertEquals(n, sentMessages.size());
    }

    public void assertFailed(int n) {
        assertEquals(n, failedMessages.size());
    }

    public void assertFailed(T message) {
        assertTrue(message.toString(), failedMessages.stream().anyMatch(f -> f.message.equals(message)));
    }

    public void reset() {
        sentMessages.clear();
        failedMessages.clear();
    }
}
