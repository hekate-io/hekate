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

package io.hekate.test;

import io.hekate.HekateTestBase;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkClientCallback;
import io.hekate.network.NetworkMessage;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NetworkClientCallbackMock<T> implements NetworkClientCallback<T> {
    private final List<T> messages = new CopyOnWriteArrayList<>();

    private final AtomicInteger connects = new AtomicInteger();

    private final AtomicInteger disconnects = new AtomicInteger();

    private final List<Throwable> errors = new CopyOnWriteArrayList<>();

    private volatile InetSocketAddress lastLocalAddress;

    @Override
    public void onMessage(NetworkMessage<T> message, NetworkClient<T> client) throws IOException {
        messages.add(message.decode());
    }

    @Override
    public void onConnect(NetworkClient<T> client) {
        connects.incrementAndGet();

        lastLocalAddress = client.localAddress();
    }

    @Override
    public void onDisconnect(NetworkClient<T> client, Optional<Throwable> cause) {
        cause.ifPresent(errors::add);

        disconnects.incrementAndGet();
    }

    public List<T> getMessages() {
        return messages;
    }

    public void assertConnects(int n) {
        assertEquals(n, connects.get());
    }

    public InetSocketAddress getLastLocalAddress() {
        return lastLocalAddress;
    }

    public void assertDisconnects(int n) {
        assertEquals(n, disconnects.get());
    }

    @SafeVarargs
    public final void awaitForMessages(T... messages) throws Exception {
        @SuppressWarnings("unchecked")
        T[][] batch = (T[][])new Object[1][];

        batch[0] = messages;

        awaitForMessagesBatch(batch);
    }

    @SafeVarargs
    public final void awaitForMessagesBatch(T[]... messages) throws Exception {
        List<T> expected = new ArrayList<>();

        for (int i = 0; i < messages.length; i++) {
            expected.addAll(Arrays.asList(messages[i]));
        }

        HekateTestBase.busyWait("messages [expected=" + expected + ", received=" + this.messages + ']', () ->
            this.messages.containsAll(expected)
        );
    }

    public void awaitForDisconnects(int n) throws Exception {
        HekateTestBase.busyWait("disconnects [count=" + n + ']', () -> disconnects.get() >= n);
    }

    public void assertErrors(int errors) {
        assertEquals(errors, this.errors.size());
    }

    public void assertNoErrors() {
        assertTrue(errors.toString(), errors.isEmpty());
    }

    public void assertMaxErrors(int maxErrors) {
        assertTrue("expected MAX errors=" + maxErrors + ", real errors=" + errors.size(), errors.size() <= maxErrors);
    }

    public List<Throwable> getErrors() {
        return errors;
    }

    public void reset() {
        messages.clear();
        connects.set(0);
        disconnects.set(0);
        errors.clear();
        lastLocalAddress = null;
    }
}
