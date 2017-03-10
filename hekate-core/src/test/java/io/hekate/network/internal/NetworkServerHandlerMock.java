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

package io.hekate.network.internal;

import io.hekate.HekateTestBase;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkMessage;
import io.hekate.network.NetworkSendCallback;
import io.hekate.network.NetworkServerHandler;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NetworkServerHandlerMock<T> implements NetworkServerHandler<T> {
    private static class ClientContext<T> {
        private final List<T> messages = new CopyOnWriteArrayList<>();

        private final List<T> connects = new CopyOnWriteArrayList<>();

        private final AtomicInteger disconnects = new AtomicInteger();

        private final List<Throwable> errors = new CopyOnWriteArrayList<>();
    }

    private static class Reply<T> {
        private final T message;

        private final NetworkSendCallback<T> callback;

        public Reply(T message, NetworkSendCallback<T> callback) {
            this.message = message;
            this.callback = callback;
        }

        private void apply(NetworkEndpoint<T> endpoint) {
            if (callback == null) {
                endpoint.send(message);
            } else {
                endpoint.send(message, callback);
            }
        }
    }

    private final Map<InetSocketAddress, ClientContext<T>> contexts = new ConcurrentHashMap<>();

    private final List<Reply<T>> sendOnConnect = new CopyOnWriteArrayList<>();

    private final Map<T, List<Reply<T>>> replyWith = new ConcurrentHashMap<>();

    private final List<T> disconnectOnMessages = new CopyOnWriteArrayList<>();

    private final AtomicReference<NetworkServerHandlerMock<T>> delegate = new AtomicReference<>();

    public void withDelegate(NetworkServerHandlerMock<T> delegate) {
        this.delegate.set(delegate);
    }

    @SafeVarargs
    public final void addSendOnConnect(T... messages) {
        addSendOnConnect(null, messages);
    }

    @SafeVarargs
    public final void addSendOnConnect(NetworkSendCallback<T> callback, T... messages) {
        for (T message : messages) {
            sendOnConnect.add(new Reply<>(message, callback));
        }
    }

    @SafeVarargs
    public final void addReplyWith(T request, T... responses) {
        addReplyWith(request, null, responses);
    }

    @SafeVarargs
    public final void addReplyWith(T request, NetworkSendCallback<T> callback, T... responses) {
        List<Reply<T>> replies = new ArrayList<>(responses.length);

        for (T response : responses) {
            replies.add(new Reply<>(response, callback));
        }

        replyWith.put(request, replies);
    }

    public void addDisconnectOnMessage(T message) {
        disconnectOnMessages.add(message);
    }

    @Override
    public void onMessage(NetworkMessage<T> netMsg, NetworkEndpoint<T> client) throws IOException {
        T msg = netMsg.decode();

        getCtx(client.getRemoteAddress(), true).messages.add(msg);

        if (disconnectOnMessages.contains(msg)) {
            client.disconnect();
        }

        List<Reply<T>> responses = replyWith.get(msg);

        if (responses != null) {
            responses.forEach(r -> r.apply(client));
        }

        if (delegate.get() != null) {
            delegate.get().onMessage(netMsg, client);
        }
    }

    @Override
    public void onConnect(T msg, NetworkEndpoint<T> client) {
        getCtx(client.getRemoteAddress(), true).connects.add(msg);

        sendOnConnect.forEach(r -> r.apply(client));

        if (delegate.get() != null) {
            delegate.get().onConnect(msg, client);
        }
    }

    @Override
    public void onFailure(NetworkEndpoint<T> client, Throwable error) {
        getCtx(client.getRemoteAddress(), true).errors.add(error);

        if (delegate.get() != null) {
            delegate.get().onFailure(client, error);
        }
    }

    @Override
    public void onDisconnect(NetworkEndpoint<T> client) {
        getCtx(client.getRemoteAddress(), true).disconnects.incrementAndGet();

        if (delegate.get() != null) {
            delegate.get().onDisconnect(client);
        }
    }

    public List<T> getConnectPayload(NetworkClient<T> client) {
        return getConnectPayload(client.getLocalAddress());
    }

    public List<T> getConnectPayload(InetSocketAddress address) {
        return getCtx(address, false).connects;
    }

    public List<T> getMessages(NetworkClient<T> client) {
        return getMessages(client.getLocalAddress());
    }

    public List<T> getMessages(InetSocketAddress address) {
        return getCtx(address, false).messages;
    }

    public int getConnects(NetworkClient<T> client) {
        return getConnects(client.getLocalAddress());
    }

    public int getConnects(InetSocketAddress address) {
        return getCtx(address, false).connects.size();
    }

    public void assertConnects(NetworkClient<T> client, int n) {
        assertConnects(client.getLocalAddress(), n);
    }

    public void assertConnects(InetSocketAddress address, int n) {
        assertEquals(n, getCtx(address, false).connects.size());
    }

    public int getDisconnects(NetworkClient<T> client) {
        return getDisconnects(client.getLocalAddress());
    }

    public int getDisconnects(InetSocketAddress address) {
        return getCtx(address, false).disconnects.get();
    }

    public void assertDisconnects(NetworkClient<T> client, int n) {
        assertDisconnects(client.getLocalAddress(), n);
    }

    public void assertDisconnects(InetSocketAddress address, int n) {
        assertEquals(n, getCtx(address, false).disconnects.get());
    }

    public void assertErrors(NetworkClient<T> client, int errors) {
        assertErrors(client.getLocalAddress(), errors);
    }

    public void assertErrors(InetSocketAddress address, int errors) {
        assertEquals(errors, getCtx(address, false).errors.size());
    }

    public void assertNoErrors(NetworkClient<T> client) {
        assertNoErrors(client.getLocalAddress());
    }

    public void assertNoErrors(InetSocketAddress address) {
        assertTrue(getCtx(address, false).errors.isEmpty());
    }

    public List<Throwable> getErrors(NetworkClient<T> client) {
        return getErrors(client.getLocalAddress());
    }

    public List<Throwable> getErrors(InetSocketAddress address) {
        return getCtx(address, false).errors;
    }

    @SafeVarargs
    public final void assertNotReceived(NetworkClient<T> client, T... messages) {
        assertNotReceived(client.getLocalAddress(), messages);
    }

    @SafeVarargs
    public final void assertNotReceived(InetSocketAddress address, T... messages) {
        List<T> expected = Arrays.asList(messages);

        assertFalse(getCtx(address, false).messages.stream().anyMatch(expected::contains));
    }

    public void reset() {
        contexts.clear();
        replyWith.clear();
        sendOnConnect.clear();
        disconnectOnMessages.clear();
        delegate.set(null);
    }

    @SafeVarargs
    public final void awaitForMessages(NetworkClient<T> client, T... messages) throws Exception {
        awaitForMessages(client.getLocalAddress(), messages);
    }

    @SafeVarargs
    public final void awaitForMessages(InetSocketAddress address, T... messages) throws Exception {
        List<T> expected = Arrays.asList(messages);

        HekateTestBase.busyWait("messages [from=" + address + ", expected=" + expected + ']', () -> {
            ClientContext<T> ctx = getCtx(address, false);

            if (ctx != null) {
                List<T> msgs = ctx.messages;

                if (msgs.containsAll(expected)) {
                    return true;
                }
            }

            return false;
        });
    }

    public final void awaitForConnect(NetworkClient<T> client) throws Exception {
        awaitForConnect(client.getLocalAddress());
    }

    public final void awaitForConnect(InetSocketAddress address) throws Exception {
        HekateTestBase.busyWait("connect [from=" + address + ']', () -> {
            ClientContext<T> ctx = getCtx(address, false);

            if (ctx != null) {
                List<T> connects = ctx.connects;

                if (!connects.isEmpty()) {
                    return true;
                }
            }

            return false;
        });
    }

    private ClientContext<T> getCtx(InetSocketAddress address, boolean register) {
        ClientContext<T> ctx = contexts.get(address);

        if (ctx == null) {
            ctx = new ClientContext<>();

            if (register) {
                ClientContext<T> oldCtx = contexts.putIfAbsent(address, ctx);

                if (oldCtx != null) {
                    ctx = oldCtx;
                }
            }
        }

        return ctx;
    }
}
