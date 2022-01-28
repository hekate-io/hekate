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

package io.hekate.messaging.internal;

import io.hekate.HekateTestBase;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.operation.AckMode;
import io.hekate.messaging.operation.Aggregate;
import io.hekate.messaging.operation.Broadcast;
import io.hekate.messaging.operation.Request;
import io.hekate.messaging.operation.Send;
import io.hekate.messaging.operation.Subscribe;
import io.hekate.messaging.operation.SubscribeCallback;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class MessagingChannelDefaultMethodsTest extends HekateTestBase {
    private MessagingChannel<String> channel;

    private Send<String> send;

    private Request<String> request;

    private Subscribe<String> subscribe;

    private Broadcast<String> broadcast;

    private Aggregate<String> aggregate;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        send = mock(Send.class, call -> {
            if (call.getMethod().getReturnType().equals(Send.class)) {
                return send;
            } else {
                return null;
            }
        });

        request = mock(Request.class, call -> {
            if (call.getMethod().getReturnType().equals(Request.class)) {
                return request;
            } else {
                return null;
            }
        });

        subscribe = mock(Subscribe.class, call -> {
            if (call.getMethod().getReturnType().equals(Subscribe.class)) {
                return subscribe;
            } else {
                return null;
            }
        });

        broadcast = mock(Broadcast.class, call -> {
            if (call.getMethod().getReturnType().equals(Broadcast.class)) {
                return broadcast;
            } else {
                return null;
            }
        });

        aggregate = mock(Aggregate.class, call -> {
            if (call.getMethod().getReturnType().equals(Aggregate.class)) {
                return aggregate;
            } else {
                return null;
            }
        });

        channel = mock(MessagingChannel.class);

        when(channel.newSend(anyString())).thenReturn(send);
        when(channel.newRequest(anyString())).thenReturn(request);
        when(channel.newSubscribe(anyString())).thenReturn(subscribe);
        when(channel.newBroadcast(anyString())).thenReturn(broadcast);
        when(channel.newAggregate(anyString())).thenReturn(aggregate);
    }

    @Test
    public void testAsyncSend() {
        when(channel.sendAsync(anyString())).thenCallRealMethod();

        channel.sendAsync("test");

        verify(channel).newSend(eq("test"));

        verify(send).withAckMode(eq(AckMode.REQUIRED));
        verify(send).submit();
        verifyNoMoreInteractions(send);
    }

    @Test
    public void testAsyncSendNoAck() {
        when(channel.sendAsync(anyString(), any(AckMode.class))).thenCallRealMethod();

        channel.sendAsync("test", AckMode.NOT_NEEDED);

        verify(channel).newSend(eq("test"));

        verify(send).withAckMode(eq(AckMode.NOT_NEEDED));
        verify(send).submit();
        verifyNoMoreInteractions(send);
    }

    @Test
    public void testAsyncAffinitySend() {
        when(channel.sendAsync(anyString(), anyString())).thenCallRealMethod();

        channel.sendAsync("affinity", "test");

        verify(channel).newSend(eq("test"));

        verify(send).withAckMode(eq(AckMode.REQUIRED));
        verify(send).withAffinity("affinity");
        verify(send).submit();
        verifyNoMoreInteractions(send);
    }

    @Test
    public void testAsyncAffinitySendNoAck() {
        when(channel.sendAsync(anyString(), anyString(), any(AckMode.class))).thenCallRealMethod();

        channel.sendAsync("affinity", "test", AckMode.NOT_NEEDED);

        verify(channel).newSend(eq("test"));

        verify(send).withAckMode(eq(AckMode.NOT_NEEDED));
        verify(send).withAffinity("affinity");
        verify(send).submit();
        verifyNoMoreInteractions(send);
    }

    @Test
    public void testSend() throws Exception {
        doCallRealMethod().when(channel).send(anyString());

        channel.send("test");

        verify(channel).newSend(eq("test"));

        verify(send).withAckMode(eq(AckMode.REQUIRED));
        verify(send).sync();
        verifyNoMoreInteractions(send);
    }

    @Test
    public void testSendNoAck() throws Exception {
        doCallRealMethod().when(channel).send(anyString(), any(AckMode.class));

        channel.send("test", AckMode.NOT_NEEDED);

        verify(channel).newSend(eq("test"));

        verify(send).withAckMode(eq(AckMode.NOT_NEEDED));
        verify(send).sync();
        verifyNoMoreInteractions(send);
    }

    @Test
    public void testAffinitySend() throws Exception {
        doCallRealMethod().when(channel).send(anyString(), anyString());

        channel.send("affinity", "test");

        verify(channel).newSend(eq("test"));

        verify(send).withAckMode(eq(AckMode.REQUIRED));
        verify(send).withAffinity(eq("affinity"));
        verify(send).sync();
        verifyNoMoreInteractions(send);
    }

    @Test
    public void testAffinitySendNoAck() throws Exception {
        doCallRealMethod().when(channel).send(anyString(), anyString(), any(AckMode.class));

        channel.send("affinity", "test", AckMode.NOT_NEEDED);

        verify(channel).newSend(eq("test"));

        verify(send).withAckMode(eq(AckMode.NOT_NEEDED));
        verify(send).withAffinity(eq("affinity"));
        verify(send).sync();
        verifyNoMoreInteractions(send);
    }

    @Test
    public void testAsyncRequest() {
        when(channel.requestAsync(anyString())).thenCallRealMethod();

        channel.requestAsync("test");

        verify(channel).newRequest(eq("test"));

        verify(request).submit();
        verifyNoMoreInteractions(request);
    }

    @Test
    public void testRequest() throws Exception {
        when(channel.request(anyString())).thenCallRealMethod();

        channel.request("test");

        verify(channel).newRequest(eq("test"));

        verify(request).response();
        verifyNoMoreInteractions(request);
    }

    @Test
    public void testAsyncAffinityRequest() {
        when(channel.requestAsync(anyString(), anyString())).thenCallRealMethod();

        channel.requestAsync("affinity", "test");

        verify(channel).newRequest(eq("test"));

        verify(request).withAffinity("affinity");
        verify(request).submit();
        verifyNoMoreInteractions(request);
    }

    @Test
    public void testAffinityRequest() throws Exception {
        when(channel.request(anyString(), anyString())).thenCallRealMethod();

        channel.request("affinity", "test");

        verify(channel).newRequest(eq("test"));

        verify(request).withAffinity("affinity");
        verify(request).response();
        verifyNoMoreInteractions(request);
    }

    @Test
    public void testAsyncSubscribe() {
        when(channel.subscribeAsync(anyString(), any())).thenCallRealMethod();

        SubscribeCallback<String> callback = (err, rsp) -> { /* No-op. */ };

        channel.subscribeAsync("test", callback);

        verify(channel).newSubscribe(eq("test"));

        verify(subscribe).submit(eq(callback));
        verifyNoMoreInteractions(subscribe);
    }

    @Test
    public void testSubscribe() throws Exception {
        when(channel.subscribe(anyString())).thenCallRealMethod();

        channel.subscribe("test");

        verify(channel).newSubscribe(eq("test"));

        verify(subscribe).responses();
        verifyNoMoreInteractions(subscribe);
    }

    @Test
    public void testAsyncAffinitySubscribe() {
        when(channel.subscribeAsync(anyString(), anyString(), any())).thenCallRealMethod();

        SubscribeCallback<String> callback = (err, rsp) -> { /* No-op. */ };

        channel.subscribeAsync("affinity", "test", callback);

        verify(channel).newSubscribe(eq("test"));

        verify(subscribe).withAffinity(eq("affinity"));
        verify(subscribe).submit(eq(callback));
        verifyNoMoreInteractions(subscribe);
    }

    @Test
    public void testAffinitySubscribe() throws Exception {
        when(channel.subscribe(anyString(), anyString())).thenCallRealMethod();

        channel.subscribe("affinity", "test");

        verify(channel).newSubscribe(eq("test"));

        verify(subscribe).withAffinity(eq("affinity"));
        verify(subscribe).responses();
        verifyNoMoreInteractions(subscribe);
    }

    @Test
    public void testAsyncBroadcast() {
        when(channel.broadcastAsync(anyString())).thenCallRealMethod();

        channel.broadcastAsync("test");

        verify(channel).newBroadcast(eq("test"));

        verify(broadcast).withAckMode(eq(AckMode.REQUIRED));
        verify(broadcast).submit();
        verifyNoMoreInteractions(broadcast);
    }

    @Test
    public void testAsyncBroadcastNoAck() {
        when(channel.broadcastAsync(anyString(), any(AckMode.class))).thenCallRealMethod();

        channel.broadcastAsync("test", AckMode.NOT_NEEDED);

        verify(channel).newBroadcast(eq("test"));

        verify(broadcast).withAckMode(eq(AckMode.NOT_NEEDED));
        verify(broadcast).submit();
        verifyNoMoreInteractions(broadcast);
    }

    @Test
    public void testAsyncAffinityBroadcast() {
        when(channel.broadcastAsync(anyString(), anyString())).thenCallRealMethod();

        channel.broadcastAsync("affinity", "test");

        verify(channel).newBroadcast(eq("test"));

        verify(broadcast).withAckMode(eq(AckMode.REQUIRED));
        verify(broadcast).withAffinity("affinity");
        verify(broadcast).submit();
        verifyNoMoreInteractions(broadcast);
    }

    @Test
    public void testAsyncAffinityBroadcastNoAck() {
        when(channel.broadcastAsync(anyString(), anyString(), any(AckMode.class))).thenCallRealMethod();

        channel.broadcastAsync("affinity", "test", AckMode.NOT_NEEDED);

        verify(channel).newBroadcast(eq("test"));

        verify(broadcast).withAckMode(eq(AckMode.NOT_NEEDED));
        verify(broadcast).withAffinity("affinity");
        verify(broadcast).submit();
        verifyNoMoreInteractions(broadcast);
    }

    @Test
    public void testBroadcast() throws Exception {
        doCallRealMethod().when(channel).broadcast(anyString());

        channel.broadcast("test");

        verify(channel).newBroadcast(eq("test"));

        verify(broadcast).withAckMode(eq(AckMode.REQUIRED));
        verify(broadcast).sync();
        verifyNoMoreInteractions(broadcast);
    }

    @Test
    public void testBroadcastNoAck() throws Exception {
        doCallRealMethod().when(channel).broadcast(anyString(), any(AckMode.class));

        channel.broadcast("test", AckMode.NOT_NEEDED);

        verify(channel).newBroadcast(eq("test"));

        verify(broadcast).withAckMode(eq(AckMode.NOT_NEEDED));
        verify(broadcast).sync();
        verifyNoMoreInteractions(broadcast);
    }

    @Test
    public void testAffinityBroadcast() throws Exception {
        doCallRealMethod().when(channel).broadcast(anyString(), anyString());

        channel.broadcast("affinity", "test");

        verify(channel).newBroadcast(eq("test"));

        verify(broadcast).withAckMode(eq(AckMode.REQUIRED));
        verify(broadcast).withAffinity(eq("affinity"));
        verify(broadcast).sync();
        verifyNoMoreInteractions(broadcast);
    }

    @Test
    public void testAffinityBroadcastNoAck() throws Exception {
        doCallRealMethod().when(channel).broadcast(anyString(), anyString(), any(AckMode.class));

        channel.broadcast("affinity", "test", AckMode.NOT_NEEDED);

        verify(channel).newBroadcast(eq("test"));

        verify(broadcast).withAckMode(eq(AckMode.NOT_NEEDED));
        verify(broadcast).withAffinity(eq("affinity"));
        verify(broadcast).sync();
        verifyNoMoreInteractions(broadcast);
    }

    @Test
    public void testAsyncAggregate() {
        when(channel.aggregateAsync(anyString())).thenCallRealMethod();

        channel.aggregateAsync("test");

        verify(channel).newAggregate(eq("test"));

        verify(aggregate).submit();
        verifyNoMoreInteractions(aggregate);
    }

    @Test
    public void testAggregate() throws Exception {
        when(channel.aggregate(anyString())).thenCallRealMethod();

        channel.aggregate("test");

        verify(channel).newAggregate(eq("test"));

        verify(aggregate).get();
        verifyNoMoreInteractions(aggregate);
    }

    @Test
    public void testAsyncAffinityAggregate() {
        when(channel.aggregateAsync(anyString(), anyString())).thenCallRealMethod();

        channel.aggregateAsync("affinity", "test");

        verify(channel).newAggregate(eq("test"));

        verify(aggregate).withAffinity("affinity");
        verify(aggregate).submit();
        verifyNoMoreInteractions(aggregate);
    }

    @Test
    public void testAffinityAggregate() throws Exception {
        when(channel.aggregate(anyString(), anyString())).thenCallRealMethod();

        channel.aggregate("affinity", "test");

        verify(channel).newAggregate(eq("test"));

        verify(aggregate).withAffinity("affinity");
        verify(aggregate).get();
        verifyNoMoreInteractions(aggregate);
    }
}
