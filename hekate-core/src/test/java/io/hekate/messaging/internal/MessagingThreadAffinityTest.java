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

package io.hekate.messaging.internal;

import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.broadcast.AggregateCallback;
import io.hekate.messaging.broadcast.BroadcastCallback;
import io.hekate.messaging.unicast.ResponseCallback;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class MessagingThreadAffinityTest extends MessagingServiceTestBase {
    private static class AffinityCollector {
        private final Map<Integer, List<Map.Entry<Integer, Thread>>> callbackBuf = new ConcurrentHashMap<>();

        private final Map<Integer, List<Map.Entry<Integer, Thread>>> futureBuf = new ConcurrentHashMap<>();

        public static String messageForFuture(int partition, int msg) {
            return partition + ":" + msg + ":future";
        }

        public static String messageForCallback(int partition, int msg) {
            return partition + ":" + msg + ":callback";
        }

        public void collect(String msg) {
            String[] tokens = msg.split(":", 3);
            Integer key = Integer.parseInt(tokens[0]);
            Integer value = Integer.parseInt(tokens[1]);
            String type = tokens[2];

            Map<Integer, List<Map.Entry<Integer, Thread>>> buf;

            if ("future".equals(type)) {
                buf = futureBuf;
            } else {
                buf = callbackBuf;
            }

            List<Map.Entry<Integer, Thread>> partition = buf.get(key);

            if (partition == null) {
                partition = Collections.synchronizedList(new ArrayList<>());

                List<Map.Entry<Integer, Thread>> existing = buf.putIfAbsent(key, partition);

                if (existing != null) {
                    partition = existing;
                }
            }

            partition.add(new SimpleEntry<>(value, Thread.currentThread()));
        }

        public Map<Integer, List<Map.Entry<Integer, Thread>>> getCallbackBuffer() {
            return callbackBuf;
        }

        public Map<Integer, List<Map.Entry<Integer, Thread>>> getFutureBuffer() {
            return futureBuf;
        }
    }

    public MessagingThreadAffinityTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testSend() throws Exception {
        AffinityCollector collector = new AffinityCollector();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg ->
            collector.collect(msg.get())
        )).join();

        TestChannel sender = createChannel().join();

        sender.awaitForTopology(Arrays.asList(sender, receiver));

        int partitionSize = 10;

        for (int i = 0; i < partitionSize; i++) {
            for (int j = 0; j < partitionSize; j++) {
                MessagingChannel<String> channel = sender.get()
                    .forNode(receiver.nodeId())
                    .withAffinity(j);

                channel.send(AffinityCollector.messageForCallback(j, i), new SendCallbackMock());
                channel.send(AffinityCollector.messageForFuture(j, i));
            }
        }

        for (int i = 0; i < partitionSize; i++) {
            receiver.awaitForMessage(AffinityCollector.messageForCallback(i, partitionSize - 1));
            receiver.awaitForMessage(AffinityCollector.messageForFuture(i, partitionSize - 1));
        }

        verifyAffinity(collector.getCallbackBuffer(), partitionSize);
        verifyAffinity(collector.getFutureBuffer(), partitionSize);
    }

    @Test
    public void testRequest() throws Exception {
        AffinityCollector collector = new AffinityCollector();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
            collector.collect(msg.get());

            msg.reply("ok");
        })).join();

        TestChannel sender = createChannel().join();

        sender.awaitForTopology(Arrays.asList(sender, receiver));

        int partitionSize = 10;

        for (int i = 0; i < partitionSize; i++) {
            for (int j = 0; j < partitionSize; j++) {
                MessagingChannel<String> channel = sender.get()
                    .forNode(receiver.nodeId())
                    .withAffinity(j);

                @SuppressWarnings("unchecked")
                ResponseCallback<String> callback = mock(ResponseCallback.class);

                channel.request(AffinityCollector.messageForCallback(j, i), callback);
                channel.request(AffinityCollector.messageForFuture(j, i));
            }
        }

        for (int i = 0; i < partitionSize; i++) {
            receiver.awaitForMessage(AffinityCollector.messageForCallback(i, partitionSize - 1));
            receiver.awaitForMessage(AffinityCollector.messageForFuture(i, partitionSize - 1));
        }

        verifyAffinity(collector.getCallbackBuffer(), partitionSize);
        verifyAffinity(collector.getFutureBuffer(), partitionSize);
    }

    @Test
    public void testResponseThread() throws Exception {
        TestChannel sender = createChannel().join();

        AtomicReference<SendCallbackMock> callbackRef = new AtomicReference<>();

        TestChannel receiver = createChannel(c -> c.setReceiver(msg -> {
            Thread thread = Thread.currentThread();

            SendCallbackMock callback = new SendCallbackMock() {
                @Override
                protected void onSendSuccess() {
                    assertSame(Thread.currentThread(), thread);
                }
            };

            callbackRef.set(callback);

            msg.reply("response", callback);
        })).join();

        awaitForChannelsTopology(sender, receiver);

        repeat(5, i -> {
            get(sender.get().forNode(receiver.nodeId()).withAffinity(i).request("request"));

            callbackRef.get().get();

            receiver.checkReceiverError();
        });
    }

    @Test
    public void testBroadcast() throws Exception {
        int testNodes = 3;

        List<AffinityCollector> collectors = new ArrayList<>();
        List<TestChannel> receivers = new ArrayList<>();

        for (int i = 0; i < testNodes; i++) {
            AffinityCollector collector = new AffinityCollector();

            receivers.add(createChannel(c -> {
                c.setBackupNodes(testNodes - 1);
                c.setReceiver(msg ->
                    collector.collect(msg.get())
                );
            }).join());

            collectors.add(collector);
        }

        TestChannel sender = createChannel(c -> c.withBackupNodes(testNodes - 1)).join();

        int partitionSize = 10;

        for (int i = 0; i < partitionSize; i++) {
            for (int j = 0; j < partitionSize; j++) {
                MessagingChannel<String> channel = sender.get()
                    .forRemotes()
                    .withAffinity(j);

                @SuppressWarnings("unchecked")
                BroadcastCallback<String> callback = mock(BroadcastCallback.class);

                channel.broadcast(AffinityCollector.messageForCallback(j, i), callback);
                channel.broadcast(AffinityCollector.messageForFuture(j, i));
            }
        }

        for (TestChannel receiver : receivers) {
            for (int i = 0; i < partitionSize; i++) {
                receiver.awaitForMessage(AffinityCollector.messageForCallback(i, partitionSize - 1));
                receiver.awaitForMessage(AffinityCollector.messageForFuture(i, partitionSize - 1));
            }
        }

        for (AffinityCollector collector : collectors) {
            verifyAffinity(collector.getCallbackBuffer(), partitionSize);
            verifyAffinity(collector.getFutureBuffer(), partitionSize);
        }
    }

    @Test
    public void testAggregate() throws Exception {
        int testNodes = 3;

        List<AffinityCollector> collectors = new ArrayList<>(testNodes);
        List<TestChannel> receivers = new ArrayList<>(testNodes);

        for (int i = 0; i < testNodes; i++) {
            AffinityCollector collector = new AffinityCollector();

            receivers.add(createChannel(c -> {
                c.setBackupNodes(testNodes - 1);
                c.setReceiver(msg -> {
                        collector.collect(msg.get());

                        msg.reply("ok");
                    }
                );
            }).join());

            collectors.add(collector);
        }

        TestChannel sender = createChannel(c -> c.withBackupNodes(testNodes - 1)).join();

        int partitionSize = 10;

        for (int i = 0; i < partitionSize; i++) {
            for (int j = 0; j < partitionSize; j++) {
                MessagingChannel<String> channel = sender.get()
                    .forRemotes()
                    .withAffinity(j);

                @SuppressWarnings("unchecked")
                AggregateCallback<String> callback = mock(AggregateCallback.class);

                channel.aggregate(AffinityCollector.messageForCallback(j, i), callback);
                channel.aggregate(AffinityCollector.messageForFuture(j, i));
            }
        }

        for (TestChannel receiver : receivers) {
            for (int i = 0; i < partitionSize; i++) {
                receiver.awaitForMessage(AffinityCollector.messageForCallback(i, partitionSize - 1));
                receiver.awaitForMessage(AffinityCollector.messageForFuture(i, partitionSize - 1));
            }
        }

        for (AffinityCollector collector : collectors) {
            verifyAffinity(collector.getCallbackBuffer(), partitionSize);
            verifyAffinity(collector.getFutureBuffer(), partitionSize);
        }
    }

    private void verifyAffinity(Map<Integer, List<Map.Entry<Integer, Thread>>> partitions, int partitionSize) {
        assertFalse(partitions.isEmpty());

        for (List<Map.Entry<Integer, Thread>> partition : partitions.values()) {
            Thread thread = null;

            for (int i = 0; i < partitionSize; i++) {
                assertEquals(i, partition.get(i).getKey().intValue());

                if (thread == null) {
                    thread = partition.get(i).getValue();
                } else {
                    assertSame(thread, partition.get(i).getValue());
                }
            }
        }
    }
}
