package io.hekate.messaging.internal;

import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.broadcast.AggregateCallback;
import io.hekate.messaging.broadcast.BroadcastCallback;
import io.hekate.messaging.unicast.RequestCallback;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class MessagingAffinityTest extends MessagingServiceTestBase {
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
            String[] tokens = msg.split(":");
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

    public MessagingAffinityTest(MessagingTestContext ctx) {
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
                MessagingChannel<String> channel = sender.get().forNode(receiver.getNodeId());

                channel.affinitySend(j, AffinityCollector.messageForCallback(j, i), new SendCallbackMock());
                channel.affinitySend(j, AffinityCollector.messageForFuture(j, i));
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
                MessagingChannel<String> channel = sender.get().forNode(receiver.getNodeId());

                @SuppressWarnings("unchecked")
                RequestCallback<String> callback = mock(RequestCallback.class);

                channel.affinityRequest(j, AffinityCollector.messageForCallback(j, i), callback);
                channel.affinityRequest(j, AffinityCollector.messageForFuture(j, i));
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
            sender.requestWithSyncCallback(receiver.getNodeId(), "request");

            assertNotNull(callbackRef.get());

            callbackRef.get().get();

            receiver.checkReceiverError();
        });
    }

    @Test
    public void testBroadcast() throws Exception {
        List<AffinityCollector> collectors = new ArrayList<>();
        List<TestChannel> receivers = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            AffinityCollector collector = new AffinityCollector();

            receivers.add(createChannel(c -> c.setReceiver(msg ->
                collector.collect(msg.get())
            )).join());

            collectors.add(collector);
        }

        TestChannel sender = createChannel().join();

        int partitionSize = 10;

        for (int i = 0; i < partitionSize; i++) {
            for (int j = 0; j < partitionSize; j++) {
                MessagingChannel<String> channel = sender.get().forRemotes();

                @SuppressWarnings("unchecked")
                BroadcastCallback<String> callback = mock(BroadcastCallback.class);

                channel.affinityBroadcast(j, AffinityCollector.messageForCallback(j, i), callback);
                channel.affinityBroadcast(j, AffinityCollector.messageForFuture(j, i));
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
        List<AffinityCollector> collectors = new ArrayList<>();
        List<TestChannel> receivers = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            AffinityCollector collector = new AffinityCollector();

            receivers.add(createChannel(c -> c.setReceiver(msg -> {
                    collector.collect(msg.get());

                    msg.reply("ok");
                }
            )).join());

            collectors.add(collector);
        }

        TestChannel sender = createChannel().join();

        int partitionSize = 10;

        for (int i = 0; i < partitionSize; i++) {
            for (int j = 0; j < partitionSize; j++) {
                MessagingChannel<String> channel = sender.get().forRemotes();

                @SuppressWarnings("unchecked")
                AggregateCallback<String> callback = mock(AggregateCallback.class);

                channel.affinityAggregate(j, AffinityCollector.messageForCallback(j, i), callback);
                channel.affinityAggregate(j, AffinityCollector.messageForFuture(j, i));
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
