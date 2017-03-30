package io.hekate.messaging.internal;

import io.hekate.messaging.Message;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.MessagingTimeoutException;
import io.hekate.messaging.unicast.RequestFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MessagingTimeoutTest extends MessagingServiceTestBase {
    public MessagingTimeoutTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testRequest() throws Exception {
        AtomicReference<CountDownLatch> hangLatchRef = new AtomicReference<>();

        createChannel(c -> c.withReceiver(msg -> {
            await(hangLatchRef.get());

            msg.reply("done");
        })).join();

        TestChannel sender = createChannel(c -> c.withMessagingTimeout(1)).join();

        repeat(5, i -> {
            hangLatchRef.set(new CountDownLatch(1));

            try {
                sender.get().forRemotes().request("must-fail-" + i).get(3, TimeUnit.SECONDS);
            } catch (MessagingFutureException e) {
                assertTrue(getStacktrace(e), e.isCausedBy(MessagingTimeoutException.class));
                assertEquals("Messaging operation timed out [message=must-fail-" + i + ']',
                    e.findCause(MessagingTimeoutException.class).getMessage());
            } finally {
                hangLatchRef.get().countDown();
            }
        });
    }

    @Test
    public void testPartialReply() throws Exception {
        Exchanger<Message<String>> msgRef = new Exchanger<>();

        createChannel(c -> c.withReceiver(msg -> {
            try {
                msgRef.exchange(msg);
            } catch (InterruptedException e) {
                fail(e.toString());
            }
        })).join();

        TestChannel sender = createChannel(c -> c.withMessagingTimeout(150)).join();

        repeat(3, i -> {
            RequestFuture<String> future = sender.get().forRemotes().request("must-fail-" + i);

            Message<String> request = msgRef.exchange(null);

            repeat(5, j -> {
                request.replyPartial("part");

                sleep(50);
            });

            request.reply("final");

            future.get(3, TimeUnit.SECONDS);
        });
    }

    @Test
    public void testRequestNoTimeout() throws Exception {
        createChannel(c -> c.withReceiver(msg -> {
            msg.reply("done");
        })).join();

        TestChannel sender = createChannel(c -> c.withMessagingTimeout(1000)).join();

        repeat(5, i ->
            sender.get().forRemotes().request("request-" + i).get(3, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testSend() throws Exception {
        createChannel().join();

        TestChannel sender = createChannel(c -> c.withMessagingTimeout(1)).join();

        repeat(5, i -> {
            try {
                sender.get().forRemotes().send("must-fail-" + i).get(3, TimeUnit.SECONDS);
            } catch (MessagingFutureException e) {
                assertTrue(getStacktrace(e), e.isCausedBy(MessagingTimeoutException.class));
                assertEquals("Messaging operation timed out [message=must-fail-" + i + ']',
                    e.findCause(MessagingTimeoutException.class).getMessage());
            }
        });
    }

    @Test
    public void testSendNoTimeout() throws Exception {
        createChannel(c -> c.withReceiver(msg -> {
            msg.reply("done");
        })).join();

        TestChannel sender = createChannel(c -> c.withMessagingTimeout(1000)).join();

        repeat(5, i ->
            sender.get().forRemotes().request("request-" + i).get(3, TimeUnit.SECONDS)
        );
    }
}
