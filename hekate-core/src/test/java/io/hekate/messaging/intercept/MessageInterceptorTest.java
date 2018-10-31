package io.hekate.messaging.intercept;

import io.hekate.messaging.MessageMetaData;
import io.hekate.messaging.MessageMetaData.MetaDataCodec;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.internal.MessagingServiceTestBase;
import io.hekate.messaging.internal.TestChannel;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MessageInterceptorTest extends MessagingServiceTestBase {
    private static final MessageMetaData.Key<String> TEST_KEY = MessageMetaData.Key.of("TEST", MetaDataCodec.TEXT);

    public MessageInterceptorTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testRequest() throws Exception {
        AtomicReference<String> lastServerReceiveComplete = new AtomicReference<>();

        List<TestChannel> channels = createAndJoinChannels(3, c -> {
            c.withReceiver(msg -> msg.reply(msg.payload() + "-reply"));
            c.withInterceptor(new AllMessageInterceptor<String>() {
                @Override
                public void interceptClientSend(ClientSendContext<String> ctx) {
                    assertNotNull(ctx.payload());
                    assertSame(OutboundType.REQUEST, ctx.type());
                    assertNotNull(ctx.receiver());
                    assertNotNull(ctx.channelName());
                    assertNotNull(ctx.topology());
                    assertFalse(ctx.hasMetaData());

                    // Store attribute to verify later.
                    ctx.setAttribute("test-attr", "test-val");

                    // Store meta-data to verify on the server side.
                    ctx.metaData().set(TEST_KEY, "client-meta-val");
                    assertTrue(ctx.hasMetaData());

                    ctx.overrideMessage(ctx.payload() + "-CS-" + ctx.hasAffinity() + "-" + ctx.affinityKey());
                }

                @Override
                public void interceptClientReceiveResponse(ClientReceiveContext<String> ctx) {
                    assertNotNull(ctx.payload());
                    assertSame(InboundType.FINAL_RESPONSE, ctx.type());

                    assertSame(OutboundType.REQUEST, ctx.outboundContext().type());
                    assertNotNull(ctx.outboundContext().receiver());
                    assertNotNull(ctx.outboundContext().channelName());
                    assertNotNull(ctx.outboundContext().topology());

                    // Verify attribute.
                    assertEquals("test-val", ctx.outboundContext().getAttribute("test-attr"));

                    // Verify meta-data.
                    assertTrue(ctx.readMetaData().isPresent());
                    assertEquals("server-meta-val", ctx.readMetaData().get().get(TEST_KEY));

                    ctx.overrideMessage(ctx.payload() + "-CR");
                }

                @Override
                public void interceptServerReceive(ServerReceiveContext<String> ctx) {
                    assertNotNull(ctx.payload());
                    assertSame(OutboundType.REQUEST, ctx.type());
                    assertNotNull(ctx.from());
                    assertNotNull(ctx.channelName());

                    // Store attribute to verify later.
                    ctx.setAttribute("test-attr", "test-val");

                    // Verify meta-data.
                    assertTrue(ctx.readMetaData().isPresent());
                    assertEquals("client-meta-val", ctx.readMetaData().get().get(TEST_KEY));

                    ctx.overrideMessage(ctx.payload() + "-SR");
                }

                @Override
                public void interceptServerReceiveComplete(ServerInboundContext<String> ctx) {
                    assertSame(OutboundType.REQUEST, ctx.type());
                    assertNotNull(ctx.from());
                    assertNotNull(ctx.channelName());

                    // Verify attribute.
                    assertEquals("test-val", ctx.getAttribute("test-attr"));

                    lastServerReceiveComplete.compareAndSet(null, ctx.payload());
                }

                @Override
                public void interceptServerSend(ServerSendContext<String> ctx) {
                    assertNotNull(ctx.payload());
                    assertSame(InboundType.FINAL_RESPONSE, ctx.type());
                    assertFalse(ctx.hasMetaData());

                    assertNotNull(ctx.inboundContext().channelName());
                    assertSame(OutboundType.REQUEST, ctx.inboundContext().type());
                    assertNotNull(ctx.inboundContext().from());

                    // Verify attribute.
                    assertEquals("test-val", ctx.inboundContext().getAttribute("test-attr"));

                    // Store meta-data to verify on the client side.
                    ctx.metaData().set(TEST_KEY, "server-meta-val");
                    assertTrue(ctx.hasMetaData());

                    ctx.overrideMessage(ctx.payload() + "-SS");
                }

                @Override
                public void interceptClientReceiveConfirmation(ClientOutboundContext<String> ctx) {
                    throw new UnsupportedOperationException("Unexpected method call.");
                }
            });
        });

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                String msg = "test-" + from.nodeId();

                MessagingChannel<String> channel = from.channel().forNode(to.nodeId());

                // No affinity.
                assertEquals(msg + "-CS-false-null-SR-reply-SS-CR", channel.request(msg).submit().result());
                busyWait("receive complete", () ->
                    (msg + "-CS-false-null-SR").equals(lastServerReceiveComplete.getAndSet(null))
                );

                to.assertReceived(msg + "-CS-false-null-SR");

                // With affinity.
                assertEquals(msg + "-CS-true-1-SR-reply-SS-CR", channel.request(msg)
                    .withAffinity(1)
                    .submit()
                    .result()
                );

                busyWait("receive complete", () ->
                    (msg + "-CS-true-1-SR").equals(lastServerReceiveComplete.getAndSet(null))
                );

                to.assertReceived(msg + "-CS-true-1-SR");
            }
        }
    }

    @Test
    public void testRequestFailure() throws Exception {
        @SuppressWarnings("unchecked")
        ClientMessageInterceptor<String> interceptor = mock(ClientMessageInterceptor.class);

        List<TestChannel> channels = createAndJoinChannels(3, c -> {
            c.withReceiver(msg -> {
                throw TEST_ERROR;
            });
            c.withInterceptor(interceptor);
        });

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                MessagingChannel<String> channel = from.channel().forNode(to.nodeId());

                MessagingFutureException err = expect(MessagingFutureException.class, () ->
                    get(channel.request("msg").submit())
                );

                verify(interceptor).interceptClientReceiveError(any(), ArgumentMatchers.same(err.getCause()));
            }
        }
    }

    @Test
    public void testSubscribe() throws Exception {
        AtomicReference<String> lastServerReceiveComplete = new AtomicReference<>();

        List<TestChannel> channels = createAndJoinChannels(3, c -> {
            c.withReceiver(msg -> {
                msg.partialReply(msg.payload() + "-reply-part");
                msg.partialReply(msg.payload() + "-reply-part");
                msg.reply(msg.payload() + "-reply");
            });
            c.withInterceptor(new AllMessageInterceptor<String>() {
                @Override
                public void interceptClientSend(ClientSendContext<String> ctx) {
                    assertNotNull(ctx.payload());
                    assertSame(OutboundType.SUBSCRIBE, ctx.type());
                    assertNotNull(ctx.receiver());
                    assertNotNull(ctx.topology());

                    // Store attribute to verify later.
                    ctx.setAttribute("test-attr", "test-val");

                    // Store meta-data to verify on the server side.
                    ctx.metaData().set(TEST_KEY, "client-meta-val");
                    assertTrue(ctx.hasMetaData());

                    ctx.overrideMessage(ctx.payload() + "-CS-" + ctx.hasAffinity() + "-" + ctx.affinityKey());
                }

                @Override
                public void interceptClientReceiveResponse(ClientReceiveContext<String> ctx) {
                    assertNotNull(ctx.payload());

                    if (ctx.payload().contains("-part")) {
                        assertSame(InboundType.RESPONSE_CHUNK, ctx.type());
                    } else {
                        assertSame(InboundType.FINAL_RESPONSE, ctx.type());
                    }

                    assertSame(OutboundType.SUBSCRIBE, ctx.outboundContext().type());
                    assertNotNull(ctx.outboundContext().receiver());
                    assertNotNull(ctx.outboundContext().topology());

                    // Verify attribute.
                    assertEquals("test-val", ctx.outboundContext().getAttribute("test-attr"));

                    // Verify meta-data.
                    assertTrue(ctx.readMetaData().isPresent());
                    assertEquals("server-meta-val" + ctx.type(), ctx.readMetaData().get().get(TEST_KEY));

                    ctx.overrideMessage(ctx.payload() + "-CR");
                }

                @Override
                public void interceptServerReceive(ServerReceiveContext<String> ctx) {
                    assertNotNull(ctx.payload());
                    assertSame(OutboundType.SUBSCRIBE, ctx.type());
                    assertNotNull(ctx.from());

                    // Store attribute to verify later.
                    ctx.setAttribute("test-attr", "test-val");

                    // Verify meta-data.
                    assertTrue(ctx.readMetaData().isPresent());
                    assertEquals("client-meta-val", ctx.readMetaData().get().get(TEST_KEY));

                    ctx.overrideMessage(ctx.payload() + "-SR");
                }

                @Override
                public void interceptServerReceiveComplete(ServerInboundContext<String> ctx) {
                    assertNotNull(ctx.payload());
                    assertSame(OutboundType.SUBSCRIBE, ctx.type());
                    assertNotNull(ctx.from());

                    lastServerReceiveComplete.compareAndSet(null, ctx.payload());

                    // Verify attribute.
                    assertEquals("test-val", ctx.getAttribute("test-attr"));
                }

                @Override
                public void interceptServerSend(ServerSendContext<String> ctx) {
                    assertNotNull(ctx.payload());

                    if (ctx.payload().contains("-part")) {
                        assertSame(InboundType.RESPONSE_CHUNK, ctx.type());
                    } else {
                        assertSame(InboundType.FINAL_RESPONSE, ctx.type());
                    }

                    assertNotNull(ctx.inboundContext().from());
                    assertSame(OutboundType.SUBSCRIBE, ctx.inboundContext().type());

                    // Verify attribute.
                    assertEquals("test-val", ctx.inboundContext().getAttribute("test-attr"));

                    // Store meta-data to verify on the client side.
                    ctx.metaData().set(TEST_KEY, "server-meta-val" + ctx.type());
                    assertTrue(ctx.hasMetaData());

                    ctx.overrideMessage(ctx.payload() + "-SS");
                }

                @Override
                public void interceptClientReceiveConfirmation(ClientOutboundContext<String> ctx) {
                    throw new UnsupportedOperationException("Unexpected method call.");
                }
            });
        });

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                MessagingChannel<String> channel = from.channel().forNode(to.nodeId());

                String msg = "test1-" + from.nodeId();

                // No affinity.
                List<String> replies = channel.subscribe(msg).collectAll(3, TimeUnit.SECONDS);

                busyWait("receive complete", () ->
                    (msg + "-CS-false-null-SR").equals(lastServerReceiveComplete.getAndSet(null))
                );

                assertEquals(msg + "-CS-false-null-SR-reply-part-SS-CR", replies.get(0));
                assertEquals(msg + "-CS-false-null-SR-reply-part-SS-CR", replies.get(1));
                assertEquals(msg + "-CS-false-null-SR-reply-SS-CR", replies.get(2));

                to.assertReceived(msg + "-CS-false-null-SR");

                // With affinity.
                List<String> affinityReplies = channel.subscribe(msg)
                    .withAffinity(1)
                    .collectAll(3, TimeUnit.SECONDS);

                busyWait("receive complete", () ->
                    (msg + "-CS-true-1-SR").equals(lastServerReceiveComplete.getAndSet(null))
                );

                assertEquals(msg + "-CS-true-1-SR-reply-part-SS-CR", affinityReplies.get(0));
                assertEquals(msg + "-CS-true-1-SR-reply-part-SS-CR", affinityReplies.get(1));
                assertEquals(msg + "-CS-true-1-SR-reply-SS-CR", affinityReplies.get(2));

                to.assertReceived(msg + "-CS-true-1-SR");
            }
        }
    }

    @Test
    public void testSubscribeError() throws Exception {
        @SuppressWarnings("unchecked")
        ClientMessageInterceptor<String> interceptor = mock(ClientMessageInterceptor.class);

        List<TestChannel> channels = createAndJoinChannels(3, c -> {
            c.withReceiver(msg -> {
                throw TEST_ERROR;
            });
            c.withInterceptor(interceptor);
        });

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                MessagingChannel<String> channel = from.channel().forNode(to.nodeId());

                MessagingFutureException err = expect(MessagingFutureException.class, () ->
                    channel.subscribe("msg").collectAll(3, TimeUnit.SECONDS)
                );

                verify(interceptor).interceptClientReceiveError(any(), ArgumentMatchers.same(err.getCause()));
            }
        }
    }

    @Test
    public void testSend() throws Exception {
        AtomicReference<String> lastServerReceiveComplete = new AtomicReference<>();

        List<TestChannel> channels = createAndJoinChannels(3, c ->
            c.withInterceptor(new AllMessageInterceptor<String>() {
                @Override
                public void interceptClientSend(ClientSendContext<String> ctx) {
                    assertNotNull(ctx.payload());
                    assertSame(OutboundType.SEND_NO_ACK, ctx.type());
                    assertNotNull(ctx.receiver());
                    assertNotNull(ctx.topology());
                    assertNotNull(ctx.channelName());

                    // Store meta-data to very on the server side.
                    ctx.metaData().set(TEST_KEY, "client-meta-val");
                    assertTrue(ctx.hasMetaData());

                    ctx.overrideMessage(ctx.payload() + "-CS");
                }

                @Override
                public void interceptServerReceive(ServerReceiveContext<String> ctx) {
                    assertNotNull(ctx.payload());
                    assertSame(OutboundType.SEND_NO_ACK, ctx.type());
                    assertNotNull(ctx.from());
                    assertNotNull(ctx.channelName());

                    // Verify meta-data.
                    assertTrue(ctx.readMetaData().isPresent());
                    assertEquals("client-meta-val", ctx.readMetaData().get().get(TEST_KEY));

                    ctx.overrideMessage(ctx.payload() + "-SR");
                }

                @Override
                public void interceptServerReceiveComplete(ServerInboundContext<String> ctx) {
                    assertNotNull(ctx.payload());
                    assertSame(OutboundType.SEND_NO_ACK, ctx.type());
                    assertNotNull(ctx.from());
                    assertNotNull(ctx.channelName());

                    lastServerReceiveComplete.compareAndSet(null, ctx.payload());
                }

                @Override
                public void interceptClientReceiveConfirmation(ClientOutboundContext<String> ctx) {
                    throw new UnsupportedOperationException("Unexpected method call.");
                }

                @Override
                public void interceptClientReceiveResponse(ClientReceiveContext<String> ctx) {
                    throw new UnsupportedOperationException("Unexpected method call.");
                }

                @Override
                public void interceptClientReceiveError(ClientOutboundContext<String> ctx, Throwable err) {
                    throw new UnsupportedOperationException("Unexpected method call.");
                }

                @Override
                public void interceptServerSend(ServerSendContext<String> ctx) {
                    throw new UnsupportedOperationException("Unexpected method call.");
                }
            })
        );

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                String msg = "test-" + from.nodeId();

                from.channel().forNode(to.nodeId())
                    .send(msg)
                    .submit()
                    .get();

                busyWait("receive complete", () ->
                    (msg + "-CS-SR").equals(lastServerReceiveComplete.getAndSet(null))
                );
            }
        }

        for (TestChannel to : channels) {
            for (TestChannel from : channels) {
                to.awaitForMessage("test-" + from.nodeId() + "-CS-SR");
            }
        }
    }

    @Test
    public void testSendWithConfirmation() throws Exception {
        AtomicReference<String> lastServerReceiveComplete = new AtomicReference<>();

        List<TestChannel> channels = createAndJoinChannels(3, c ->
            c.withInterceptor(new AllMessageInterceptor<String>() {
                @Override
                public void interceptClientSend(ClientSendContext<String> ctx) {
                    assertNotNull(ctx.payload());
                    assertSame(OutboundType.SEND_WITH_ACK, ctx.type());
                    assertNotNull(ctx.receiver());
                    assertNotNull(ctx.topology());
                    assertNotNull(ctx.channelName());
                    assertFalse(ctx.hasMetaData());

                    // Store attribute to verify later.
                    ctx.setAttribute("test-attr", "test-val");

                    // Store meta-data to very on the server side.
                    ctx.metaData().set(TEST_KEY, "client-meta-val");
                    assertTrue(ctx.hasMetaData());

                    ctx.overrideMessage(ctx.payload() + "-CS");
                }

                @Override
                public void interceptClientReceiveConfirmation(ClientOutboundContext<String> ctx) {
                    assertNotNull(ctx.payload());
                    assertSame(OutboundType.SEND_WITH_ACK, ctx.type());
                    assertNotNull(ctx.receiver());
                    assertNotNull(ctx.topology());
                    assertNotNull(ctx.channelName());
                    assertEquals("test-val", ctx.getAttribute("test-attr"));
                }

                @Override
                public void interceptServerReceive(ServerReceiveContext<String> ctx) {
                    assertNotNull(ctx.payload());
                    assertSame(OutboundType.SEND_WITH_ACK, ctx.type());
                    assertNotNull(ctx.channelName());
                    assertNotNull(ctx.from());

                    // Verify meta-data.
                    assertTrue(ctx.readMetaData().isPresent());
                    assertEquals("client-meta-val", ctx.readMetaData().get().get(TEST_KEY));

                    ctx.overrideMessage(ctx.payload() + "-SR");
                }

                @Override
                public void interceptServerReceiveComplete(ServerInboundContext<String> ctx) {
                    assertNotNull(ctx.payload());
                    assertSame(OutboundType.SEND_WITH_ACK, ctx.type());
                    assertNotNull(ctx.channelName());
                    assertNotNull(ctx.from());

                    lastServerReceiveComplete.compareAndSet(null, ctx.payload());
                }

                @Override
                public void interceptClientReceiveError(ClientOutboundContext<String> ctx, Throwable err) {
                    throw new UnsupportedOperationException("Unexpected method call.");
                }

                @Override
                public void interceptClientReceiveResponse(ClientReceiveContext<String> ctx) {
                    throw new UnsupportedOperationException("Unexpected method call.");
                }

                @Override
                public void interceptServerSend(ServerSendContext<String> ctx) {
                    throw new UnsupportedOperationException("Unexpected method call.");
                }
            })
        );

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                String msg = "test-" + from.nodeId();

                from.channel().forNode(to.nodeId())
                    .send(msg)
                    .withConfirmReceive(true)
                    .submit()
                    .get();

                busyWait("receive complete", () ->
                    (msg + "-CS-SR").equals(lastServerReceiveComplete.getAndSet(null))
                );
            }
        }

        for (TestChannel to : channels) {
            for (TestChannel from : channels) {
                to.awaitForMessage("test-" + from.nodeId() + "-CS-SR");
            }
        }
    }

    @Test
    public void testSendWithConfirmationFailure() throws Exception {
        @SuppressWarnings("unchecked")
        ClientMessageInterceptor<String> interceptor = mock(ClientMessageInterceptor.class);

        List<TestChannel> channels = createAndJoinChannels(3, c -> {
            c.withReceiver(msg -> {
                throw TEST_ERROR;
            });
            c.withInterceptor(interceptor);
        });

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                MessagingChannel<String> channel = from.channel().forNode(to.nodeId());

                MessagingFutureException err = expect(MessagingFutureException.class, () ->
                    get(channel.send("msg").withConfirmReceive(true).submit())
                );

                verify(interceptor).interceptClientReceiveError(any(), ArgumentMatchers.same(err.getCause()));
            }
        }
    }
}
