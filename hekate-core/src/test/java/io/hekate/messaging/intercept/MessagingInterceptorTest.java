package io.hekate.messaging.intercept;

import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.internal.MessagingServiceTestBase;
import io.hekate.messaging.internal.TestChannel;
import java.util.List;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MessagingInterceptorTest extends MessagingServiceTestBase {
    public MessagingInterceptorTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testRequest() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(3, c -> {
            c.withReceiver(msg -> msg.reply(msg.get() + "-reply"));
            c.withInterceptor(new MessageInterceptor<String>() {
                @Override
                public String beforeClientSend(String msg, ClientSendContext sndCtx) {
                    assertSame(OutboundType.REQUEST, sndCtx.type());
                    assertNotNull(sndCtx.receiver());
                    assertNotNull(sndCtx.channelName());
                    assertNotNull(sndCtx.topology());

                    // Store attribute to verify later.
                    sndCtx.setAttribute("test-attr", "test-val");

                    return msg + "-CS-" + sndCtx.hasAffinity() + "-" + sndCtx.affinityKey();
                }

                @Override
                public String beforeClientReceiveResponse(String rsp, ClientReceiveContext rcvCtx, ClientSendContext sndCtx) {
                    assertNotNull(rsp);
                    assertSame(InboundType.FINAL_RESPONSE, rcvCtx.type());

                    assertSame(OutboundType.REQUEST, sndCtx.type());
                    assertNotNull(sndCtx.receiver());
                    assertNotNull(sndCtx.channelName());
                    assertNotNull(sndCtx.topology());

                    // Verify attribute.
                    assertEquals("test-val", sndCtx.getAttribute("test-attr"));

                    return rsp + "-CR";
                }

                @Override
                public String beforeServerReceive(String msg, ServerReceiveContext rcvCtx) {
                    assertSame(OutboundType.REQUEST, rcvCtx.type());
                    assertNotNull(rcvCtx.from());
                    assertNotNull(rcvCtx.channelName());

                    // Store attribute to verify later.
                    rcvCtx.setAttribute("test-attr", "test-val");

                    return msg + "-SR";
                }

                @Override
                public String beforeServerSend(String rsp, ServerSendContext sndCtx, ServerReceiveContext rcvCtx) {
                    assertNotNull(rsp);
                    assertSame(InboundType.FINAL_RESPONSE, sndCtx.type());
                    assertNotNull(rcvCtx.channelName());

                    assertSame(OutboundType.REQUEST, rcvCtx.type());
                    assertNotNull(rcvCtx.from());

                    // Verify attribute.
                    assertEquals("test-val", rcvCtx.getAttribute("test-attr"));

                    return rsp + "-SS";
                }

                @Override
                public void onClientReceiveConfirmation(ClientSendContext sndCtx) {
                    throw new UnsupportedOperationException("Unexpected method call.");
                }
            });
        });

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                String msg1 = "test1-" + from.nodeId();
                String msg2 = "test2-" + from.nodeId();

                MessagingChannel<String> channel = from.get().forNode(to.nodeId());

                // No affinity.
                assertEquals(msg1 + "-CS-false-null-SR-reply-SS-CR", channel.request(msg1).response());
                assertEquals(msg2 + "-CS-false-null-SR-reply-SS-CR", channel.request(msg2).responseUninterruptedly());

                to.assertReceived(msg1 + "-CS-false-null-SR");
                to.assertReceived(msg2 + "-CS-false-null-SR");

                // With affinity.
                assertEquals(msg1 + "-CS-true-1-SR-reply-SS-CR", channel.withAffinity(1).request(msg1).response());
                assertEquals(msg2 + "-CS-true-1-SR-reply-SS-CR", channel.withAffinity(1).request(msg2).responseUninterruptedly());

                to.assertReceived(msg1 + "-CS-true-1-SR");
                to.assertReceived(msg2 + "-CS-true-1-SR");
            }
        }
    }

    @Test
    public void testRequestFailure() throws Exception {
        @SuppressWarnings("unchecked")
        MessageInterceptor<String> interceptor = mock(MessageInterceptor.class);

        List<TestChannel> channels = createAndJoinChannels(3, c -> {
            c.withReceiver(msg -> {
                throw TEST_ERROR;
            });
            c.withInterceptor(interceptor);
        });

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                MessagingChannel<String> channel = from.get().forNode(to.nodeId());

                MessagingFutureException err = expect(MessagingFutureException.class, () ->
                    get(channel.request("msg"))
                );

                verify(interceptor).onClientReceiveError(ArgumentMatchers.same(err.getCause()), any());
            }
        }
    }

    @Test
    public void testSend() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(3, c ->
            c.withInterceptor(new MessageInterceptor<String>() {
                @Override
                public String beforeClientSend(String msg, ClientSendContext sndCtx) {
                    assertSame(OutboundType.SEND_NO_ACK, sndCtx.type());
                    assertNotNull(sndCtx.receiver());
                    assertNotNull(sndCtx.topology());
                    assertNotNull(sndCtx.channelName());

                    return msg + "-CS";
                }

                @Override
                public String beforeServerReceive(String msg, ServerReceiveContext rcvCtx) {
                    assertSame(OutboundType.SEND_NO_ACK, rcvCtx.type());
                    assertNotNull(rcvCtx.from());
                    assertNotNull(rcvCtx.channelName());

                    return msg + "-SR";
                }

                @Override
                public void onClientReceiveConfirmation(ClientSendContext sndCtx) {
                    throw new UnsupportedOperationException("Unexpected method call.");
                }

                @Override
                public String beforeClientReceiveResponse(String rsp, ClientReceiveContext rcvCtx, ClientSendContext sndCtx) {
                    throw new UnsupportedOperationException("Unexpected method call.");
                }

                @Override
                public void onClientReceiveError(Throwable err, ClientSendContext sndCtx) {
                    throw new UnsupportedOperationException("Unexpected method call.");
                }

                @Override
                public String beforeServerSend(String rsp, ServerSendContext sndCtx, ServerReceiveContext rcvCtx) {
                    throw new UnsupportedOperationException("Unexpected method call.");
                }
            })
        );

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                String msg1 = "test1-" + from.nodeId();
                String msg2 = "test2-" + from.nodeId();

                from.get().forNode(to.nodeId()).send(msg1).get();
                from.get().forNode(to.nodeId()).send(msg2).getUninterruptedly();
            }
        }

        for (TestChannel to : channels) {
            for (TestChannel from : channels) {
                to.awaitForMessage("test1-" + from.nodeId() + "-CS-SR");
                to.awaitForMessage("test2-" + from.nodeId() + "-CS-SR");
            }
        }
    }

    @Test
    public void testSendWithConfirmation() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(3, c ->
            c.withInterceptor(new MessageInterceptor<String>() {
                @Override
                public String beforeClientSend(String msg, ClientSendContext sndCtx) {
                    assertSame(OutboundType.SEND_WITH_ACK, sndCtx.type());
                    assertNotNull(sndCtx.receiver());
                    assertNotNull(sndCtx.topology());
                    assertNotNull(sndCtx.channelName());

                    return msg + "-CS";
                }

                @Override
                public void onClientReceiveConfirmation(ClientSendContext sndCtx) {
                    assertSame(OutboundType.SEND_WITH_ACK, sndCtx.type());
                    assertNotNull(sndCtx.receiver());
                    assertNotNull(sndCtx.topology());
                    assertNotNull(sndCtx.channelName());
                }

                @Override
                public String beforeServerReceive(String msg, ServerReceiveContext rcvCtx) {
                    assertSame(OutboundType.SEND_WITH_ACK, rcvCtx.type());
                    assertNotNull(rcvCtx.channelName());
                    assertNotNull(rcvCtx.from());

                    return msg + "-SR";
                }

                @Override
                public void onClientReceiveError(Throwable err, ClientSendContext sndCtx) {
                    throw new UnsupportedOperationException("Unexpected method call.");
                }

                @Override
                public String beforeClientReceiveResponse(String rsp, ClientReceiveContext rcvCtx, ClientSendContext sndCtx) {
                    throw new UnsupportedOperationException("Unexpected method call.");
                }

                @Override
                public String beforeServerSend(String rsp, ServerSendContext sndCtx, ServerReceiveContext rcvCtx) {
                    throw new UnsupportedOperationException("Unexpected method call.");
                }
            })
        );

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                String ack1 = "test1-" + from.nodeId();
                String ack2 = "test2-" + from.nodeId();

                from.get().forNode(to.nodeId()).withConfirmReceive(true).send(ack1).get();
                from.get().forNode(to.nodeId()).withConfirmReceive(true).send(ack2).getUninterruptedly();
            }
        }

        for (TestChannel to : channels) {
            for (TestChannel from : channels) {
                to.awaitForMessage("test1-" + from.nodeId() + "-CS-SR");
                to.awaitForMessage("test2-" + from.nodeId() + "-CS-SR");
            }
        }
    }

    @Test
    public void testSendWithConfirmationFailure() throws Exception {
        @SuppressWarnings("unchecked")
        MessageInterceptor<String> interceptor = mock(MessageInterceptor.class);

        List<TestChannel> channels = createAndJoinChannels(3, c -> {
            c.withReceiver(msg -> {
                throw TEST_ERROR;
            });
            c.withInterceptor(interceptor);
        });

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                MessagingChannel<String> channel = from.get().forNode(to.nodeId()).withConfirmReceive(true);

                MessagingFutureException err = expect(MessagingFutureException.class, () ->
                    get(channel.send("msg"))
                );

                verify(interceptor).onClientReceiveError(ArgumentMatchers.same(err.getCause()), any());
            }
        }
    }

    @Test
    public void testSubscribe() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(3, c -> {
            c.withReceiver(msg -> {
                msg.partialReply(msg.get() + "-reply-part");
                msg.partialReply(msg.get() + "-reply-part");
                msg.reply(msg.get() + "-reply");
            });
            c.withInterceptor(new MessageInterceptor<String>() {
                @Override
                public String beforeClientSend(String msg, ClientSendContext sndCtx) {
                    assertSame(OutboundType.SUBSCRIBE, sndCtx.type());
                    assertNotNull(sndCtx.receiver());
                    assertNotNull(sndCtx.topology());

                    // Store attribute to verify later.
                    sndCtx.setAttribute("test-attr", "test-val");

                    return msg + "-CS-" + sndCtx.hasAffinity() + "-" + sndCtx.affinityKey();
                }

                @Override
                public String beforeClientReceiveResponse(String rsp, ClientReceiveContext rcvCtx, ClientSendContext sndCtx) {
                    assertNotNull(rsp);

                    if (rsp.contains("-part")) {
                        assertSame(InboundType.RESPONSE_CHUNK, rcvCtx.type());
                    } else {
                        assertSame(InboundType.FINAL_RESPONSE, rcvCtx.type());
                    }

                    assertSame(OutboundType.SUBSCRIBE, sndCtx.type());
                    assertNotNull(sndCtx.receiver());
                    assertNotNull(sndCtx.topology());

                    // Verify attribute.
                    assertEquals("test-val", sndCtx.getAttribute("test-attr"));

                    return rsp + "-CR";
                }

                @Override
                public String beforeServerReceive(String msg, ServerReceiveContext rcvCtx) {
                    assertSame(OutboundType.SUBSCRIBE, rcvCtx.type());
                    assertNotNull(rcvCtx.from());

                    // Store attribute to verify later.
                    rcvCtx.setAttribute("test-attr", "test-val");

                    return msg + "-SR";
                }

                @Override
                public String beforeServerSend(String rsp, ServerSendContext sndCtx, ServerReceiveContext rcvCtx) {
                    assertNotNull(rsp);

                    if (rsp.contains("-part")) {
                        assertSame(InboundType.RESPONSE_CHUNK, sndCtx.type());
                    } else {
                        assertSame(InboundType.FINAL_RESPONSE, sndCtx.type());
                    }

                    assertNotNull(rcvCtx.from());
                    assertSame(OutboundType.SUBSCRIBE, rcvCtx.type());

                    // Verify attribute.
                    assertEquals("test-val", rcvCtx.getAttribute("test-attr"));

                    return rsp + "-SS";
                }

                @Override
                public void onClientReceiveConfirmation(ClientSendContext sndCtx) {
                    throw new UnsupportedOperationException("Unexpected method call.");
                }
            });
        });

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                MessagingChannel<String> channel = from.get().forNode(to.nodeId());

                String msg = "test1-" + from.nodeId();

                // No affinity.
                List<String> replies = get(channel.subscribe(msg));

                assertEquals(msg + "-CS-false-null-SR-reply-part-SS-CR", replies.get(0));
                assertEquals(msg + "-CS-false-null-SR-reply-part-SS-CR", replies.get(1));
                assertEquals(msg + "-CS-false-null-SR-reply-SS-CR", replies.get(2));

                to.assertReceived(msg + "-CS-false-null-SR");

                // With affinity.
                List<String> affinityReplies = get(channel.withAffinity(1).subscribe(msg));

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
        MessageInterceptor<String> interceptor = mock(MessageInterceptor.class);

        List<TestChannel> channels = createAndJoinChannels(3, c -> {
            c.withReceiver(msg -> {
                throw TEST_ERROR;
            });
            c.withInterceptor(interceptor);
        });

        for (TestChannel from : channels) {
            for (TestChannel to : channels) {
                MessagingChannel<String> channel = from.get().forNode(to.nodeId());

                MessagingFutureException err = expect(MessagingFutureException.class, () ->
                    get(channel.subscribe("msg"))
                );

                verify(interceptor).onClientReceiveError(ArgumentMatchers.same(err.getCause()), any());
            }
        }
    }
}
