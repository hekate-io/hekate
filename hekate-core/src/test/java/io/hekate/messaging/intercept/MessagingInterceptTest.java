package io.hekate.messaging.intercept;

import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.internal.MessagingServiceTestBase;
import io.hekate.messaging.internal.TestChannel;
import java.util.List;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MessagingInterceptTest extends MessagingServiceTestBase {
    public MessagingInterceptTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testRequest() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(3, c -> {
            c.withReceiver(msg -> msg.reply(msg.get() + "-reply"));
            c.withInterceptor(new MessageInterceptor<String>() {
                @Override
                public String interceptClientSend(String msg, ClientSendContext<String> sndCtx) {
                    assertSame(RequestType.REQUEST, sndCtx.type());
                    assertNotNull(sndCtx.receiver());
                    assertNotNull(sndCtx.topology());
                    assertEquals(msg, sndCtx.message());

                    // Store attribute to verify later.
                    sndCtx.setAttribute("test-attr", msg);

                    return msg + "-CS-" + sndCtx.hasAffinity() + "-" + sndCtx.affinityKey();
                }

                @Override
                public String interceptClientReceive(String rsp, ResponseContext<String> rspCtx, ClientSendContext<String> sndCtx) {
                    assertNotNull(rsp);
                    assertEquals(rsp, rspCtx.message());
                    assertSame(ResponseType.FINAL_RESPONSE, rspCtx.type());

                    assertSame(RequestType.REQUEST, sndCtx.type());
                    assertNotNull(sndCtx.receiver());
                    assertNotNull(sndCtx.topology());
                    assertNotNull(sndCtx.message());

                    // Verify attribute.
                    assertEquals(sndCtx.message(), sndCtx.getAttribute("test-attr"));

                    return rsp + "-CR";
                }

                @Override
                public String interceptServerReceive(String msg, ServerReceiveContext<String> rcvCtx) {
                    assertSame(RequestType.REQUEST, rcvCtx.type());
                    assertNotNull(rcvCtx.endpoint());
                    assertEquals(msg, rcvCtx.message());

                    return msg + "-SR";
                }

                @Override
                public String interceptServerSend(String rsp, ResponseContext<String> rspCtx, ServerReceiveContext<String> rcvCtx) {
                    assertNotNull(rsp);
                    assertEquals(rsp, rspCtx.message());
                    assertSame(ResponseType.FINAL_RESPONSE, rspCtx.type());

                    assertSame(RequestType.REQUEST, rcvCtx.type());
                    assertNotNull(rcvCtx.endpoint());
                    assertNotEquals(rsp, rcvCtx.message());

                    return rsp + "-SS";
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

                verify(interceptor).interceptClientReceiveError(ArgumentMatchers.same(err.getCause()), any());
            }
        }
    }

    @Test
    public void testSend() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(3, c ->
            c.withInterceptor(new MessageInterceptor<String>() {
                @Override
                public String interceptClientSend(String msg, ClientSendContext<String> sndCtx) {
                    assertSame(RequestType.SEND_NO_ACK, sndCtx.type());
                    assertNotNull(sndCtx.receiver());
                    assertNotNull(sndCtx.topology());
                    assertEquals(msg, sndCtx.message());

                    return msg + "-CS";
                }

                @Override
                public String interceptServerReceive(String msg, ServerReceiveContext<String> rcvCtx) {
                    assertSame(RequestType.SEND_NO_ACK, rcvCtx.type());
                    assertNotNull(rcvCtx.endpoint());
                    assertEquals(msg, rcvCtx.message());

                    return msg + "-SR";
                }

                @Override
                public String interceptClientReceive(String rsp, ResponseContext<String> rspCtx, ClientSendContext<String> sndCtx) {
                    throw new UnsupportedOperationException("Unexpected method call.");
                }

                @Override
                public void interceptClientReceiveError(Throwable err, ClientSendContext<String> sndCtx) {
                    throw new UnsupportedOperationException("Unexpected method call.");
                }

                @Override
                public String interceptServerSend(String rsp, ResponseContext<String> rspCtx, ServerReceiveContext<String> rcvCtx) {
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
                public String interceptClientSend(String msg, ClientSendContext<String> sndCtx) {
                    assertSame(RequestType.SEND_WITH_ACK, sndCtx.type());
                    assertNotNull(sndCtx.receiver());
                    assertNotNull(sndCtx.topology());
                    assertEquals(msg, sndCtx.message());

                    return msg + "-CS";
                }

                @Override
                public String interceptServerReceive(String msg, ServerReceiveContext<String> rcvCtx) {
                    assertSame(RequestType.SEND_WITH_ACK, rcvCtx.type());
                    assertNotNull(rcvCtx.endpoint());
                    assertEquals(msg, rcvCtx.message());

                    return msg + "-SR";
                }

                @Override
                public void interceptClientReceiveError(Throwable err, ClientSendContext<String> sndCtx) {
                    throw new UnsupportedOperationException("Unexpected method call.");
                }

                @Override
                public String interceptClientReceive(String rsp, ResponseContext<String> rspCtx, ClientSendContext<String> sndCtx) {
                    throw new UnsupportedOperationException("Unexpected method call.");
                }

                @Override
                public String interceptServerSend(String rsp, ResponseContext<String> rspCtx, ServerReceiveContext<String> rcvCtx) {
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

                verify(interceptor).interceptClientReceiveError(ArgumentMatchers.same(err.getCause()), any());
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
                public String interceptClientSend(String msg, ClientSendContext<String> sndCtx) {
                    assertSame(RequestType.SUBSCRIBE, sndCtx.type());
                    assertNotNull(sndCtx.receiver());
                    assertNotNull(sndCtx.topology());
                    assertEquals(msg, sndCtx.message());

                    // Store attribute to verify later.
                    sndCtx.setAttribute("test-attr", msg);

                    return msg + "-CS-" + sndCtx.hasAffinity() + "-" + sndCtx.affinityKey();
                }

                @Override
                public String interceptClientReceive(String rsp, ResponseContext<String> rspCtx, ClientSendContext<String> sndCtx) {
                    assertNotNull(rsp);
                    assertEquals(rsp, rspCtx.message());

                    if (rsp.contains("-part")) {
                        assertSame(ResponseType.RESPONSE_CHUNK, rspCtx.type());
                    } else {
                        assertSame(ResponseType.FINAL_RESPONSE, rspCtx.type());
                    }

                    assertSame(RequestType.SUBSCRIBE, sndCtx.type());
                    assertNotNull(sndCtx.receiver());
                    assertNotNull(sndCtx.topology());
                    assertNotEquals(rsp, sndCtx.message());

                    // Verify attribute.
                    assertEquals(sndCtx.message(), sndCtx.getAttribute("test-attr"));

                    return rsp + "-CR";
                }

                @Override
                public String interceptServerReceive(String msg, ServerReceiveContext<String> rcvCtx) {
                    assertSame(RequestType.SUBSCRIBE, rcvCtx.type());
                    assertNotNull(rcvCtx.endpoint());
                    assertEquals(msg, rcvCtx.message());

                    return msg + "-SR";
                }

                @Override
                public String interceptServerSend(String rsp, ResponseContext<String> rspCtx, ServerReceiveContext<String> rcvCtx) {
                    assertNotNull(rsp);
                    assertEquals(rsp, rspCtx.message());

                    if (rsp.contains("-part")) {
                        assertSame(ResponseType.RESPONSE_CHUNK, rspCtx.type());
                    } else {
                        assertSame(ResponseType.FINAL_RESPONSE, rspCtx.type());
                    }

                    assertNotNull(rcvCtx.endpoint());
                    assertSame(RequestType.SUBSCRIBE, rcvCtx.type());

                    return rsp + "-SS";
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

                verify(interceptor).interceptClientReceiveError(ArgumentMatchers.same(err.getCause()), any());
            }
        }
    }
}
