package io.hekate.trace.zipkin;

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import brave.propagation.Propagation;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContext.Injector;
import io.hekate.cluster.ClusterAddress;
import io.hekate.messaging.MessageMetaData;
import io.hekate.messaging.MessageMetaData.MetaDataCodec;
import io.hekate.messaging.intercept.AllMessageInterceptor;
import io.hekate.messaging.intercept.ClientReceiveContext;
import io.hekate.messaging.intercept.ClientSendContext;
import io.hekate.messaging.intercept.ServerReceiveContext;
import io.hekate.messaging.intercept.ServerSendContext;
import io.hekate.util.format.ToString;
import io.hekate.util.trace.TraceInfo;
import java.util.Optional;

import static brave.Span.Kind.CLIENT;
import static brave.Span.Kind.CONSUMER;
import static brave.Span.Kind.PRODUCER;
import static brave.Span.Kind.SERVER;
import static io.hekate.messaging.intercept.OutboundType.SEND_NO_ACK;
import static io.hekate.messaging.intercept.OutboundType.SEND_WITH_ACK;

class ZipkinMessageInterceptor implements AllMessageInterceptor<Object> {
    private static final String SPAN_ATTRIBUTE = "zipkin-span";

    private static final String SCOPE_ATTRIBUTE = "zipkin-scope";

    private final Tracer tracer;

    private final Injector<ClientSendContext> injector;

    private final Extractor<ServerReceiveContext> extractor;

    public ZipkinMessageInterceptor(Tracing tracing) {
        tracer = tracing.tracer();

        // Propagate spans via messages' meta-data.
        Propagation<MessageMetaData.Key<String>> propagation = tracing.propagationFactory().create(name ->
            MessageMetaData.Key.of(name, MetaDataCodec.TEXT)
        );

        // Client-side meta-data injector.
        this.injector = propagation.injector((sndCtx, key, value) ->
            sndCtx.metaData().set(key, value)
        );

        // Server-side meta-data extractor.
        this.extractor = propagation.extractor((rcvCtx, key) -> {
            Optional<MessageMetaData> metaData = rcvCtx.readMetaData();

            if (metaData.isPresent()) {
                return metaData.get().get(key);
            } else {
                return null;
            }
        });
    }

    @Override
    public Object beforeClientSend(Object msg, ClientSendContext sndCtx) {
        // Try to get a previous span in case if this is a failover attempt.
        Span prevAttempt = (Span)sndCtx.getAttribute(SPAN_ATTRIBUTE);

        Span span = tryJoinOrStartNew(prevAttempt);

        if (!span.isNoop()) {
            recordRemoteAddress(sndCtx.receiver().address(), span);

            switch (sndCtx.type()) {
                case REQUEST: {
                    annotate("request", span, msg, sndCtx.channelName());

                    sndCtx.setAttribute(SPAN_ATTRIBUTE, span.kind(CLIENT).start());

                    break;
                }
                case SUBSCRIBE: {
                    annotate("subscribe", span, msg, sndCtx.channelName());

                    sndCtx.setAttribute(SPAN_ATTRIBUTE, span.kind(CLIENT).start());

                    break;
                }
                case SEND_WITH_ACK: {
                    annotate("send-with-ack", span, msg, sndCtx.channelName());

                    sndCtx.setAttribute(SPAN_ATTRIBUTE, span.kind(CLIENT).start());

                    break;
                }
                case SEND_NO_ACK: {
                    annotate("send-no-ack", span, msg, sndCtx.channelName());

                    // Immediately finish as we don't expected any feedback from the server.
                    span.kind(PRODUCER).start().flush();

                    break;
                }
                default: {
                    throw new IllegalArgumentException("Unexpected type: " + sndCtx.type());
                }
            }
        }

        // Inject the span into the message's meta-data.
        injector.inject(span.context(), sndCtx);

        return null;
    }

    @Override
    public Object beforeClientReceiveResponse(Object rsp, ClientReceiveContext rcvCtx, ClientSendContext sndCtx) {
        Span span = (Span)sndCtx.getAttribute(SPAN_ATTRIBUTE);

        if (span != null) {
            switch (rcvCtx.type()) {
                case RESPONSE_CHUNK: {
                    Span chunkSpan = tracer.newChild(span.context());

                    annotate("receive-chunk", chunkSpan, rsp, null)
                        .kind(CONSUMER)
                        .start()
                        .flush();

                    break;
                }
                case FINAL_RESPONSE: {
                    span.finish();

                    break;
                }
                default: {
                    throw new IllegalArgumentException("Unexpected type: " + rcvCtx.type());
                }
            }
        }

        return null;
    }

    @Override
    public void onClientReceiveConfirmation(ClientSendContext sndCtx) {
        Span span = (Span)sndCtx.getAttribute(SPAN_ATTRIBUTE);

        if (span != null) {
            span.finish();
        }
    }

    @Override
    public void onClientReceiveError(Throwable err, ClientSendContext sndCtx) {
        Span span = (Span)sndCtx.getAttribute(SPAN_ATTRIBUTE);

        if (span != null) {
            span.error(err).finish();
        }
    }

    @Override
    public Object beforeServerReceive(Object msg, ServerReceiveContext rcvCtx) {
        // Extract from the message's meta-data.
        Span span = tracer.nextSpan(extractor.extract(rcvCtx));

        // Put span into the scope.
        rcvCtx.setAttribute(SCOPE_ATTRIBUTE, tracer.withSpanInScope(span));

        if (!span.isNoop()) {
            recordRemoteAddress(rcvCtx.from(), span);

            switch (rcvCtx.type()) {
                case SEND_WITH_ACK: {
                    span.kind(SERVER);

                    annotate("receive", span, msg, rcvCtx.channelName());

                    break;
                }
                case SEND_NO_ACK: {
                    span.kind(CONSUMER);

                    annotate("receive", span, msg, rcvCtx.channelName());

                    break;
                }
                case REQUEST:
                case SUBSCRIBE: {
                    span.kind(SERVER);

                    // No need to annotate here (will annotate with the server's response later).

                    break;
                }
                default: {
                    throw new IllegalArgumentException("Unexpected type: " + rcvCtx.type());
                }
            }

            span.start();

            // Store span in the context.
            rcvCtx.setAttribute(SPAN_ATTRIBUTE, span);
        }

        return null;
    }

    @Override
    public void onServerReceiveComplete(Object msg, ServerReceiveContext rcvCtx) {
        // Cleanup scope.
        Tracer.SpanInScope scope = (Tracer.SpanInScope)rcvCtx.getAttribute(SCOPE_ATTRIBUTE);

        if (scope != null) {
            scope.close();
        }

        // Finish the span if it represents a send operation.
        Span span = (Span)rcvCtx.getAttribute(SPAN_ATTRIBUTE);

        if (span != null && (rcvCtx.type() == SEND_NO_ACK || rcvCtx.type() == SEND_WITH_ACK)) {
            span.finish();
        }
    }

    @Override
    public Object beforeServerSend(Object rsp, ServerSendContext sndCtx, ServerReceiveContext rcvCtx) {
        Span span = (Span)rcvCtx.getAttribute(SPAN_ATTRIBUTE);

        if (span != null) {
            switch (sndCtx.type()) {
                case RESPONSE_CHUNK: {
                    Span chunkSpan = tracer.newChild(span.context());

                    annotate("reply-chunk", chunkSpan, rsp, null)
                        .kind(PRODUCER)
                        .start()
                        .flush();

                    break;
                }
                case FINAL_RESPONSE: {
                    annotate("reply", span, rsp, null);

                    span.finish();

                    break;
                }
                default: {
                    throw new IllegalArgumentException("Unexpected type: " + rcvCtx.type());
                }
            }
        }

        return null;
    }

    private Span tryJoinOrStartNew(Span prevAttempt) {
        if (prevAttempt == null) {
            // Start a new span.
            return tracer.nextSpan();
        } else {
            // Join the previous attempt.
            return tracer.newChild(prevAttempt.context());
        }
    }

    private static Span annotate(String action, Span span, Object msg, String channel) {
        StringBuilder name = new StringBuilder(action);

        if (channel == null) {
            name.append(": ");
        } else {
            name.append(": /").append(channel).append('/');
        }

        TraceInfo info = TraceInfo.extract(msg);

        if (info == null) {
            span.name(name.append(msg.getClass().getSimpleName()).toString());
        } else {
            span.name(name.append(info.name()).toString());

            if (info.tags() != null && !info.tags().isEmpty()) {
                info.tags().forEach((tag, value) ->
                    span.tag(tag, String.valueOf(value))
                );
            }
        }

        return span;
    }

    private static void recordRemoteAddress(ClusterAddress addr, Span span) {
        span.remoteIpAndPort(addr.host(), addr.port());
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
