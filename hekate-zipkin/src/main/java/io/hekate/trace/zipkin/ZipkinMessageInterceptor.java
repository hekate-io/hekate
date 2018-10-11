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
import io.hekate.messaging.intercept.ClientOutboundContext;
import io.hekate.messaging.intercept.ClientReceiveContext;
import io.hekate.messaging.intercept.ClientSendContext;
import io.hekate.messaging.intercept.ServerInboundContext;
import io.hekate.messaging.intercept.ServerReceiveContext;
import io.hekate.messaging.intercept.ServerSendContext;
import io.hekate.util.format.ToString;
import io.hekate.util.trace.TraceInfo;

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

    private final Injector<ClientSendContext<Object>> injector;

    private final Extractor<ServerReceiveContext<?>> extractor;

    public ZipkinMessageInterceptor(Tracing tracing) {
        tracer = tracing.tracer();

        // Propagate spans via messages' meta-data.
        Propagation<MessageMetaData.Key<String>> propagation = tracing.propagationFactory().create(name ->
            MessageMetaData.Key.of(name, MetaDataCodec.TEXT)
        );

        // Client-side meta-data injector.
        this.injector = propagation.injector((ctx, key, value) ->
            ctx.metaData().set(key, value)
        );

        // Server-side meta-data extractor.
        this.extractor = propagation.extractor((ctx, key) ->
            ctx.readMetaData().map(it -> it.get(key)).orElse(null)
        );
    }

    @Override
    public void interceptClientSend(ClientSendContext<Object> ctx) {
        // Try to get a previous span in case if this is a failover attempt.
        Span prevAttempt = (Span)ctx.getAttribute(SPAN_ATTRIBUTE);

        Span span = tryJoinOrStartNew(prevAttempt);

        if (!span.isNoop()) {
            recordRemoteAddress(ctx.receiver().address(), span);

            switch (ctx.type()) {
                case REQUEST: {
                    annotate("request", span, ctx.get(), ctx.channelName());

                    ctx.setAttribute(SPAN_ATTRIBUTE, span.kind(CLIENT).start());

                    break;
                }
                case SUBSCRIBE: {
                    annotate("subscribe", span, ctx.get(), ctx.channelName());

                    ctx.setAttribute(SPAN_ATTRIBUTE, span.kind(CLIENT).start());

                    break;
                }
                case SEND_WITH_ACK: {
                    annotate("send-with-ack", span, ctx.get(), ctx.channelName());

                    ctx.setAttribute(SPAN_ATTRIBUTE, span.kind(CLIENT).start());

                    break;
                }
                case SEND_NO_ACK: {
                    annotate("send-no-ack", span, ctx.get(), ctx.channelName());

                    // Immediately finish as we don't expected any feedback from the server.
                    span.kind(PRODUCER).start().flush();

                    break;
                }
                default: {
                    throw new IllegalArgumentException("Unexpected type: " + ctx.type());
                }
            }
        }

        // Inject the span into the message's meta-data.
        injector.inject(span.context(), ctx);
    }

    @Override
    public void interceptClientReceiveResponse(ClientReceiveContext ctx) {
        Span span = (Span)ctx.outboundContext().getAttribute(SPAN_ATTRIBUTE);

        if (span != null) {
            switch (ctx.type()) {
                case RESPONSE_CHUNK: {
                    Span chunkSpan = tracer.newChild(span.context());

                    annotate("receive-chunk", chunkSpan, ctx.get(), null)
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
                    throw new IllegalArgumentException("Unexpected type: " + ctx.type());
                }
            }
        }
    }

    @Override
    public void interceptClientReceiveConfirmation(ClientOutboundContext ctx) {
        Span span = (Span)ctx.getAttribute(SPAN_ATTRIBUTE);

        if (span != null) {
            span.finish();
        }
    }

    @Override
    public void interceptClientReceiveError(ClientOutboundContext ctx, Throwable err) {
        Span span = (Span)ctx.getAttribute(SPAN_ATTRIBUTE);

        if (span != null) {
            span.error(err).finish();
        }
    }

    @Override
    public void interceptServerReceive(ServerReceiveContext ctx) {
        // Extract from the message's meta-data.
        Span span = tracer.nextSpan(extractor.extract(ctx));

        // Put span into the scope.
        ctx.setAttribute(SCOPE_ATTRIBUTE, tracer.withSpanInScope(span));

        if (!span.isNoop()) {
            recordRemoteAddress(ctx.from(), span);

            switch (ctx.type()) {
                case SEND_WITH_ACK: {
                    span.kind(SERVER);

                    annotate("receive", span, ctx.get(), ctx.channelName());

                    break;
                }
                case SEND_NO_ACK: {
                    span.kind(CONSUMER);

                    annotate("receive", span, ctx.get(), ctx.channelName());

                    break;
                }
                case REQUEST:
                case SUBSCRIBE: {
                    span.kind(SERVER);

                    // No need to annotate here (will annotate with the server's response later).

                    break;
                }
                default: {
                    throw new IllegalArgumentException("Unexpected type: " + ctx.type());
                }
            }

            span.start();

            // Store span in the context.
            ctx.setAttribute(SPAN_ATTRIBUTE, span);
        }
    }

    @Override
    public void interceptServerReceiveComplete(ServerInboundContext ctx) {
        // Cleanup scope.
        Tracer.SpanInScope scope = (Tracer.SpanInScope)ctx.getAttribute(SCOPE_ATTRIBUTE);

        if (scope != null) {
            scope.close();
        }

        // Finish the span if it represents a send operation.
        Span span = (Span)ctx.getAttribute(SPAN_ATTRIBUTE);

        if (span != null && (ctx.type() == SEND_NO_ACK || ctx.type() == SEND_WITH_ACK)) {
            span.finish();
        }
    }

    @Override
    public void interceptServerSend(ServerSendContext ctx) {
        Span span = (Span)ctx.inboundContext().getAttribute(SPAN_ATTRIBUTE);

        if (span != null) {
            switch (ctx.type()) {
                case RESPONSE_CHUNK: {
                    Span chunkSpan = tracer.newChild(span.context());

                    annotate("reply-chunk", chunkSpan, ctx.get(), null)
                        .kind(PRODUCER)
                        .start()
                        .flush();

                    break;
                }
                case FINAL_RESPONSE: {
                    annotate("reply", span, ctx.get(), null);

                    span.finish();

                    break;
                }
                default: {
                    throw new IllegalArgumentException("Unexpected type: " + ctx.type());
                }
            }
        }
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
