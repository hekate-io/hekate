package io.hekate.trace.zipkin;

import brave.Span;
import brave.Tracing;
import brave.propagation.Propagation;
import brave.propagation.TraceContext;
import io.hekate.cluster.ClusterAddress;
import io.hekate.messaging.MessageMetaData;
import io.hekate.messaging.intercept.ClientSendContext;
import io.hekate.messaging.intercept.InboundType;
import io.hekate.messaging.intercept.MessageInterceptor;
import io.hekate.messaging.intercept.OutboundType;
import io.hekate.messaging.intercept.ResponseContext;
import io.hekate.messaging.intercept.ServerReceiveContext;
import io.hekate.util.format.ToString;

class ZipkinMessageInterceptor implements MessageInterceptor<Object> {
    private static final String SPAN_ATTRIBUTE = "span";

    private final Tracing tracing;

    private TraceContext.Injector<ClientSendContext> injector;

    private TraceContext.Extractor<ServerReceiveContext> extractor;

    public ZipkinMessageInterceptor(Tracing tracing) {
        this.tracing = tracing;

        // Propagate spans via messages' meta-data.
        Propagation<MessageMetaData.Key<String>> propagation = tracing.propagationFactory().create(name ->
            MessageMetaData.Key.of(name, MessageMetaData.MetaDataCodec.TEXT)
        );

        this.injector = propagation.injector((sndCtx, key, value) -> {
            sndCtx.metaData().set(key, value);
        });

        this.extractor = propagation.extractor((rcvCtx, key) -> {
            if (rcvCtx.metaData().isPresent()) {
                return rcvCtx.metaData().get().get(key);
            } else {
                return null;
            }
        });
    }

    @Override
    public Object interceptClientSend(Object msg, ClientSendContext sndCtx) {
        Span span = tracing.tracer().nextSpan();

        span.name(nameOf(sndCtx.channelName(), msg));

        recordRemoteAddress(sndCtx.receiver().address(), span);

        if (sndCtx.type() == OutboundType.SEND_NO_ACK) {
            // Acknowledgement is not expected.
            span.kind(Span.Kind.PRODUCER).start().flush();
        } else {
            // Wait for a response or for an acknowledgement.
            span.kind(Span.Kind.CLIENT).start();

            // Store span in the context.
            sndCtx.setAttribute(SPAN_ATTRIBUTE, span);
        }

        // Inject the span into the message's meta-data.
        injector.inject(span.context(), sndCtx);

        return null;
    }

    @Override
    public Object interceptClientReceive(Object rsp, ResponseContext rspCtx, ClientSendContext sndCtx) {
        Span span = (Span)sndCtx.getAttribute(SPAN_ATTRIBUTE);

        if (span != null) {
            if (rspCtx.type() == InboundType.FINAL_RESPONSE) {
                // No more responses are expected for this span.
                span.finish();
            } else {
                // Record a new child span, but do not finish the parent one as we expected for more responses to come.
                tracing.tracer().newChild(span.context())
                    .kind(Span.Kind.CONSUMER)
                    .name(nameOf(sndCtx.channelName(), rsp))
                    .start()
                    .flush();
            }
        }

        return null;
    }

    @Override
    public void interceptClientReceiveError(Throwable err, ClientSendContext sndCtx) {
        Span span = (Span)sndCtx.getAttribute(SPAN_ATTRIBUTE);

        if (span != null) {
            span.error(err).finish();
        }
    }

    @Override
    public Object interceptServerReceive(Object msg, ServerReceiveContext rcvCtx) {
        // Extract a span from the message's meta-data.
        Span span = tracing.tracer().nextSpan(extractor.extract(rcvCtx));

        recordRemoteAddress(rcvCtx.from(), span);

        // Put span into the thread's scope.
        tracing.tracer().withSpanInScope(span);

        if (rcvCtx.type() == OutboundType.SEND_NO_ACK) {
            // Acknowledgement is not expected.
            span.name(nameOf(rcvCtx.channelName(), msg)).kind(Span.Kind.CONSUMER).start().flush();
        } else {
            // Keep span active as we plan to send a response or an acknowledgement.
            span.kind(Span.Kind.SERVER).start();

            // Store span in the context.
            rcvCtx.setAttribute(SPAN_ATTRIBUTE, span);
        }

        return null;
    }

    @Override
    public Object interceptServerSend(Object rsp, ResponseContext rspCtx, ServerReceiveContext rcvCtx) {
        Span span = (Span)rcvCtx.getAttribute(SPAN_ATTRIBUTE);

        if (span != null) {
            if (rspCtx.type() == InboundType.FINAL_RESPONSE) {
                // No more responses are expected for this span.
                span.name(nameOf(rcvCtx.channelName(), rsp)).finish();
            } else {
                // Record a new child span, but do not finish the parent one as we expect to send more responses.
                tracing.tracer().newChild(span.context())
                    .kind(Span.Kind.PRODUCER)
                    .name(nameOf(rcvCtx.channelName(), rsp))
                    .start()
                    .flush();
            }
        }

        return null;
    }

    private static void recordRemoteAddress(ClusterAddress addr, Span span) {
        span.remoteIpAndPort(addr.socket().getAddress().getHostAddress(), addr.socket().getPort());
    }

    private static String nameOf(String channel, Object msg) {
        return '/' + channel + '/' + msg.getClass().getSimpleName();
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
