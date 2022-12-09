package io.hekate.trace.zipkin;

import brave.Tracing;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.plugin.Plugin;
import io.hekate.messaging.MessagingServiceFactory;

public class HekateZipkinPlugin implements Plugin {
    private final Tracing tracing;

    public HekateZipkinPlugin(Tracing tracing) {
        ArgAssert.notNull(tracing, "Tracing");

        this.tracing = tracing;
    }

    @Override
    public void install(HekateBootstrap boot) {
        boot.withService(MessagingServiceFactory.class, msg ->
            msg.withGlobalInterceptor(new ZipkinMessageInterceptor(tracing))
        );
    }

    @Override
    public void start(Hekate hekate) throws HekateException {
        // No-op.
    }

    @Override
    public void stop() throws HekateException {
        // No-op.
    }
}
