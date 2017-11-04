package io.hekate.network.netty;

import io.hekate.core.internal.util.ErrorUtils;
import io.netty.handler.codec.DecoderException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import javax.net.ssl.SSLException;

final class NettyErrorUtils {
    private NettyErrorUtils() {
        // No-op.
    }

    public static Throwable unwrap(Throwable err) {
        if (err instanceof DecoderException && err.getCause() instanceof SSLException) {
            return err.getCause();
        }

        return err;
    }

    public static boolean isNonFatalIoError(Throwable err) {
        return err instanceof IOException
            && !ErrorUtils.isCausedBy(GeneralSecurityException.class, err);
    }
}
