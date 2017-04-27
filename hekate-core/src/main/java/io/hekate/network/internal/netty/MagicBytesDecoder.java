package io.hekate.network.internal.netty;

import io.hekate.core.internal.util.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.net.SocketAddress;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MagicBytesDecoder extends ByteToMessageDecoder {
    private static final Logger log = LoggerFactory.getLogger(MagicBytesDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() >= 4) {
            if (Utils.MAGIC_BYTES == in.readInt()) {
                ctx.pipeline().remove(this);
            } else {
                if (log.isWarnEnabled()) {
                    SocketAddress address = ctx.channel().remoteAddress();

                    log.warn("Rejecting connection from an unknown software [address={}]", address);
                }

                ctx.close();
            }
        }
    }
}
