package io.hekate.network.internal.netty;

import io.hekate.core.internal.util.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import java.util.List;

@Sharable
final class NetworkVersionEncoder extends MessageToMessageEncoder<ByteBuf> {
    public static final NetworkVersionEncoder INSTANCE = new NetworkVersionEncoder();

    private NetworkVersionEncoder() {
        // No-op.
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
        int bufSize = Integer.BYTES * 2;

        out.add(ctx.alloc().ioBuffer(bufSize, bufSize)
            .writeInt(Utils.MAGIC_BYTES)
            .writeInt(NetworkProtocol.VERSION)
        );

        out.add(msg.retain());

        ctx.pipeline().remove(this);
    }
}
