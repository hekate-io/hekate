package io.hekate.network.internal.netty;

import io.hekate.core.internal.util.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import java.util.List;

@Sharable
final class MagicBytesEncoder extends MessageToMessageEncoder<ByteBuf> {
    public static final MagicBytesEncoder INSTANCE = new MagicBytesEncoder();

    private MagicBytesEncoder() {
        // No-op.
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
        out.add(ctx.alloc().ioBuffer(4, 4).writeInt(Utils.MAGIC_BYTES));

        out.add(msg.retain());

        ctx.pipeline().remove(this);
    }
}
