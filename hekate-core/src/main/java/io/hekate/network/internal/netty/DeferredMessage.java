package io.hekate.network.internal.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.DefaultChannelPromise;

class DeferredMessage extends DefaultChannelPromise {
    private final Object payload;

    private final Object source;

    public DeferredMessage(Object payload, Object source, Channel channel) {
        super(channel);

        this.payload = payload;
        this.source = source;
    }

    public Object encoded() {
        return payload;
    }

    public Object source() {
        return source;
    }

    public boolean isPreEncoded() {
        return payload instanceof ByteBuf;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[message=" + source + "]";
    }
}
