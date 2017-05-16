package io.hekate.network.internal.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.DefaultChannelPromise;

class DeferredMessage extends DefaultChannelPromise {
    private final Object payload;

    private final Object info;

    public DeferredMessage(Object payload, Object info, Channel channel) {
        super(channel);

        this.payload = payload;
        this.info = info;
    }

    public Object payload() {
        return payload;
    }

    public Object info() {
        return info;
    }

    public boolean isPreEncoded() {
        return payload instanceof ByteBuf;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[message=" + info + "]";
    }
}
