package io.hekate.rpc.internal;

import io.hekate.codec.Codec;
import io.hekate.codec.CodecFactory;
import io.hekate.util.format.ToString;

class RpcProtocolCodecFactory implements CodecFactory<RpcProtocol> {
    private final CodecFactory<Object> delegate;

    public RpcProtocolCodecFactory(CodecFactory<Object> delegate) {
        assert delegate != null : "Delegate is null.";

        this.delegate = delegate;
    }

    @Override
    public Codec<RpcProtocol> createCodec() {
        return new RpcProtocolCodec(delegate.createCodec());
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
