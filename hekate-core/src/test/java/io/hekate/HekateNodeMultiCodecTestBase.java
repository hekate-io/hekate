package io.hekate;

import io.hekate.codec.CodecFactory;
import io.hekate.codec.JdkCodecFactory;
import io.hekate.codec.fst.FstCodecFactory;
import io.hekate.codec.kryo.KryoCodecFactory;
import java.util.Collection;
import java.util.stream.Stream;
import org.junit.runners.Parameterized.Parameters;

public abstract class HekateNodeMultiCodecTestBase extends HekateNodeParamTestBase {
    public static class MultiCodecTestContext extends HekateTestContext {
        private final CodecFactory<Object> codecFactory;

        public MultiCodecTestContext(HekateTestContext src, CodecFactory<Object> codecFactory) {
            super(src);

            this.codecFactory = codecFactory;
        }

        public CodecFactory<Object> codecFactory() {
            return codecFactory;
        }
    }

    private final MultiCodecTestContext ctx;

    public HekateNodeMultiCodecTestBase(MultiCodecTestContext ctx) {
        super(ctx);

        this.ctx = ctx;
    }

    @Parameters(name = "{index}: {0}")
    public static Collection<MultiCodecTestContext> getLockAsyncTestContexts() {
        return mapTestContext(p -> Stream.of(
            new MultiCodecTestContext(p, new KryoCodecFactory<>()),
            new MultiCodecTestContext(p, new FstCodecFactory<>()),
            new MultiCodecTestContext(p, new JdkCodecFactory<>())
        ));
    }

    @Override
    protected CodecFactory<Object> defaultCodec() {
        return ctx.codecFactory();
    }
}
