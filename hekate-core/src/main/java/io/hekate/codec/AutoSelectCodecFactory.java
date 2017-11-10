package io.hekate.codec;

import io.hekate.codec.fst.FstCodecFactory;
import io.hekate.codec.kryo.KryoCodecFactory;
import io.hekate.util.format.ToString;

/**
 * Codec factory that automatically selects an underlying implementation based on what is available on the classpath.
 *
 * <p>
 * This factory uses the following selection algorithm:
 * </p>
 * <ul>
 * <li>If Kryo is {@link KryoCodecFactory#isAvailable() available} then {@link KryoCodecFactory} will be used</li>
 * <li>If FST is {@link FstCodecFactory#isAvailable() available} then {@link FstCodecFactory} will be used</li>
 * <li>otherwise {@link JdkCodecFactory} will be used</li>
 * </ul>
 *
 * @param <T> Base data type that is supported by the {@link Codec}.
 */
public class AutoSelectCodecFactory<T> implements CodecFactory<T> {
    private final CodecFactory<T> factory;

    /**
     * Constructs a new instance.
     */
    public AutoSelectCodecFactory() {
        if (KryoCodecFactory.isAvailable()) {
            factory = new KryoCodecFactory<>();
        } else if (FstCodecFactory.isAvailable()) {
            factory = new FstCodecFactory<>();
        } else {
            factory = new JdkCodecFactory<>();
        }
    }

    /**
     * Returns the selected factory.
     *
     * @return Selected factory.
     */
    public CodecFactory<T> selected() {
        return factory;
    }

    @Override
    public Codec<T> createCodec() {
        return factory.createCodec();
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
