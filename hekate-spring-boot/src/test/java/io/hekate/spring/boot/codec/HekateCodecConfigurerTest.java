package io.hekate.spring.boot.codec;

import io.hekate.cluster.internal.DefaultClusterNode;
import io.hekate.codec.AutoSelectCodecFactory;
import io.hekate.codec.JdkCodecFactory;
import io.hekate.codec.fst.FstCodecFactory;
import io.hekate.codec.kryo.KryoCodecFactory;
import io.hekate.core.HekateBootstrap;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import org.junit.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import static org.junit.Assert.assertEquals;

public class HekateCodecConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableAutoConfiguration
    public static class TestConfig extends HekateTestConfigBase {
        // No-op.
    }

    @Test
    public void testDefault() throws Exception {
        registerAndRefresh(TestConfig.class);

        assertEquals(AutoSelectCodecFactory.class, get(HekateBootstrap.class).getDefaultCodec().getClass());
    }

    @Test
    public void testKryo() throws Exception {
        registerAndRefresh(new String[]{
            "hekate.codec=kryo",
            "hekate.codec.kryo.knownTypes.100500=" + DefaultClusterNode.class.getName()
        }, TestConfig.class);

        KryoCodecFactory<Object> codec = (KryoCodecFactory<Object>)get(HekateBootstrap.class).getDefaultCodec();

        assertEquals(DefaultClusterNode.class, codec.getKnownTypes().get(100500));
    }

    @Test
    public void testFst() throws Exception {
        registerAndRefresh(new String[]{
            "hekate.codec=fst",
            "hekate.codec.fst.knownTypes.100500=" + DefaultClusterNode.class.getName(),
        }, TestConfig.class);

        FstCodecFactory<Object> codec = (FstCodecFactory<Object>)get(HekateBootstrap.class).getDefaultCodec();

        assertEquals(DefaultClusterNode.class, codec.getKnownTypes().get(100500));
    }

    @Test
    public void testJdk() throws Exception {
        registerAndRefresh(new String[]{
            "hekate.codec=jdk",
        }, TestConfig.class);

        assertEquals(JdkCodecFactory.class, get(HekateBootstrap.class).getDefaultCodec().getClass());
    }
}
