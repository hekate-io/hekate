package io.hekate.codec;

import java.util.Collection;
import java.util.Collections;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertFalse;

@RunWith(Parameterized.class)
public class JdkCodecTest extends CodecTestBase<JdkCodecFactory<Object>> {
    public JdkCodecTest(JdkCodecFactory<Object> factory) {
        super(factory);
    }

    @Parameters(name = "{index}: factory={0}")
    public static Collection<Object[]> getParams() {
        return Collections.singletonList(new Object[]{new JdkCodecFactory<>()});
    }

    @Test
    public void testStateless() {
        assertFalse(factory.createCodec().isStateful());
    }
}
