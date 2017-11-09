package io.hekate.codec.kryo;

import io.hekate.HekateTestBase;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class KryoSerializersRegistrarTest extends HekateTestBase {
    @Test
    public void testSupported() {
        assertTrue(KryoSerializersRegistrar.isSupported());
    }
}
