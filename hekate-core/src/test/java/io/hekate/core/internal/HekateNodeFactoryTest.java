package io.hekate.core.internal;

import io.hekate.HekateTestBase;
import io.hekate.core.HekateBootstrap;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class HekateNodeFactoryTest extends HekateTestBase {
    @Test
    public void test() throws Exception {
        assertValidUtilityClass(HekateNodeFactory.class);

        assertNotNull(HekateNodeFactory.create(new HekateBootstrap()));
    }
}
