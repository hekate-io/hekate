package io.hekate.rpc.internal;

import io.hekate.HekateTestBase;
import org.junit.Test;

public class RpcUtilsTest extends HekateTestBase {
    @Test
    public void testValidUtilityClass() throws Exception {
        assertValidUtilityClass(RpcUtils.class);
    }
}
