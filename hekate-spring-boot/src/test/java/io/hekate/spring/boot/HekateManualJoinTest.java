package io.hekate.spring.boot;

import io.hekate.core.Hekate;
import org.junit.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import static org.junit.Assert.assertSame;

public class HekateManualJoinTest extends HekateAutoConfigurerTestBase {
    @EnableAutoConfiguration
    static class ManualJoinApp extends HekateTestConfigBase {
        // No-op.
    }

    @Test
    public void test() throws Exception {
        registerAndRefresh(new String[]{
            "hekate.deferred-join=true",
            "hekate.deferred-join-condition=manual"
        }, ManualJoinApp.class);

        Hekate node = getNode();

        assertSame(Hekate.State.INITIALIZED, node.state());

        node.join();

        assertSame(Hekate.State.UP, node.state());
    }
}
