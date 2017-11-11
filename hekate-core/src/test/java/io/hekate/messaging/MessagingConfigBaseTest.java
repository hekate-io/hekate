package io.hekate.messaging;

import io.hekate.HekateTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public class MessagingConfigBaseTest extends HekateTestBase {
    private static class TestConfig extends MessagingConfigBase<TestConfig> {
        // No-op.
    }

    private final TestConfig cfg = new TestConfig();

    @Test
    public void testNioThreads() {
        assertEquals(0, cfg.getNioThreads());

        cfg.setNioThreads(10001);

        assertEquals(10001, cfg.getNioThreads());

        assertSame(cfg, cfg.withNioThreads(10002));

        assertEquals(10002, cfg.getNioThreads());
    }

    @Test
    public void testIdleSocketTimeout() {
        assertEquals(0, cfg.getIdleSocketTimeout());

        cfg.setIdleSocketTimeout(10001);

        assertEquals(10001, cfg.getIdleSocketTimeout());

        assertSame(cfg, cfg.withIdleSocketTimeout(10002));

        assertEquals(10002, cfg.getIdleSocketTimeout());
    }

    @Test
    public void testBackPressure() {
        MessagingBackPressureConfig c1 = new MessagingBackPressureConfig();
        MessagingBackPressureConfig c2 = new MessagingBackPressureConfig();

        assertNotNull(cfg.getBackPressure());

        cfg.setBackPressure(c1);

        assertSame(c1, cfg.getBackPressure());

        assertSame(cfg, cfg.withBackPressure(c2));

        assertSame(c2, cfg.getBackPressure());

        expect(IllegalArgumentException.class, () -> cfg.setBackPressure(null));

        assertSame(cfg, cfg.withBackPressure(bp ->
            assertSame(c2, bp)
        ));
    }
}
