package io.hekate.cluster.seed.consul;

import io.hekate.HekateTestBase;
import io.hekate.util.format.ToString;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class ConsulSeedNodeProviderConfigTest extends HekateTestBase {
    private ConsulSeedNodeProviderConfig cfg = new ConsulSeedNodeProviderConfig();

    @Test
    public void testUrl() {
        assertNull(cfg.getUrl());

        cfg.setUrl("http://127.0.0.1:8501");

        assertEquals("http://127.0.0.1:8501", cfg.getUrl());

        assertSame(cfg, cfg.withUrl("http://127.0.0.1:8502"));

        assertEquals("http://127.0.0.1:8502", cfg.getUrl());
    }

    @Test
    public void testBasePath() {
        assertEquals(ConsulSeedNodeProviderConfig.DEFAULT_BASE_PATH, cfg.getBasePath());

        cfg.setBasePath("/test/path");

        assertEquals("/test/path", cfg.getBasePath());

        assertSame(cfg, cfg.withBasePath("/test/path2"));

        assertEquals("/test/path2", cfg.getBasePath());
    }

    @Test
    public void testCleanupInterval() {
        assertEquals(ConsulSeedNodeProviderConfig.DEFAULT_CLEANUP_INTERVAL, cfg.getCleanupInterval());

        cfg.setCleanupInterval(10001);

        assertEquals(10001, cfg.getCleanupInterval());

        assertSame(cfg, cfg.withCleanupInterval(10002));

        assertEquals(10002, cfg.getCleanupInterval());
    }

    @Test
    public void testConnectTimeout() {
        assertNull(cfg.getConnectTimeout());

        cfg.setConnectTimeout(10001L);

        assertEquals(10001L, cfg.getConnectTimeout().longValue());

        assertSame(cfg, cfg.withConnectTimeout(10002L));

        assertEquals(10002L, cfg.getConnectTimeout().longValue());
    }

    @Test
    public void testReadTimeout() {
        assertNull(cfg.getReadTimeout());

        cfg.setReadTimeout(10001L);

        assertEquals(10001L, cfg.getReadTimeout().longValue());

        assertSame(cfg, cfg.withReadTimeout(10002L));

        assertEquals(10002L, cfg.getReadTimeout().longValue());
    }

    @Test
    public void testWriteTimeout() {
        assertNull(cfg.getWriteTimeout());

        cfg.setWriteTimeout(10001L);

        assertEquals(10001L, cfg.getWriteTimeout().longValue());

        assertSame(cfg, cfg.withWriteTimeout(10002L));

        assertEquals(10002L, cfg.getWriteTimeout().longValue());
    }

    @Test
    public void testToString() {
        cfg.setUrl("http://127.0.0.1:8501");

        assertEquals(ToString.format(cfg), cfg.toString());
    }

}
