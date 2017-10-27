package io.hekate.rpc;

import io.hekate.HekateTestBase;
import io.hekate.util.format.ToString;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class RpcServerConfigTest extends HekateTestBase {
    public interface TestRpc {
        // No-op.
    }

    private final RpcServerConfig cfg = new RpcServerConfig();

    @Test
    public void testHandler() {
        assertNull(cfg.getHandler());

        TestRpc handler = mock(TestRpc.class);

        cfg.setHandler(handler);

        assertSame(handler, cfg.getHandler());

        cfg.setHandler(null);

        assertNull(cfg.getHandler());

        assertSame(cfg, cfg.withHandler(handler));
        assertSame(handler, cfg.getHandler());
    }

    @Test
    public void testTags() {
        assertNull(cfg.getTags());

        Set<String> tags = new HashSet<>(Arrays.asList("one", "two"));

        cfg.setTags(tags);

        assertEquals(tags, cfg.getTags());

        cfg.setTags(null);

        assertNull(cfg.getTags());

        assertSame(cfg, cfg.withTag("three"));
        assertEquals(Collections.singleton("three"), cfg.getTags());
    }

    @Test
    public void testToString() {
        assertEquals(ToString.format(cfg), cfg.toString());
    }
}
