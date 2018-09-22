package io.hekate.util.trace;

import io.hekate.HekateTestBase;
import io.hekate.util.format.ToString;
import java.util.Collections;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class TraceInfoTest extends HekateTestBase {
    @Test
    public void test() {
        TraceInfo trace = TraceInfo.of("test");

        assertEquals("test", trace.name());
        assertNull(trace.tags());

        assertSame(trace, trace.withTag("t", "v"));

        assertNotNull(trace.tags());
        assertEquals(Collections.singletonMap("t", "v"), trace.tags());

        assertEquals(ToString.format(trace), trace.toString());
    }
}
