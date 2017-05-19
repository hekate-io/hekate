package io.hekate.core.internal.util;

import io.hekate.HekateTestBase;
import java.util.Collections;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StreamUtilsTest extends HekateTestBase {
    @Test
    public void testNullSafe() {
        assertNotNull(StreamUtils.nullSafe(null));
        assertNotNull(StreamUtils.nullSafe(Collections.emptyList()));

        assertEquals(singletonList("non null"), StreamUtils.nullSafe(asList(null, "non null", null)).collect(toList()));
    }

    @Test
    public void testUtilityClass() throws Exception {
        assertValidUtilityClass(StreamUtils.class);
    }
}
