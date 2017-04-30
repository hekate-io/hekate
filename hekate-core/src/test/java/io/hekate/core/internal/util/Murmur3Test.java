package io.hekate.core.internal.util;

import io.hekate.HekateTestBase;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class Murmur3Test extends HekateTestBase {
    @Test
    public void test() throws Exception {
        assertValidUtilityClass(Murmur3.class);

        Set<Integer> hashes = new HashSet<>(10000, 1.0f);

        for (int i = 0; i < 10000; i++) {
            hashes.add(Murmur3.hash(1, i));
        }

        assertEquals(10000, hashes.size());
    }
}
