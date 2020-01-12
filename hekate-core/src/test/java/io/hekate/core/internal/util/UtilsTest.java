/*
 * Copyright 2019 The Hekate Project
 *
 * The Hekate Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.hekate.core.internal.util;

import io.hekate.HekateTestBase;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

import static io.hekate.core.internal.util.Utils.camelCase;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UtilsTest extends HekateTestBase {
    @Test
    public void testMod() {
        assertEquals(1, Utils.mod(6, 5));
        assertEquals(1, Utils.mod(-6, 5));
    }

    @Test
    public void testCamelCase() {
        assertEquals("TEST", camelCase("TEST"));
        assertEquals("Test", camelCase("Test"));
        assertEquals("TeSt", camelCase("Te.st"));
        assertEquals("TeSt", camelCase("Te..st"));
        assertEquals("TeSt", camelCase("Te.-st"));
        assertEquals("TeSt", camelCase("Te-st"));
        assertEquals("TeSt", camelCase("Te--st"));
        assertEquals("TeSt", camelCase("Te-_st"));
        assertEquals("TeSt", camelCase("Te_st"));
        assertEquals("TeST", camelCase("Te_s_t_"));
        assertEquals("TeST", camelCase("te_s_t_"));
    }

    @Test
    public void nullSafeImmutableCopy() {
        assertTrue(Utils.nullSafeImmutableCopy(null).isEmpty());
        assertTrue(Utils.nullSafeImmutableCopy(Collections.emptyList()).isEmpty());
        assertTrue(Utils.nullSafeImmutableCopy(Arrays.asList(null, null, null)).isEmpty());
        assertEquals(3, Utils.nullSafeImmutableCopy(Arrays.asList(1, 2, 3)).size());

        expect(UnsupportedOperationException.class, () -> {
            Utils.nullSafeImmutableCopy(Arrays.asList(1, 2, 3)).add(4);
        });
    }
}
