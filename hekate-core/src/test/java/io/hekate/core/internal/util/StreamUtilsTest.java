/*
 * Copyright 2021 The Hekate Project
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
import org.junit.Test;

import static io.hekate.core.internal.util.StreamUtils.nullSafe;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StreamUtilsTest extends HekateTestBase {
    @Test
    public void testNullSafe() {
        assertNotNull(nullSafe(null));
        assertNotNull(nullSafe(emptyList()));

        assertEquals(singletonList("non null"), nullSafe(asList(null, "non null", null)).collect(toList()));
    }

    @Test
    public void testUtilityClass() throws Exception {
        assertValidUtilityClass(StreamUtils.class);
    }
}
