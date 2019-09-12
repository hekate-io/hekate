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
