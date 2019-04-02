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

package io.hekate.util;

import io.hekate.HekateTestBase;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public abstract class UuidTestBase<T extends UuidBase<T>> extends HekateTestBase {
    protected abstract T newUuid();

    protected abstract T newUuid(long hi, long lo);

    protected abstract T newUuid(String s);

    @Test
    public void testRandomness() throws Exception {
        Set<T> ids = new HashSet<>();

        for (int i = 0; i < 100; i++) {
            ids.add(newUuid());
        }

        assertEquals(100, ids.size());
    }

    @Test
    public void testConstructFromBits() {
        assertEquals(newUuid(0, 0), newUuid(0, 0));
        assertEquals(newUuid(0, 1), newUuid(0, 1));
        assertEquals(newUuid(1, 0), newUuid(1, 0));
        assertEquals(newUuid(1, 1), newUuid(1, 1));

        for (int i = 0; i < 100; i++) {
            T id = newUuid();

            assertEquals(id, newUuid(id.hiBits(), id.loBits()));
        }
    }

    @Test
    public void testToAndFromString() throws Exception {
        assertEquals(newUuid(0, 0), newUuid(newUuid(0, 0).toString()));
        assertEquals(newUuid(0, 1), newUuid(newUuid(0, 1).toString()));
        assertEquals(newUuid(1, 0), newUuid(newUuid(1, 0).toString()));
        assertEquals(newUuid(1, 1), newUuid(newUuid(1, 1).toString()));

        for (int i = 0; i < 100; i++) {
            T id1 = newUuid();
            T id2 = newUuid(id1.toString());

            assertEquals(id1, id2);
        }
    }

    @Test
    public void testCompareTo() {
        assertEquals(0, newUuid(0, 0).compareTo(newUuid(0, 0)));
        assertEquals(-1, newUuid(0, 0).compareTo(newUuid(0, 1)));
        assertEquals(1, newUuid(0, 1).compareTo(newUuid(0, 0)));
        assertEquals(-1, newUuid(0, 1).compareTo(newUuid(1, 0)));
        assertEquals(1, newUuid(1, 1).compareTo(newUuid(0, 1)));
    }
}
