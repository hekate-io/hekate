/*
 * Copyright 2017 The Hekate Project
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

package io.hekate.lock.internal;

import io.hekate.HekateInstanceTestBase;
import io.hekate.lock.LockRegion;
import io.hekate.lock.LockRegionConfig;
import io.hekate.lock.LockService;
import io.hekate.lock.LockServiceFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class LockServiceSingleNodeTest extends HekateInstanceTestBase {
    @Test
    public void testEmptyRegions() throws Exception {
        LockService locks = createInstance(boot -> boot.withService(new LockServiceFactory())).join().get(LockService.class);

        assertTrue(locks.allRegions().isEmpty());

        assertFalse(locks.hasRegion("no-such-region"));

        expect(IllegalArgumentException.class, () -> locks.region("no-such-region"));
    }

    @Test
    public void testMultipleRegions() throws Exception {
        LockService locks = createInstance(boot ->
            boot.withService(new LockServiceFactory()
                .withRegion(new LockRegionConfig("test1"))
                .withRegion(new LockRegionConfig("test2"))
            )
        ).join().get(LockService.class);

        assertTrue(locks.hasRegion("test1"));
        assertTrue(locks.hasRegion("test2"));

        LockRegion region1 = locks.region("test1");
        LockRegion region2 = locks.region("test2");

        assertNotNull(region1);
        assertNotNull(region2);

        assertEquals(2, locks.allRegions().size());
        assertTrue(locks.allRegions().contains(locks.region("test1")));
        assertTrue(locks.allRegions().contains(region2));

        region1.getLock("lock").lock();
        region2.getLock("lock").lock();
        region1.getLock("lock").unlock();
        region2.getLock("lock").unlock();
    }
}
