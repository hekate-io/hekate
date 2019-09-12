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
import io.hekate.util.format.ToString;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StampedStateGuardTest extends HekateTestBase {
    private final StampedStateGuard guard = new StampedStateGuard(getClass());

    @Test
    public void testBecomeInitializing() {
        long lock = guard.lockWrite();

        try {
            assertFalse(guard.isInitializing());

            guard.becomeInitializing();

            assertTrue(guard.isInitializing());

            expect(IllegalStateException.class, getClass().getSimpleName() + " is already initializing.",
                guard::becomeInitializing);
        } finally {
            guard.unlockWrite(lock);
        }
    }

    @Test
    public void testBecomeInitialized() {
        long lock = guard.lockWrite();

        try {
            assertFalse(guard.isInitialized());

            guard.becomeInitialized();

            assertTrue(guard.isInitialized());

            expect(IllegalStateException.class, getClass().getSimpleName() + " is already initialized.", guard::becomeInitialized);
            expect(IllegalStateException.class, getClass().getSimpleName() + " is already initialized.",
                guard::becomeInitializing);
        } finally {
            guard.unlockWrite(lock);
        }
    }

    @Test
    public void testBecomeTerminating() {
        long lock = guard.lockWrite();

        try {
            assertFalse(guard.becomeTerminating());

            guard.becomeInitialized();

            assertTrue(guard.becomeTerminating());
        } finally {
            guard.unlockWrite(lock);
        }
    }

    @Test
    public void testBecomeTerminated() {
        long lock = guard.lockWrite();

        try {
            assertFalse(guard.becomeTerminated());

            guard.becomeInitialized();

            assertTrue(guard.becomeTerminated());
        } finally {
            guard.unlockWrite(lock);
        }
    }

    @Test
    public void testLockReadWithStateCheck() {
        expect(IllegalStateException.class, getClass().getSimpleName() + " is not initialized.", guard::lockReadWithStateCheck);

        long lock = guard.lockWrite();
        guard.becomeInitialized();
        guard.unlockWrite(lock);

        lock = guard.lockReadWithStateCheck();
        guard.unlockRead(lock);
    }

    @Test
    public void testLockWriteWithStateCheck() {
        expect(IllegalStateException.class, getClass().getSimpleName() + " is not initialized.", guard::lockWriteWithStateCheck);

        long lock = guard.lockWrite();
        guard.becomeInitialized();
        guard.unlockWrite(lock);

        lock = guard.lockWriteWithStateCheck();

        assertTrue(guard.isWriteLocked());

        guard.unlockWrite(lock);
    }

    @Test
    public void testLockUnlockRead() throws InterruptedException {
        long lock = guard.lockRead();

        guard.unlockRead(lock);
    }

    @Test
    public void testLockUnlockWrite() throws InterruptedException {
        assertFalse(guard.isWriteLocked());

        long lock = guard.lockWrite();

        assertTrue(guard.isWriteLocked());

        guard.unlockWrite(lock);
    }

    @Test
    public void testToString() {
        assertEquals(guard.toString(), ToString.format(guard), guard.toString());
    }
}
