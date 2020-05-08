/*
 * Copyright 2020 The Hekate Project
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
import io.hekate.util.StateGuard.GuardedRunnable;
import io.hekate.util.StateGuard.GuardedSupplier;
import io.hekate.util.async.Waiting;
import io.hekate.util.format.ToString;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class StateGuardTest extends HekateTestBase {
    private final StateGuard guard = new StateGuard(getClass());

    @Test
    public void testRunWithReadLock() throws Exception {
        GuardedRunnable<?> task = mock(GuardedRunnable.class);

        guard.withReadLock(task);

        verify(task).run();
        verifyNoMoreInteractions(task);
    }

    @Test
    public void testGetWithReadLock() throws Exception {
        GuardedSupplier<String, ?> task = () -> "test";

        assertEquals("test", guard.withReadLock(task));
    }

    @Test
    public void testRunWithReadLockAndStateCheck() throws Exception {
        GuardedRunnable<?> task = mock(GuardedRunnable.class);

        expect(
            IllegalStateException.class,
            getClass().getSimpleName() + " is not initialized.",
            () -> guard.withReadLockAndStateCheck(task)
        );

        verifyNoMoreInteractions(task);

        guard.lockWrite();
        guard.becomeInitialized();
        guard.unlockWrite();

        guard.withReadLockAndStateCheck(task);

        verify(task).run();
        verifyNoMoreInteractions(task);
    }

    @Test
    public void testGetWithReadLockAndStateCheck() throws Exception {
        GuardedSupplier<String, ?> task = () -> "test";

        expect(
            IllegalStateException.class,
            getClass().getSimpleName() + " is not initialized.",
            () -> guard.withReadLockAndStateCheck(task)
        );

        guard.lockWrite();
        guard.becomeInitialized();
        guard.unlockWrite();

        assertEquals("test", guard.withReadLockAndStateCheck(task));
    }

    @Test
    public void testRunWithWriteLock() throws Exception {
        GuardedRunnable<?> task = mock(GuardedRunnable.class);

        doAnswer(invocationOnMock -> {
            assertTrue(guard.isWriteLocked());

            return null;
        }).when(task).run();

        guard.withWriteLock(task);

        verify(task).run();
        verifyNoMoreInteractions(task);

        assertFalse(guard.isWriteLocked());
    }

    @Test
    public void testGetWithWriteLock() throws Exception {
        GuardedSupplier<?, ?> task = mock(GuardedSupplier.class);

        doAnswer(invocationOnMock -> {
            assertTrue(guard.isWriteLocked());

            return "test";
        }).when(task).get();

        assertEquals("test", guard.withWriteLock(task));

        verify(task).get();
        verifyNoMoreInteractions(task);

        assertFalse(guard.isWriteLocked());
    }

    @Test
    public void testRunWithReadLockIfInitialized() throws Exception {
        GuardedRunnable<?> task = mock(GuardedRunnable.class);

        doAnswer(invocationOnMock -> {
            assertTrue(guard.isInitialized());

            return null;
        }).when(task).run();

        assertFalse(guard.withReadLockIfInitialized(task));

        verifyNoMoreInteractions(task);

        guard.withWriteLock((GuardedRunnable<?>)guard::becomeInitialized);

        guard.withReadLockIfInitialized(task);

        verify(task).run();
        verifyNoMoreInteractions(task);
    }

    @Test
    public void testRunWithWriteLockIfInitialized() throws Exception {
        GuardedRunnable<?> task = mock(GuardedRunnable.class);

        doAnswer(invocationOnMock -> {
            assertTrue(guard.isWriteLocked());
            assertTrue(guard.isInitialized());

            return null;
        }).when(task).run();

        assertFalse(guard.withWriteLockIfInitialized(task));

        verifyNoMoreInteractions(task);

        guard.withWriteLock((GuardedRunnable<?>)guard::becomeInitialized);

        guard.withWriteLockIfInitialized(task);

        verify(task).run();
        verifyNoMoreInteractions(task);

        assertFalse(guard.isWriteLocked());
    }

    @Test
    public void testBecomeInitializing() {
        guard.lockWrite();

        try {
            assertFalse(guard.isInitializing());

            guard.becomeInitializing();

            assertTrue(guard.isInitializing());

            expect(
                IllegalStateException.class,
                getClass().getSimpleName() + " is already initializing.",
                guard::becomeInitializing
            );
        } finally {
            guard.unlockWrite();
        }
    }

    @Test
    public void testBecomeInitialized() {
        guard.lockWrite();

        try {
            assertFalse(guard.isInitialized());

            guard.becomeInitialized();

            assertTrue(guard.isInitialized());

            expect(
                IllegalStateException.class,
                getClass().getSimpleName() + " already initialized.",
                guard::becomeInitialized
            );

            expect(
                IllegalStateException.class,
                getClass().getSimpleName() + " is already initialized.",
                guard::becomeInitializing
            );
        } finally {
            guard.unlockWrite();
        }
    }

    @Test
    public void testBecomeInitializedWithRunnable() throws Exception {
        guard.withReadLock(() ->
            assertFalse(guard.isInitialized())
        );

        GuardedRunnable<?> task = mock(GuardedRunnable.class);

        guard.becomeInitialized(task);

        guard.withReadLock(() ->
            assertTrue(guard.isInitialized())
        );

        expect(
            IllegalStateException.class,
            getClass().getSimpleName() + " already initialized.",
            () -> guard.becomeInitialized(task)
        );

        verify(task).run();
        verifyNoMoreInteractions(task);
    }

    @Test
    public void testBecomeTerminating() {
        guard.lockWrite();

        try {
            assertFalse(guard.becomeTerminating());

            guard.becomeInitialized();

            assertTrue(guard.becomeTerminating());
        } finally {
            guard.unlockWrite();
        }
    }

    @Test
    public void testBecomeTerminatingWithRunnable() throws Exception {
        GuardedRunnable<?> task = mock(GuardedRunnable.class);

        guard.becomeTerminating(task);

        verifyNoMoreInteractions(task);

        guard.becomeInitialized(() -> { /* No-op. */ });

        guard.becomeTerminating(task);

        verify(task).run();
        verifyNoMoreInteractions(task);

        guard.becomeTerminating(task);

        verifyNoMoreInteractions(task);
    }

    @Test
    public void testBecomeTerminatingWithSupplier() throws Exception {
        @SuppressWarnings("unchecked")
        GuardedSupplier<List<Waiting>, ?> task = mock(GuardedSupplier.class);

        assertNotNull(guard.becomeTerminating(task));

        verifyNoMoreInteractions(task);

        guard.becomeInitialized(() -> { /* No-op. */ });

        assertNotNull(guard.becomeTerminating(task));

        verify(task).get();
        verifyNoMoreInteractions(task);

        assertNotNull(guard.becomeTerminating(task));

        verifyNoMoreInteractions(task);
    }

    @Test
    public void testBecomeTerminated() {
        guard.lockWrite();

        try {
            assertFalse(guard.becomeTerminated());

            guard.becomeInitialized();

            assertTrue(guard.becomeTerminated());
        } finally {
            guard.unlockWrite();
        }
    }

    @Test
    public void testBecomeTerminatedWithRunnable() throws Exception {
        GuardedRunnable<?> task = mock(GuardedRunnable.class);

        guard.becomeTerminated(task);

        verifyNoMoreInteractions(task);

        guard.becomeInitialized(() -> { /* No-op. */ });

        guard.becomeTerminated(task);

        verify(task).run();
        verifyNoMoreInteractions(task);

        guard.becomeTerminated(task);

        verifyNoMoreInteractions(task);
    }

    @Test
    public void testBecomeTerminatedWithSupplier() throws Exception {
        @SuppressWarnings("unchecked")
        GuardedSupplier<List<Waiting>, ?> task = mock(GuardedSupplier.class);

        assertNotNull(guard.becomeTerminated(task));

        verifyNoMoreInteractions(task);

        guard.becomeInitialized(() -> { /* No-op. */ });

        assertNotNull(guard.becomeTerminated(task));

        verify(task).get();
        verifyNoMoreInteractions(task);

        assertNotNull(guard.becomeTerminated(task));

        verifyNoMoreInteractions(task);
    }

    @Test
    public void testLockReadWithStateCheck() {
        expect(
            IllegalStateException.class,
            getClass().getSimpleName() + " is not initialized.",
            guard::lockReadWithStateCheck
        );

        expect(
            IllegalStateException.class,
            getClass().getSimpleName() + " is not initialized.",
            () -> guard.lockReadWithStateCheck(StateGuard.State.INITIALIZED)
        );

        guard.lockWrite();
        guard.becomeInitialized();
        guard.unlockWrite();

        guard.lockReadWithStateCheck();
        guard.unlockRead();

        guard.lockReadWithStateCheck(StateGuard.State.INITIALIZED);
        guard.unlockRead();
    }

    @Test
    public void testTryLockReadWithStateCheck() {
        assertFalse(guard.tryLockReadWithStateCheck());
        assertFalse(guard.tryLockReadWithStateCheck(StateGuard.State.INITIALIZED));

        guard.lockWrite();
        guard.becomeInitialized();
        guard.unlockWrite();

        assertTrue(guard.tryLockReadWithStateCheck());
        guard.unlockRead();

        assertTrue(guard.tryLockReadWithStateCheck(StateGuard.State.INITIALIZED));
        guard.unlockRead();
    }

    @Test
    public void testLockWriteWithStateCheck() {
        expect(
            IllegalStateException.class,
            getClass().getSimpleName() + " is not initialized.",
            guard::lockWriteWithStateCheck
        );

        guard.lockWrite();
        guard.becomeInitialized();
        guard.unlockWrite();

        guard.lockWriteWithStateCheck();

        assertTrue(guard.isWriteLocked());

        guard.unlockWrite();
    }

    @Test
    public void testLockUnlockRead() throws InterruptedException {
        guard.lockRead();

        assertFalse(guard.tryLockWrite(1, TimeUnit.NANOSECONDS));
        assertTrue(guard.tryLockRead(1, TimeUnit.NANOSECONDS));

        guard.unlockRead();
    }

    @Test
    public void testLockUnlockWrite() throws InterruptedException {
        assertFalse(guard.isWriteLocked());

        guard.lockWrite();

        assertTrue(guard.isWriteLocked());

        guard.unlockWrite();
    }

    @Test
    public void testToString() {
        assertEquals(guard.toString(), ToString.format(guard), guard.toString());
    }
}
