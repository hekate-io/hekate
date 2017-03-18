package io.hekate.util;

import io.hekate.HekateTestBase;
import io.hekate.util.format.ToString;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StateGuardTest extends HekateTestBase {
    private final StateGuard guard = new StateGuard(getClass());

    @Test
    public void testBecomeInitializing() {
        guard.lockWrite();

        try {
            assertFalse(guard.isInitializing());

            guard.becomeInitializing();

            assertTrue(guard.isInitializing());

            expect(IllegalStateException.class, getClass().getSimpleName() + " is already in INITIALIZING state.",
                guard::becomeInitializing);
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

            expect(IllegalStateException.class, getClass().getSimpleName() + " already initialized.", guard::becomeInitialized);
            expect(IllegalStateException.class, getClass().getSimpleName() + " is already in INITIALIZED state.",
                guard::becomeInitializing);
        } finally {
            guard.unlockWrite();
        }
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
    public void testLockReadWithStateCheck() {
        expect(IllegalStateException.class, getClass().getSimpleName() + " is not initialized.", guard::lockReadWithStateCheck);
        expect(IllegalStateException.class, getClass().getSimpleName() + " is not in INITIALIZED state.", () ->
            guard.lockReadWithStateCheck(StateGuard.State.INITIALIZED)
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
        expect(IllegalStateException.class, getClass().getSimpleName() + " is not initialized.", guard::lockWriteWithStateCheck);

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
