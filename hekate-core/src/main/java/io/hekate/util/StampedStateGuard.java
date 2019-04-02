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

import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.Locale;
import java.util.concurrent.locks.StampedLock;

import static io.hekate.util.StampedStateGuard.State.INITIALIZED;
import static io.hekate.util.StampedStateGuard.State.INITIALIZING;
import static io.hekate.util.StampedStateGuard.State.TERMINATED;
import static io.hekate.util.StampedStateGuard.State.TERMINATING;

/**
 * Helper class that provides support for components lifecycle management and locking.
 *
 * <p>
 * Internally this class is backed by {@link StampedLock}.
 * </p>
 *
 * @see StateGuard
 */
public class StampedStateGuard {
    /**
     * Enumeration of {@link StampedStateGuard} states.
     */
    public enum State {
        /** See {@link #becomeInitializing()}. */
        INITIALIZING,

        /** See {@link #becomeInitialized()}. */
        INITIALIZED,

        /** See {@link #becomeTerminating()}. */
        TERMINATING,

        /** See {@link #becomeTerminated()}. */
        TERMINATED
    }

    private final Class<?> type;

    @ToStringIgnore
    private final StampedLock lock = new StampedLock();

    private State state = TERMINATED;

    /**
     * Constructs new instance.
     *
     * @param type Type of a guarded component (for errors reporting).
     */
    public StampedStateGuard(Class<?> type) {
        assert type != null : "Type is null.";

        this.type = type;
    }

    /**
     * Switches this guard to {@link State#INITIALIZING} state.
     *
     * <p>
     * <b>Important!!!</b> This method must be invoked with {@link #lockWrite() write lock} being held.
     * </p>
     */
    public void becomeInitializing() {
        assert lock.isWriteLocked() : "Thread must hold write lock.";

        if (state == INITIALIZING || state == INITIALIZED) {
            throw new IllegalStateException(type.getSimpleName() + " is already " + state.name().toLowerCase(Locale.US) + '.');
        }

        state = INITIALIZING;
    }

    /**
     * Returns {@code true} if this guard is in {@link State#INITIALIZING} state.
     *
     * <p>
     * <b>Important!!!</b> This method must be invoked with {@link #lockWrite() write lock} or {@link #lockRead() read lock} being held.
     * </p>
     *
     * @return {@code true} if this guard is in {@link State#INITIALIZING} state.
     */
    public boolean isInitializing() {
        assert lock.isReadLocked() || lock.isWriteLocked() : "Thread must hold read or write lock.";

        return state == INITIALIZING;
    }

    /**
     * Switches this guard to {@link State#INITIALIZED} state.
     *
     * <p>
     * <b>Important!!!</b> This method must be invoked with {@link #lockWrite() write lock} being held.
     * </p>
     */
    public void becomeInitialized() {
        assert lock.isWriteLocked() : "Thread must hold write lock.";

        if (state == INITIALIZED) {
            throw new IllegalStateException(type.getSimpleName() + " is already " + INITIALIZED.name().toLowerCase(Locale.US) + '.');
        }

        state = INITIALIZED;
    }

    /**
     * Returns {@code true} if this guard is in {@link State#INITIALIZED} state.
     *
     * <p>
     * <b>Important!!!</b> This method must be invoked with {@link #lockWrite() write lock} or {@link #lockRead() read lock} being held.
     * </p>
     *
     * @return {@code true} if this guard is in {@link State#INITIALIZED} state.
     */
    public boolean isInitialized() {
        assert lock.isReadLocked() || lock.isWriteLocked() : "Thread must hold read or write lock.";

        return state == INITIALIZED;
    }

    /**
     * Switches this guard to {@link State#TERMINATING} state if guard is currently in {@link State#INITIALIZING} or {@link
     * State#INITIALIZED} state.
     *
     * <p>
     * <b>Important!!!</b> This method must be invoked with {@link #lockWrite() write lock} being held.
     * </p>
     *
     * @return {@code true} if successfully switched to {@link State#TERMINATING} state.
     */
    public boolean becomeTerminating() {
        assert lock.isWriteLocked() : "Thread must hold write lock.";

        if (state != TERMINATING && state != TERMINATED) {
            state = TERMINATING;

            return true;
        }

        return false;
    }

    /**
     * Switches this guard to {@link State#TERMINATED} state if guard is currently in any other state besides {@link State#TERMINATED}.
     *
     * <p>
     * <b>Important!!!</b> This method must be invoked with {@link #lockWrite() write lock} being held.
     * </p>
     *
     * @return {@code true} if successfully switched to {@link State#TERMINATED} state.
     */
    public boolean becomeTerminated() {
        assert lock.isWriteLocked() : "Thread must hold write lock.";

        if (state != TERMINATED) {
            state = TERMINATED;

            return true;
        }

        return false;
    }

    /**
     * Acquires the read lock on this guard and checks its current state. If state is anything other than {@link State#INITIALIZED} then
     * {@link IllegalStateException} will be thrown and lock will be released.
     *
     * @return Stamp that can be used to unlock.
     *
     * @throws IllegalStateException If guard is not in {@link State#INITIALIZED} state.
     */
    public long lockReadWithStateCheck() {
        long locked = lock.readLock();

        if (state != INITIALIZED) {
            lock.unlockRead(locked);

            throw new IllegalStateException(type.getSimpleName() + " is not " + INITIALIZED.name().toLowerCase(Locale.US) + '.');
        }

        return locked;
    }

    /**
     * Acquires the read lock.
     *
     * @return Stamp that can be used to unlock.
     *
     * @see StampedLock#readLock()
     */
    public long lockRead() {
        return lock.readLock();
    }

    /**
     * Releases the read lock.
     *
     * @param stamp Lock stamp (see {@link #lockRead()}).
     *
     * @see StampedLock#unlockRead(long)
     */
    public void unlockRead(long stamp) {
        lock.unlockRead(stamp);
    }

    /**
     * Acquires the write lock on this guard and checks its current state. If state is anything other than {@link State#INITIALIZED} then
     * {@link IllegalStateException} will be thrown and lock will be released.
     *
     * @return Stamp that can be used to unlock.
     *
     * @throws IllegalStateException If guard is not in {@link State#INITIALIZED} state.
     */
    public long lockWriteWithStateCheck() {
        long locked = lock.writeLock();

        if (state != INITIALIZED) {
            lock.unlockWrite(locked);

            throw new IllegalStateException(type.getSimpleName() + " is not " + INITIALIZED.name().toLowerCase(Locale.US) + '.');
        }

        return locked;
    }

    /**
     * Acquires the write lock.
     *
     * @return Stamp that can be used to unlock.
     *
     * @see StampedLock#writeLock()
     */
    public long lockWrite() {
        return lock.writeLock();
    }

    /**
     * Releases the write lock.
     *
     * @param stamp Lock stamp (see {@link #lockWrite()}).
     *
     * @see StampedLock#unlockWrite(long)
     */
    public void unlockWrite(long stamp) {
        lock.unlockWrite(stamp);
    }

    /**
     * Returns {@code true} if current thread hold the write lock.
     *
     * @return {@code true} if current thread hold the write lock.
     *
     * @see StampedLock#isWriteLocked()
     */
    public boolean isWriteLocked() {
        return lock.isWriteLocked();
    }

    @Override
    public String toString() {
        long locked = lock.readLock();

        try {
            return ToString.format(this);
        } finally {
            lock.unlockRead(locked);
        }
    }
}
