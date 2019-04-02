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

package io.hekate.election.internal;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.core.Hekate;
import io.hekate.election.Candidate;
import io.hekate.election.FollowerContext;
import io.hekate.election.LeaderChangeListener;
import io.hekate.election.LeaderContext;
import io.hekate.election.LeaderFuture;
import io.hekate.lock.DistributedLock;
import io.hekate.lock.LockOwnerInfo;
import io.hekate.lock.internal.DefaultLockOwnerInfo;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class CandidateHandlerTest extends HekateTestBase {
    private final Candidate candidate = mock(Candidate.class);

    private final ExecutorService worker = mock(ExecutorService.class);

    private final DistributedLock lock = mock(DistributedLock.class);

    private final ArgumentCaptor<LeaderContext> leaderCtx = ArgumentCaptor.forClass(LeaderContext.class);

    private final ArgumentCaptor<FollowerContext> followerCtx = ArgumentCaptor.forClass(FollowerContext.class);

    private ClusterNode localNode;

    private CandidateHandler handler;

    @Before
    public void setUp() throws Exception {
        localNode = newNode();

        handler = new CandidateHandler("test", candidate, worker, lock, localNode, mock(Hekate.class));

        doAnswer(invocation -> {
            ((Runnable)invocation.getArguments()[0]).run();

            return null;
        }).when(worker).execute(any(Runnable.class));

        handler.initialize();

        verify(lock).lockAsync(worker, handler);

        reset(lock);
    }

    @Test
    public void testTerminate() throws Exception {
        handler.terminate();
        handler.shutdown();

        verify(candidate, never()).terminate();
        verify(worker).shutdown();

        reset(candidate);

        handler.onLockBusy(new DefaultLockOwnerInfo(1, newNode()));
        handler.onLockOwnerChange(new DefaultLockOwnerInfo(1, newNode()));
        handler.onLockAcquire(lock);

        verifyNoMoreInteractions(candidate);
    }

    @Test
    public void testTerminateLeader() throws Exception {
        handler.onLockAcquire(lock);

        handler.terminate();
        handler.shutdown();

        verify(candidate).terminate();
        verify(worker).shutdown();
    }

    @Test
    public void testTerminateLeaderWithError() throws Exception {
        handler.onLockAcquire(lock);

        doThrow(TEST_ERROR).when(candidate).terminate();

        handler.terminate();
        handler.shutdown();

        verify(candidate).terminate();
        verify(worker).shutdown();
    }

    @Test
    public void testTerminateFollowerWithError() throws Exception {
        handler.onLockBusy(new DefaultLockOwnerInfo(1, newNode()));

        doThrow(TEST_ERROR).when(candidate).terminate();

        handler.terminate();
        handler.shutdown();

        verify(candidate).terminate();
        verify(worker).shutdown();
    }

    @Test
    public void testTerminateFollower() throws Exception {
        handler.onLockBusy(new DefaultLockOwnerInfo(1, newNode()));

        handler.terminate();
        handler.shutdown();

        verify(candidate).terminate();
        verify(worker).shutdown();
    }

    @Test
    public void testOnLockAcquire() throws Exception {
        handler.onLockAcquire(lock);

        verify(candidate).becomeLeader(leaderCtx.capture());

        assertEquals(localNode, leaderCtx.getValue().localNode());
    }

    @Test
    public void testOnLockBusy() throws Exception {
        LockOwnerInfo lockOwner = new DefaultLockOwnerInfo(1, newNode());

        handler.onLockBusy(lockOwner);

        verify(candidate).becomeFollower(followerCtx.capture());

        assertEquals(localNode, followerCtx.getValue().localNode());
        assertEquals(lockOwner.node(), followerCtx.getValue().leader());
    }

    @Test
    public void testOnLockOwnerChangeListener() throws Exception {
        LockOwnerInfo lockOwner1 = new DefaultLockOwnerInfo(1, newNode());

        handler.onLockBusy(lockOwner1);

        verify(candidate).becomeFollower(followerCtx.capture());

        LeaderChangeListener listener = mock(LeaderChangeListener.class);

        followerCtx.getValue().addListener(listener);

        for (int i = 0; i < 3; i++) {
            LockOwnerInfo lockOwner2 = new DefaultLockOwnerInfo(1, newNode());

            handler.onLockOwnerChange(lockOwner2);

            verify(listener).onLeaderChange(followerCtx.getValue());

            verifyNoMoreInteractions(listener);

            reset(listener);
        }

        assertTrue(followerCtx.getValue().removeListener(listener));

        handler.onLockOwnerChange(new DefaultLockOwnerInfo(1, newNode()));

        verifyNoMoreInteractions(listener);
    }

    @Test
    public void testGetLeaderFutureOfLeader() throws Exception {
        LeaderFuture future = handler.leaderFuture();

        assertFalse(future.isDone());

        handler.onLockAcquire(lock);

        assertTrue(future.isDone());
        assertEquals(localNode, future.get());
        assertEquals(localNode, get(future));
    }

    @Test
    public void testGetLeaderFutureOfFollower() throws Exception {
        LeaderFuture future = handler.leaderFuture();

        assertFalse(future.isDone());

        LockOwnerInfo lockOwner = new DefaultLockOwnerInfo(1, newNode());

        handler.onLockBusy(lockOwner);

        assertTrue(future.isDone());
        assertEquals(lockOwner.node(), future.get());

        assertTrue(handler.leaderFuture().isDone());
        assertEquals(lockOwner.node(), handler.leaderFuture().get());

        lockOwner = new DefaultLockOwnerInfo(1, newNode());

        handler.onLockOwnerChange(lockOwner);

        future = handler.leaderFuture();

        assertTrue(future.isDone());
        assertEquals(lockOwner.node(), future.get());
    }

    @Test
    public void testGetLeaderFutureOfTerminated() throws Exception {
        LeaderFuture before = handler.leaderFuture();

        handler.terminate();
        handler.shutdown();

        LeaderFuture after = handler.leaderFuture();

        assertNotSame(before, after);

        assertTrue(before.isDone());
        assertTrue(after.isDone());

        expect(CancellationException.class, before::get);
        expect(CancellationException.class, () -> get(before));
        expect(CancellationException.class, after::get);
        expect(CancellationException.class, () -> get(after));
    }

    @Test
    public void testYieldLeadership() throws Exception {
        handler.onLockAcquire(lock);

        verify(candidate).becomeLeader(leaderCtx.capture());
        reset(candidate);

        LeaderFuture futureBefore = handler.leaderFuture();

        assertTrue(futureBefore.isDone());

        when(lock.isHeldByCurrentThread()).thenReturn(true);

        leaderCtx.getValue().yieldLeadership();

        InOrder order = inOrder(lock);

        order.verify(lock).unlock();
        order.verify(lock).lockAsync(worker, handler);

        LeaderFuture futureAfter = handler.leaderFuture();

        assertNotSame(futureBefore, futureAfter);

        assertFalse(futureAfter.isDone());

        handler.onLockAcquire(lock);

        verify(candidate).becomeLeader(leaderCtx.capture());

        assertTrue(futureAfter.isDone());
        assertEquals(localNode, futureAfter.get());
    }
}
