package io.hekate.cluster.internal;

import io.hekate.HekateTestBase;
import io.hekate.cluster.internal.SplitBrainManager.Callback;
import io.hekate.cluster.split.SplitBrainAction;
import io.hekate.cluster.split.SplitBrainDetector;
import io.hekate.util.format.ToString;
import java.util.concurrent.Executor;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SplitBrainManagerTest extends HekateTestBase {
    @Test
    public void testInitTerminate() throws Exception {
        SplitBrainManager mgr = new SplitBrainManager(SplitBrainAction.TERMINATE, null);

        for (int i = 0; i < 5; i++) {
            mgr.initialize(newNode(), mock(Executor.class), mock(Callback.class));

            mgr.terminate();
        }
    }

    @Test
    public void testNoDetector() throws Exception {
        Callback callback = mock(Callback.class);

        SplitBrainManager mgr = new SplitBrainManager(SplitBrainAction.TERMINATE, null);

        mgr.initialize(newNode(), mock(Executor.class), callback);

        assertSame(SplitBrainAction.TERMINATE, mgr.action());
        assertNull(mgr.detector());
        assertTrue(mgr.check());
    }

    @Test
    public void testApplyActionRejoin() throws Exception {
        Callback callback = mock(Callback.class);

        SplitBrainManager mgr = new SplitBrainManager(SplitBrainAction.REJOIN, null);

        mgr.initialize(newNode(), mock(Executor.class), callback);

        mgr.applyAction();

        verify(callback).rejoin();
        verifyNoMoreInteractions(callback);
    }

    @Test
    public void testApplyActionTerminate() throws Exception {
        Callback callback = mock(Callback.class);

        SplitBrainManager mgr = new SplitBrainManager(SplitBrainAction.TERMINATE, null);

        mgr.initialize(newNode(), mock(Executor.class), callback);

        mgr.applyAction();

        verify(callback).terminate();
        verifyNoMoreInteractions(callback);
    }

    @Test
    public void testApplyActionKillJvm() throws Exception {
        Callback callback = mock(Callback.class);

        SplitBrainManager mgr = new SplitBrainManager(SplitBrainAction.KILL_JVM, null);

        mgr.initialize(newNode(), mock(Executor.class), callback);

        mgr.applyAction();

        verify(callback).kill();
        verifyNoMoreInteractions(callback);
    }

    @Test
    public void testCheckAsync() throws Exception {
        SplitBrainDetector detector = mock(SplitBrainDetector.class);
        Callback callback = mock(Callback.class);

        SplitBrainManager mgr = new SplitBrainManager(SplitBrainAction.TERMINATE, detector);

        mgr.initialize(newNode(), Runnable::run, callback);

        when(detector.isValid(any())).thenReturn(false);

        mgr.checkAsync();

        verify(callback).terminate();
    }

    @Test
    public void testCheckAsyncError() throws Exception {
        SplitBrainDetector detector = mock(SplitBrainDetector.class);
        Callback callback = mock(Callback.class);

        SplitBrainManager mgr = new SplitBrainManager(SplitBrainAction.TERMINATE, detector);

        mgr.initialize(newNode(), Runnable::run, callback);

        when(detector.isValid(any())).thenThrow(TEST_ERROR);

        mgr.checkAsync();

        verify(callback).error(eq(TEST_ERROR));
    }

    @Test
    public void testStaleAsyncTaskAfterTerminate() throws Exception {
        SplitBrainDetector detector = mock(SplitBrainDetector.class);
        Callback callback = mock(Callback.class);
        Executor async = mock(Executor.class);
        ArgumentCaptor<Runnable> asyncCaptor = ArgumentCaptor.forClass(Runnable.class);

        SplitBrainManager mgr = new SplitBrainManager(SplitBrainAction.TERMINATE, detector);

        mgr.initialize(newNode(), async, callback);

        when(detector.isValid(any())).thenReturn(false);

        mgr.checkAsync();

        verify(async, times(1)).execute(asyncCaptor.capture());

        mgr.terminate();

        asyncCaptor.getValue().run();

        verifyNoMoreInteractions(async, detector, callback);
    }

    @Test
    public void testCheckAsyncRepeatingCalls() throws Exception {
        SplitBrainDetector detector = mock(SplitBrainDetector.class);
        Executor async = mock(Executor.class);
        ArgumentCaptor<Runnable> asyncCaptor = ArgumentCaptor.forClass(Runnable.class);

        SplitBrainManager mgr = new SplitBrainManager(SplitBrainAction.TERMINATE, detector);

        mgr.initialize(newNode(), async, mock(Callback.class));

        for (int i = 0; i < 5; i++) {
            mgr.checkAsync();
            mgr.checkAsync();
            mgr.checkAsync();

            verify(async, times(1)).execute(asyncCaptor.capture());

            asyncCaptor.getValue().run();

            verify(detector, times(1)).isValid(any());

            reset(detector, async);
        }
    }

    @Test
    public void testToString() {
        SplitBrainManager mgr = new SplitBrainManager(SplitBrainAction.TERMINATE, mock(SplitBrainDetector.class));

        assertEquals(ToString.format(mgr), mgr.toString());
    }
}
