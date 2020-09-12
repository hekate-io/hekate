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

package io.hekate.cluster.internal;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterSplitBrainException;
import io.hekate.cluster.internal.SplitBrainManager.ErrorCallback;
import io.hekate.cluster.split.SplitBrainDetector;
import io.hekate.util.format.ToString;
import java.util.concurrent.Executor;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
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
        SplitBrainManager mgr = new SplitBrainManager(0, null);

        for (int i = 0; i < 5; i++) {
            mgr.initialize(newNode(), mock(Executor.class), mock(ErrorCallback.class));

            mgr.terminate();
        }
    }

    @Test
    public void testNoDetector() throws Exception {
        ErrorCallback callback = mock(ErrorCallback.class);

        SplitBrainManager mgr = new SplitBrainManager(0, null);

        mgr.initialize(newNode(), mock(Executor.class), callback);

        assertNull(mgr.detector());
        assertTrue(mgr.check());
    }

    @Test
    public void testNotifyCallback() throws Exception {
        ErrorCallback callback = mock(ErrorCallback.class);

        SplitBrainManager mgr = new SplitBrainManager(0, null);

        mgr.initialize(newNode(), mock(Executor.class), callback);

        mgr.notifyOnSplitBrain();

        verify(callback).onError(any(ClusterSplitBrainException.class));
        verifyNoMoreInteractions(callback);
    }

    @Test
    public void testCheckAsync() throws Exception {
        SplitBrainDetector detector = mock(SplitBrainDetector.class);
        ErrorCallback callback = mock(ErrorCallback.class);

        SplitBrainManager mgr = new SplitBrainManager(0, detector);

        mgr.initialize(newNode(), Runnable::run, callback);

        when(detector.isValid(any())).thenReturn(false);

        mgr.checkAsync();

        verify(callback).onError(any(ClusterSplitBrainException.class));
    }

    @Test
    public void testCheckAsyncError() throws Exception {
        SplitBrainDetector detector = mock(SplitBrainDetector.class);
        ErrorCallback callback = mock(ErrorCallback.class);

        SplitBrainManager mgr = new SplitBrainManager(0, detector);

        mgr.initialize(newNode(), Runnable::run, callback);

        when(detector.isValid(any())).thenThrow(TEST_ERROR);

        mgr.checkAsync();

        verify(callback).onError(eq(TEST_ERROR));
    }

    @Test
    public void testStaleAsyncTaskAfterTerminate() throws Exception {
        SplitBrainDetector detector = mock(SplitBrainDetector.class);
        ErrorCallback callback = mock(ErrorCallback.class);
        Executor async = mock(Executor.class);
        ArgumentCaptor<Runnable> asyncCaptor = ArgumentCaptor.forClass(Runnable.class);

        SplitBrainManager mgr = new SplitBrainManager(0, detector);

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

        SplitBrainManager mgr = new SplitBrainManager(0, detector);

        mgr.initialize(newNode(), async, mock(ErrorCallback.class));

        for (int i = 0; i < 5; i++) {
            when(detector.isValid(any())).thenReturn(true);

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
        SplitBrainManager mgr = new SplitBrainManager(0, mock(SplitBrainDetector.class));

        assertEquals(ToString.format(mgr), mgr.toString());
    }
}
