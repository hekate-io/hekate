/*
 * Copyright 2021 The Hekate Project
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

package io.hekate.core;

import io.hekate.util.HekateFutureTestBase;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TerminateFutureTest extends HekateFutureTestBase<Hekate, TerminateFuture> {
    @Test
    public void testCompleted() {
        assertTrue(TerminateFuture.completed(mock(Hekate.class)).isSuccess());
    }

    @Override
    protected TerminateFuture createFuture() {
        return new TerminateFuture();
    }

    @Override
    protected Hekate createValue() {
        return mock(Hekate.class);
    }
}
