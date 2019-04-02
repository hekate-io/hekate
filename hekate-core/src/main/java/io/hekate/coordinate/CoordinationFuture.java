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

package io.hekate.coordinate;

import io.hekate.util.HekateFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Coordination future.
 *
 * @see CoordinationService#futureOf(String)
 */
public class CoordinationFuture extends HekateFuture<CoordinationProcess, CoordinationFuture> {
    @Override
    public CoordinationProcess get() throws InterruptedException, CoordinationException {
        try {
            return super.get();
        } catch (ExecutionException e) {
            throw new CoordinationException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public CoordinationProcess get(long timeout, TimeUnit unit) throws InterruptedException, CoordinationException, TimeoutException {
        try {
            return super.get(timeout, unit);
        } catch (ExecutionException e) {
            throw new CoordinationException(e.getMessage(), e.getCause());
        }
    }

    @Override
    protected CoordinationFuture newInstance() {
        return new CoordinationFuture();
    }
}
