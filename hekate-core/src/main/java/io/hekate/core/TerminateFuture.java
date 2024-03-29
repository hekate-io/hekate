/*
 * Copyright 2022 The Hekate Project
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

import io.hekate.util.HekateFuture;

/**
 * Asynchronous result of {@link Hekate#terminateAsync()} operation.
 *
 * @see Hekate#terminateAsync()
 */
public class TerminateFuture extends HekateFuture<Hekate, TerminateFuture> {
    /**
     * Creates a future object that is already {@link #complete(Object) completed}.
     *
     * @param node Node to complete the future.
     *
     * @return Future.
     */
    public static TerminateFuture completed(Hekate node) {
        TerminateFuture future = new TerminateFuture();

        future.complete(node);

        return future;
    }

    @Override
    protected TerminateFuture newInstance() {
        return new TerminateFuture();
    }
}
