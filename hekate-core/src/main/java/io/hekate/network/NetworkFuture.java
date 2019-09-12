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

package io.hekate.network;

import java.util.concurrent.CompletableFuture;

/**
 * Result of an asynchronous operation in {@link NetworkEndpoint}.
 *
 * @param <T> Base type of {@link NetworkEndpoint}'s messages.
 */
public class NetworkFuture<T> extends CompletableFuture<NetworkEndpoint<T>> {
    /**
     * Returns completed future.
     *
     * @param endpoint Endpoint.
     * @param <T> Endpoint type.
     *
     * @return Completed future.
     */
    public static <T> NetworkFuture<T> completed(NetworkEndpoint<T> endpoint) {
        NetworkFuture<T> future = new NetworkFuture<>();

        future.complete(endpoint);

        return future;
    }
}
