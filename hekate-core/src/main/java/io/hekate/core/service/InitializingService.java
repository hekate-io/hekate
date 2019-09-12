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

package io.hekate.core.service;

import io.hekate.core.Hekate;
import io.hekate.core.HekateException;
import java.util.Set;

/**
 * Lifecycle interface for services that require initialization.
 *
 * <p>
 * Methods of this interface get invoked when {@link Hekate} instance starts {@link Hekate#join() joining} the cluster.
 * </p>
 * <p>
 * Invocation order is:
 * </p>
 * <ol>
 * <li>{@link #preInitialize(InitializationContext)} all services one by one</li>
 * <li>Await for {@link ClusterContext#onJoin(int, Set)} notification from the cluster service</li>
 * <li>{@link #initialize(InitializationContext)} all services one by one</li>
 * <li>{@link #postInitialize(InitializationContext)} all services one by one</li>
 * </ol>
 *
 * <p>
 * <b>Note:</b> Initialization order takes dependencies into account. If some service {@code A} {@link DependentService depends} on some
 * other service {@code B} then it is guaranteed that initialization methods of service {@code B} will be called prior to initialization
 * methods of service {@code A}.
 * </p>
 *
 * @see TerminatingService
 * @see DependentService
 */
public interface InitializingService {
    /**
     * Initializes this service.
     *
     * @param ctx Context.
     *
     * @throws HekateException If initialization failed.
     */
    void initialize(InitializationContext ctx) throws HekateException;

    /**
     * Pre-initializes this service.
     *
     * <p>
     * Default implementation of this method is empty.
     * </p>
     *
     * @param ctx Context.
     *
     * @throws HekateException If pre-initialization failed.
     */
    default void preInitialize(InitializationContext ctx) throws HekateException {
        // No-op.
    }

    /**
     * Post-initializes this service.
     *
     * <p>
     * Default implementation of this method is empty.
     * </p>
     *
     * @param ctx Context.
     *
     * @throws HekateException If post-initialization failed.
     */
    default void postInitialize(InitializationContext ctx) throws HekateException {
        // No-op.
    }
}
