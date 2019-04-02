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

import io.hekate.cluster.event.ClusterLeaveEvent;
import io.hekate.core.Hekate;
import io.hekate.core.HekateException;

/**
 * Lifecycle interface for services that require termination.
 *
 * <p>
 * Methods of this interface get invoked when {@link Hekate} instance starts {@link Hekate#leave() leaving} the cluster and during {@link
 * Hekate#terminate() termination}.
 * </p>
 * <p>
 * Invocation order is:
 * </p>
 * <ol>
 * <li>Fire {@link ClusterLeaveEvent} (if joined to the cluster)</li>
 * <li>{@link #preTerminate()} all services one by one</li>
 * <li>Await for {@link ClusterContext#onLeave()} notification from the cluster service</li>
 * <li>{@link #terminate()} all services one by one</li>
 * <li>{@link #postTerminate()} all services one by one</li>
 * </ol>
 *
 * <p>
 * <b>Note:</b> Termination order takes dependencies into account. If some service {@code A} {@link DependentService depends} on some
 * other service {@code B} then it is guaranteed that termination methods of service {@code B} will be called after initialization
 * methods of service {@code A}.
 * </p>
 *
 * @see InitializingService
 * @see DependentService
 */
public interface TerminatingService {
    /**
     * Terminates this service.
     *
     * @throws HekateException If termination failed.
     */
    void terminate() throws HekateException;

    /**
     * Pre-terminates this service.
     *
     * @throws HekateException If pre-termination failed.
     */
    default void preTerminate() throws HekateException {
        // No-op.
    }

    /**
     * Post-terminates this service.
     *
     * @throws HekateException If post-termination failed.
     */
    default void postTerminate() throws HekateException {
        // No-op.
    }
}
