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

/**
 * Handler for {@link CoordinationProcess}.
 *
 * <p>
 * Implementations of this interface are responsible for coordination logic within the {@link CoordinationService} and can be registered
 * via {@link CoordinationProcessConfig#setHandler(CoordinationHandler)} method.
 * </p>
 *
 * <p>
 * Methods of this interface are called in the following order:
 * </p>
 * <ul>
 * <li>{@link #initialize()} - gets called during the {@link CoordinationService} initialization</li>
 * <li>{@link #prepare(CoordinationContext)} - gets called once per coordination round in order to prepare this handler</li>
 * <li>{@link #coordinate(CoordinatorContext)} - gets called only on the coordinator node in order start coordination. Once coordination is
 * done the {@link CoordinatorContext#complete()} method must be called</li>
 * <li>{@link #process(CoordinationRequest, CoordinationContext)} gets called when a new request is received either from a coordinator or
 * from some other member</li>
 * <li>{@link #complete(CoordinationContext)} - gets called if was completed successfully</li>
 * <li>{@link #cancel(CoordinationContext)} - gets called if concurrent cluster topology change was detected and current coordination
 * process is cancelled</li>
 * <li>{@link #terminate()} - gets called during the {@link CoordinationService} termination</li>
 * </ul>
 *
 * <p>
 * For more details and examples please see the documentation of {@link CoordinationService} interface.
 * </p>
 */
public interface CoordinationHandler {
    /**
     * Signals the beginning of a new coordination process.
     *
     * <p>
     * Implementations of this method can perform initial preparations and initialize all necessary data structures for the new
     * coordination process.
     * </p>
     *
     * @param ctx Coordination context.
     */
    void prepare(CoordinationContext ctx);

    /**
     * Gets called when local node is selected to be the coordinator.
     *
     * <p>
     * Implementations of this method should start executing coordination logic by communicating with other
     * {@link CoordinationContext#members() coordination memebers}. Once coordination is completed the {@link CoordinatorContext#complete()}
     * method must be called.
     * </p>
     *
     * @param ctx Coordination context.
     */
    void coordinate(CoordinatorContext ctx);

    /**
     * Gets called when a new request is received from a {@link CoordinationRequest#from() coordination member}.
     *
     * <p>
     * <b>Important!!!</b> Each request must be explicitly replied via {@link CoordinationRequest#reply(Object)} method. Not replying to
     * requests can lead to memory leaks since each coordination member keeps an in-memory structure to track sent requests and releases
     * this memory only when request gets a reply.
     * </p>
     *
     * @param request Request.
     * @param ctx Coordination context.
     */
    void process(CoordinationRequest request, CoordinationContext ctx);

    /**
     * Gets called if current coordination process gets cancelled.
     *
     * <p>
     * Default implementation of this method is empty.
     * </p>
     *
     * @param ctx Coordination context.
     */
    default void cancel(CoordinationContext ctx) {
        // No-op.
    }

    /**
     * Gets called when coordination process gets completed successfully.
     *
     * <p>
     * Default implementation of this method is empty.
     * </p>
     *
     * @param ctx Coordination context.
     */
    default void complete(CoordinationContext ctx) {
        // No-op.
    }

    /**
     * Gets called during the {@link CoordinationService} initialization.
     *
     * <p>
     * Default implementation of this method is empty.
     * </p>
     */
    default void initialize() {
        // No-op.
    }

    /**
     * Gets called during the {@link CoordinationService} initialization.
     *
     * <p>
     * Default implementation of this method is empty.
     * </p>
     */
    default void terminate() {
        // No-op.
    }
}
