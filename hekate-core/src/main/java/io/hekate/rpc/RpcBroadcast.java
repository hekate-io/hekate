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

package io.hekate.rpc;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.CompletableFuture;

/**
 * Enables broadcast on RPC interface's methods.
 *
 * <p>
 * This annotation can be placed on a method of an @{@link Rpc}-enabled interface in order to execute such RPC method on all of the RPC
 * cluster nodes in parallel.
 * </p>
 *
 * <p>
 * Methods that are annotated with {@link RpcBroadcast} must have a {@code void} (for synchronous calls) or a {@link CompletableFuture}
 * (for asynchronous calls) return type. If method is declared as returning an a {@link CompletableFuture} then it is recommended to
 * parametrise such future with {@code <?>} or {@code <Void>} as such futures are always get completed with a {@code null} value by the RPC
 * service.
 * </p>
 *
 * <p>
 * Aggregation error handling policy can be specified in {@link #remoteErrors()} attribute:
 * </p>
 * <ul>
 * <li>{@link RemoteErrors#IGNORE} - Ignore all remote errors and return whatever results were successfully aggregated or an empty result
 * if
 * RPC failed on all nodes.</li>
 * <li>{@link RemoteErrors#WARN} - Same as {@link RemoteErrors#IGNORE} but also logs a WARN message for each failure.</li>
 * <li>{@link RemoteErrors#FAIL} - In case of any error fail the whole aggregation with {@link RpcAggregateException}.</li>
 * </ul>
 *
 * <p>
 * For more details about the Remote Procedure Call API and its capabilities please see the documentation of the {@link RpcService}
 * interface.
 * </p>
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RpcBroadcast {
    /**
     * Remote errors handling policy for RPC aggregation.
     */
    enum RemoteErrors {
        /**
         * Ignore all remote errors.
         */
        IGNORE,

        /**
         * Same as {@link #IGNORE} but also logs a WARN message for each failure.
         */
        WARN,

        /**
         * In case of any error fail the whole aggregation with {@link RpcAggregateException}.
         *
         * @see RpcAggregateException#errors()
         */
        FAIL
    }

    /**
     * Remote errors handling policy (see description of {@link RemoteErrors}'s values).
     *
     * @return Remote errors handling policy.
     */
    RemoteErrors remoteErrors() default RemoteErrors.FAIL;
}
