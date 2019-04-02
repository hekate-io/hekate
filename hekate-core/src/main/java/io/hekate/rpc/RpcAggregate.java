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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Enables aggregation on RPC interface's methods.
 *
 * <p>
 * This annotation can be placed on a method of an @{@link Rpc}-enabled interface in order to execute such RPC method on all of the RPC
 * cluster nodes in parallel. Once execution is completed, all results are aggregated and returned as a single result.
 * </p>
 *
 * <p>
 * Such method must have one of the following return types:
 * </p>
 *
 * <ul>
 * <li>{@link List}</li>
 * <li>{@link Set}</li>
 * <li>{@link Map}</li>
 * <li>{@link Collection}</li>
 * <li>{@link CompletableFuture}{@code <}{@link List}|{@link Set}|{@link Collection}|{@link Map}{@code >}</li>
 * </ul>
 *
 * <p>
 * Aggregation error handling policy can be specified in {@link #remoteErrors()} attribute:
 * </p>
 * <ul>
 * <li>{@link RemoteErrors#IGNORE} - Ignore all remote errors and return whatever results were successfully aggregated or an empty result if
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
public @interface RpcAggregate {
    /**
     * Remote errors handling policy for RPC aggregation.
     */
    enum RemoteErrors {
        /**
         * Ignore all remote errors and return whatever results were successfully aggregated or an empty result if RPC failed on all nodes.
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
         * @see RpcAggregateException#partialResults()
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
