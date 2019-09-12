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
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation for RPC interfaces.
 *
 * <p>
 * This annotation that can be placed on a Java interface in order to expose its methods for remote access.
 * </p>
 *
 * @see RpcService
 */
@Inherited
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Rpc {
    /**
     * Version of RPC interface.
     *
     * @return Version of RPC interface.
     *
     * @see #minClientVersion()
     */
    int version() default 0;

    /**
     * Minimum version of a client interface that is supported by this RPC interface.
     *
     * <p>
     * This attribute can be used to control compatibility between the client and the server interfaces. If client detects that its version
     * is less than the minimum required version of the server then such client will not route any RPC requests to such server.
     * </p>
     *
     * <p>
     * Consider the following scenario:
     * </p>
     * <ol>
     * <li>Same jar with RPC version {@code 1} is deployed both on the client and on the server nodes</li>
     * <li>After some time a new RPC version {@code 2} is implemented (possibly with some breaking changes of API)</li>
     * <li>New jar file with RPC version {@code 2} is deployed on a new node</li>
     * <li>At this point, if {@code minClientVersion()} is set to {@code 2} then old client with version {@code 1} will know that its API
     * is not compatible with the server version {@code 2} and will not try to route any requests to such server.</li>
     * <li>Alternatively, if {@code minClientVersion()} is set to {@code 1} (meaning that there were no breaking changes) then old client
     * will still be able to route requests to the new server.</li>
     * </ol>
     *
     * @return Minimum version of a client interface that is compatible with the server version of this interface.
     *
     * @see #version()
     */
    int minClientVersion() default 0;
}
