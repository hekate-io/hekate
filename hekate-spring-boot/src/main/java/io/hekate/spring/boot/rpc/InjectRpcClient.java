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

package io.hekate.spring.boot.rpc;

import io.hekate.rpc.RpcClientConfig;
import io.hekate.rpc.RpcService;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Provides support for RPC clients autowiring.
 *
 * <p>
 * This annotation can be placed on any {@link Autowired autowire}-capable elements (fields, properties, parameters, etc) of application
 * beans in order to inject an instance of RPC client proxy (see {@link RpcService#clientFor(Class)}) into that element.
 * </p>
 *
 * @see HekateRpcServiceConfigurer
 * @see RpcService#clientFor(Class)
 */
@Autowired
@Qualifier
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER, ElementType.ANNOTATION_TYPE})
public @interface InjectRpcClient {
    /**
     * Specifies the RPC tag that should be used for this client (see {@link RpcClientConfig#setTag(String)}).
     *
     * @return RPC tag.
     */
    String tag() default "";
}
