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

/**
 * Marks one of an RPC method's parameters as being eligible for splitting and parallel processing.
 *
 * <p>
 * This annotation can be placed on one of an @{@link RpcAggregate}-annotated method's parameters in order to instruct the {@link
 * RpcService} that such parameter must be split and processed in parallel by multiple cluster nodes. Such parameter must be of one of the
 * following types:
 * </p>
 * <ul>
 * <li>{@link List}</li>
 * <li>{@link Set}</li>
 * <li>{@link Map}</li>
 * <li>{@link Collection}</li>
 * </ul>
 *
 * <p>
 * If this annotation is present then the value of an annotated parameter will be split into smaller chunks (sub-collections) based
 * on the number of available cluster nodes. All such chunks will be evenly distributed among the cluster nodes for parallel processing and
 * once processing on all nodes is completed then results of each node will be aggregated the same way as during the regular {@link
 * RpcAggregate}
 * call.
 * </p>
 *
 * @see RpcService
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface RpcSplit {
    // No-op.
}
