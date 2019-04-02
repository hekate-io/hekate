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

package io.hekate.rpc.internal;

import io.hekate.rpc.RpcInterfaceInfo;
import io.hekate.util.format.ToString;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

class RpcInterface<T> {
    private final RpcInterfaceInfo<T> type;

    private final List<RpcMethodHandler> methods;

    public RpcInterface(RpcInterfaceInfo<T> type, Object target) {
        this.type = type;
        this.methods = unmodifiableList(type.methods().stream()
            .map(method -> {
                if (method.aggregate().isPresent() && method.splitArg().isPresent()) {
                    return new RpcSplitAggregateMethodHandler(type, method, target);
                } else {
                    return new RpcMethodHandler(type, method, target);
                }
            })
            .collect(toList())
        );
    }

    public RpcInterfaceInfo<T> type() {
        return type;
    }

    public List<RpcMethodHandler> methods() {
        return methods;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
