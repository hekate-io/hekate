/*
 * Copyright 2018 The Hekate Project
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
import io.hekate.rpc.RpcMethodInfo;
import io.hekate.rpc.internal.RpcProtocol.ObjectResponse;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class RpcUtils {
    private RpcUtils() {
        // No-op.
    }

    public static String nameProperty(RpcInterfaceInfo rpc) {
        return rpc.versionedName();
    }

    public static String taggedNameProperty(RpcInterfaceInfo rpc, String tag) {
        return rpc.versionedName() + '#' + tag;
    }

    public static String methodProperty(RpcInterfaceInfo rpc, RpcMethodInfo method) {
        return rpc.versionedName() + ':' + method.signature();
    }

    public static String taggedMethodProperty(RpcInterfaceInfo rpc, RpcMethodInfo method, String tag) {
        return rpc.versionedName() + '#' + tag + ':' + method.signature();
    }

    static void mergeToMap(RpcProtocol from, Map<Object, Object> to) {
        if (from instanceof ObjectResponse) {
            @SuppressWarnings("unchecked")
            Map<Object, Object> part = (Map<Object, Object>)((ObjectResponse)from).object();

            to.putAll(part);
        }
    }

    static void mergeToSet(RpcProtocol from, Set<Object> to) {
        if (from instanceof ObjectResponse) {
            @SuppressWarnings("unchecked")
            Collection<Object> part = (Collection<Object>)((ObjectResponse)from).object();

            to.addAll(part);
        }
    }

    static void mergeToList(RpcProtocol from, List<Object> to) {
        if (from instanceof ObjectResponse) {
            @SuppressWarnings("unchecked")
            Collection<Object> part = (Collection<Object>)((ObjectResponse)from).object();

            to.addAll(part);
        }
    }
}
