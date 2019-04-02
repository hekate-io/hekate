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

import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.core.ServiceInfo;
import io.hekate.rpc.RpcInterfaceInfo;
import io.hekate.rpc.RpcMethodInfo;
import io.hekate.rpc.RpcService;
import io.hekate.rpc.internal.RpcProtocol.RpcCallResult;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class RpcUtils {
    private RpcUtils() {
        // No-op.
    }

    public static String versionProperty(RpcInterfaceInfo rpc) {
        return rpc.versionedName();
    }

    public static String taggedVersionProperty(RpcInterfaceInfo rpc, String tag) {
        return rpc.versionedName() + '#' + tag;
    }

    public static String methodProperty(RpcInterfaceInfo rpc, RpcMethodInfo method) {
        return versionProperty(rpc) + ':' + method.signature();
    }

    public static String taggedMethodProperty(RpcInterfaceInfo rpc, RpcMethodInfo method, String tag) {
        return taggedVersionProperty(rpc, tag) + ':' + method.signature();
    }

    public static ClusterNodeFilter filterFor(RpcInterfaceInfo<?> type, String tag) {
        String versionProp;

        if (tag == null) {
            versionProp = versionProperty(type);
        } else {
            versionProp = taggedVersionProperty(type, tag);
        }

        return node -> {
            ServiceInfo service = node.service(RpcService.class);

            Integer minClientVer = service.intProperty(versionProp);

            return minClientVer != null && minClientVer <= type.version();
        };
    }

    static void mergeToMap(RpcProtocol from, Map<Object, Object> to) {
        if (from instanceof RpcCallResult) {
            @SuppressWarnings("unchecked")
            Map<Object, Object> part = (Map<Object, Object>)((RpcCallResult)from).result();

            to.putAll(part);
        }
    }

    static void mergeToSet(RpcProtocol from, Set<Object> to) {
        if (from instanceof RpcCallResult) {
            @SuppressWarnings("unchecked")
            Collection<Object> part = (Collection<Object>)((RpcCallResult)from).result();

            to.addAll(part);
        }
    }

    static void mergeToList(RpcProtocol from, List<Object> to) {
        if (from instanceof RpcCallResult) {
            @SuppressWarnings("unchecked")
            Collection<Object> part = (Collection<Object>)((RpcCallResult)from).result();

            to.addAll(part);
        }
    }
}
