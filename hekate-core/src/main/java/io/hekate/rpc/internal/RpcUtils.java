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
