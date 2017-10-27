package io.hekate.rpc.internal;

import io.hekate.rpc.RpcInterfaceInfo;
import io.hekate.rpc.RpcMethodInfo;

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
}
