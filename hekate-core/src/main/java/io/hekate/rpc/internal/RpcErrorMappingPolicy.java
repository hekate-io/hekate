package io.hekate.rpc.internal;

@FunctionalInterface
interface RpcErrorMappingPolicy {
    Throwable apply(Throwable err);
}
