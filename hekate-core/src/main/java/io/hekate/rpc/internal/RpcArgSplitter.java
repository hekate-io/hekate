package io.hekate.rpc.internal;

@FunctionalInterface
interface RpcArgSplitter {
    Object[] split(Object arg, int clusterSize);
}
