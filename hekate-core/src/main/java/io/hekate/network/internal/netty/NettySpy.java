package io.hekate.network.internal.netty;

interface NettySpy {
    void onBeforeFlush(Object msg) throws Exception;
}
