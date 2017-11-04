package io.hekate.network.netty;

interface NettySpy {
    void onBeforeFlush(Object msg) throws Exception;
}
