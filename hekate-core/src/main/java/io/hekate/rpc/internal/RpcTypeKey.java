package io.hekate.rpc.internal;

import io.hekate.core.internal.util.Utils;
import io.hekate.util.format.ToString;

class RpcTypeKey {
    private final Class<?> type;

    private final String tag;

    public RpcTypeKey(Class<?> type, String tag) {
        this.type = type;
        this.tag = Utils.nullOrTrim(tag);
    }

    public Class<?> type() {
        return type;
    }

    public String tag() {
        return tag;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof RpcTypeKey)) {
            return false;
        }

        RpcTypeKey rpcTypeKey = (RpcTypeKey)o;

        if (!type.equals(rpcTypeKey.type)) {
            return false;
        }

        return tag != null ? tag.equals(rpcTypeKey.tag) : rpcTypeKey.tag == null;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();

        result = 31 * result + (tag != null ? tag.hashCode() : 0);

        return result;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
