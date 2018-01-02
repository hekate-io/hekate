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
                    return new RpcSplitAggregateMethodHandler(method, target);
                } else {
                    return new RpcMethodHandler(method, target);
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
