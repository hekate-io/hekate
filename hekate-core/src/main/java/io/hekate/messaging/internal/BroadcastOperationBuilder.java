package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.messaging.broadcast.Broadcast;
import io.hekate.messaging.broadcast.BroadcastFuture;
import io.hekate.messaging.loadbalance.UnknownRouteException;
import java.util.List;

class BroadcastOperationBuilder<T> extends MessageOperationBuilder<T> implements Broadcast<T> {
    private Object affinity;

    private boolean confirmReceive;

    public BroadcastOperationBuilder(T message, MessagingGatewayContext<T> gateway, MessageOperationOpts<T> opts) {
        super(message, gateway, opts);
    }

    @Override
    public Broadcast<T> withAffinity(Object affinity) {
        this.affinity = affinity;

        return this;
    }

    @Override
    public Broadcast<T> withConfirmReceive(boolean confirmReceive) {
        this.confirmReceive = confirmReceive;

        return this;
    }

    @Override
    public BroadcastFuture<T> submit() {
        Object affinity = this.affinity;

        BroadcastFuture<T> future = new BroadcastFuture<>();

        List<ClusterNode> nodes = nodesForBroadcast(affinity, opts());

        if (nodes.isEmpty()) {
            future.complete(new EmptyBroadcastResult<>(message()));
        } else {
            BroadcastContext<T> ctx = new BroadcastContext<>(message(), nodes, future);

            nodes.forEach(node -> {
                BroadcastOperation<T> op = new BroadcastOperation<>(message(), affinity, gateway(), opts(), confirmReceive, node);

                gateway().submit(op);

                op.future().whenComplete((ignore, err) -> {
                    boolean complete;

                    if (err == null) {
                        complete = ctx.onSendSuccess();
                    } else if (err instanceof UnknownRouteException) {
                        // Special case for unknown routes.
                        //-----------------------------------------------
                        // Can happen in some rare cases if node leaves the cluster at the same time with this operation.
                        // We exclude such nodes from the operation's results as if it had left the cluster right before
                        // we've started the operation.
                        complete = ctx.forgetNode(node);
                    } else {
                        complete = ctx.onSendFailure(node, err);
                    }

                    if (complete) {
                        ctx.complete();
                    }
                });
            });
        }

        return future;
    }

    static List<ClusterNode> nodesForBroadcast(Object affinityKey, MessageOperationOpts<?> opts) {
        List<ClusterNode> nodes;

        if (affinityKey == null) {
            // Use the whole topology if affinity key is not specified.
            nodes = opts.cluster().topology().nodes();
        } else {
            // Use only those nodes that are mapped to the affinity key.
            nodes = opts.partitions().map(affinityKey).nodes();
        }

        return nodes;
    }
}
