package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.messaging.broadcast.Aggregate;
import io.hekate.messaging.broadcast.AggregateFuture;
import io.hekate.messaging.loadbalance.UnknownRouteException;
import java.util.List;

import static io.hekate.messaging.internal.BroadcastOperationBuilder.nodesForBroadcast;

class AggregateOperationBuilder<T> extends MessageOperationBuilder<T> implements Aggregate<T> {
    private Object affinity;

    public AggregateOperationBuilder(T message, MessagingGatewayContext<T> gateway, MessageOperationOpts<T> opts) {
        super(message, gateway, opts);
    }

    @Override
    public Aggregate<T> withAffinity(Object affinity) {
        this.affinity = affinity;

        return this;
    }

    @Override
    public AggregateFuture<T> execute() {
        Object affinity = this.affinity;

        AggregateFuture<T> future = new AggregateFuture<>();

        List<ClusterNode> nodes = nodesForBroadcast(affinity, opts());

        if (nodes.isEmpty()) {
            future.complete(new EmptyAggregateResult<>(message()));
        } else {
            AggregateContext<T> ctx = new AggregateContext<>(message(), nodes, future);

            nodes.forEach(node -> {
                AggregateOperation<T> op = new AggregateOperation<>(message(), affinity, gateway(), opts(), node);

                gateway().submit(op);

                op.future().whenComplete((result, err) -> {
                    boolean complete;

                    if (err == null) {
                        complete = ctx.onReplySuccess(node, result);
                    } else if (err instanceof UnknownRouteException) {
                        // Special case for unknown routes.
                        //-----------------------------------------------
                        // Can happen in some rare cases if node leaves the cluster at the same time with this operation.
                        // We exclude such nodes from the operation's results as if it had left the cluster right before
                        // we've started the operation.
                        complete = ctx.forgetNode(node);
                    } else {
                        complete = ctx.onReplyFailure(node, err);
                    }

                    if (complete) {
                        ctx.complete();
                    }
                });
            });
        }

        return future;
    }
}
