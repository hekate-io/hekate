package io.hekate.rpc;

import io.hekate.cluster.ClusterNode;
import io.hekate.core.internal.util.ArgAssert;
import java.util.Map;

/**
 * Signals that there was a failure while performing an RPC {@link RpcAggregate aggregation} request.
 *
 * <p>
 * Information about failed nodes and their corresponding errors can be obtained via {@link #errors()} method. Successful (partial) results
 * can be obtained via {@link #partialResults} method.
 * </p>
 */
public class RpcAggregateException extends RpcException {
    private static final long serialVersionUID = 1L;

    private final Map<ClusterNode, Throwable> errors;

    private final Map<ClusterNode, Object> partialResults;

    /**
     * Constructs a new instance.
     *
     * @param message Error message.
     * @param errors Map of failed nodes and their corresponding errors.
     * @param partialResults Map of successful nodes and their corresponding results.
     */
    public RpcAggregateException(String message, Map<ClusterNode, Throwable> errors, Map<ClusterNode, Object> partialResults) {
        super(message);

        ArgAssert.notNull(errors, "Errors");
        ArgAssert.notNull(partialResults, "Partial results");

        this.errors = errors;
        this.partialResults = partialResults;
    }

    /**
     * Returns the map of failed nodes and their corresponding errors.
     *
     * @return Map of failed nodes and their corresponding errors.
     */
    public Map<ClusterNode, Throwable> errors() {
        return errors;
    }

    /**
     * Returns the map of successful nodes and their corresponding results.
     *
     * @return Map of successful nodes and their corresponding results or an empty map if operation failed on all nodes.
     */
    public Map<ClusterNode, Object> partialResults() {
        return partialResults;
    }
}
