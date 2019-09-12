/*
 * Copyright 2019 The Hekate Project
 *
 * The Hekate Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

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

    private static final int MAX_ERRORS_IN_SUMMARY = 3;

    private static final String PADDING = "    ";

    private static final String NEW_LINE = System.lineSeparator() + PADDING + "#";

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
        super(message + remoteErrorsSummary(errors));

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

    private static String remoteErrorsSummary(Map<ClusterNode, Throwable> errors) {
        StringBuilder buf = new StringBuilder(NEW_LINE)
            .append(" ---")
            .append(NEW_LINE)
            .append(" Remote errors summary (total-errors=").append(errors.size()).append("):");

        errors.entrySet().stream()
            .limit(MAX_ERRORS_IN_SUMMARY)
            .forEach(e -> buf.append(NEW_LINE)
                .append(PADDING)
                .append(e.getKey()).append(" -> ")
                .append(e.getValue()));

        if (errors.size() > MAX_ERRORS_IN_SUMMARY) {
            buf.append(NEW_LINE)
                .append(PADDING)
                .append('(').append(errors.size() - MAX_ERRORS_IN_SUMMARY).append(" more...)");
        }

        buf.append(NEW_LINE).append(" ---");

        return buf.toString();
    }
}
