package io.hekate.rpc;

import io.hekate.core.HekateUncheckedException;

/**
 * Generic error that signals an RPC operation failure.
 */
public class RpcException extends HekateUncheckedException {
    private static final long serialVersionUID = 1;

    /**
     * Constructs a new instance.
     *
     * @param message Error message.
     */
    public RpcException(String message) {
        super(message);
    }

    /**
     * Constructs a new instance.
     *
     * @param message Error message.
     * @param cause Error cause.
     */
    public RpcException(String message, Throwable cause) {
        super(message, cause);
    }
}
