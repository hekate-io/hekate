package io.hekate.network;

import java.io.IOException;

/**
 * Signals a network operation timed out.
 */
public class NetworkTimeoutException extends IOException {
    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new instance.
     *
     * @param message Error message.
     * @param cause Cause of this error.
     */
    public NetworkTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
