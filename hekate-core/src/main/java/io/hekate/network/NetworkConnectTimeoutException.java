package io.hekate.network;

/**
 * Signals a timeout while trying to connect to a remote host.
 */
public class NetworkConnectTimeoutException extends NetworkTimeoutException {
    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new instance.
     *
     * @param message Error message.
     * @param cause Cause of this error.
     */
    public NetworkConnectTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
