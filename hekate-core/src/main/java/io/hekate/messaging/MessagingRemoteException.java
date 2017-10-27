package io.hekate.messaging;

/**
 * Signals that response message couldn't be received due to an error on a remote node.
 *
 * @see #remoteStackTrace()
 */
public class MessagingRemoteException extends MessagingException {
    private static final long serialVersionUID = 1L;

    private final String remoteStackTrace;

    /**
     * Constructs new instance.
     *
     * @param message Error message.
     * @param remoteStackTrace Stack trace of a remote failure (see {@link #remoteStackTrace()}).
     */
    public MessagingRemoteException(String message, String remoteStackTrace) {
        super(message + System.lineSeparator()
            + "[--- Remote stack trace start ---]" + System.lineSeparator()
            + remoteStackTrace
            + "[--- Remote stack trace end ---]"
        );

        this.remoteStackTrace = remoteStackTrace;
    }

    /**
     * Returns the stack trace of a remote failure.
     *
     * @return Stack trace of a remote failure.
     */
    public String remoteStackTrace() {
        return remoteStackTrace;
    }
}
