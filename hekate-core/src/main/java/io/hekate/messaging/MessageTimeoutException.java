package io.hekate.messaging;

import java.util.concurrent.TimeUnit;

/**
 * Signals that messaging operation timed out.
 *
 * @see MessagingChannelConfig#setMessagingTimeout(long)
 * @see MessagingChannel#withTimeout(long, TimeUnit)
 */
public class MessageTimeoutException extends MessagingException {
    private static final long serialVersionUID = 1599179762571202891L;

    /**
     * Constructs new instance.
     *
     * @param message Error message.
     */
    public MessageTimeoutException(String message) {
        super(message);
    }
}
