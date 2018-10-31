package io.hekate.messaging.unicast;

/**
 * Acknowledgement mode.
 */
public enum SendAckMode {
    /** Require an acknowledgement. */
    REQUIRED,

    /** Do not require an acknowledgement. */
    NOT_NEEDED
}
