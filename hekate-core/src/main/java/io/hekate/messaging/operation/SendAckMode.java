package io.hekate.messaging.operation;

/**
 * Acknowledgement mode.
 */
public enum SendAckMode {
    /** Require an acknowledgement. */
    REQUIRED,

    /** Do not require an acknowledgement. */
    NOT_NEEDED
}
