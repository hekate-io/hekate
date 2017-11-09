package io.hekate.test;

/** Unchecked error for tests. */
public class HekateTestError extends AssertionError {
    /** Common prefix for messages of expected errors that were produced by tests. */
    public static final String MESSAGE_PREFIX = "*** RELAX:)";

    /** Common message for expected errors that were produced by tests. */
    public static final String MESSAGE = MESSAGE_PREFIX + " This exception was created by the test case.";

    private static final long serialVersionUID = 1;

    public HekateTestError(String message) {
        super(message);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
