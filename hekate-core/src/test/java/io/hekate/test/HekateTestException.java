package io.hekate.test;

import io.hekate.core.HekateException;

/** Checked error for tests. */
public class HekateTestException extends HekateException {
    private static final long serialVersionUID = 1;

    public HekateTestException(String message) {
        super(message);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
