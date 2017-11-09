package io.hekate.test;

public class SerializableTestException extends Exception {
    private static final long serialVersionUID = 1;

    public SerializableTestException() {
        super(HekateTestError.MESSAGE);
    }
}
