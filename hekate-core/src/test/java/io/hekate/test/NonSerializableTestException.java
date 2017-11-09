package io.hekate.test;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.NotSerializableException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class NonSerializableTestException extends SerializableTestException implements Externalizable {
    private static final long serialVersionUID = 1L;

    private final boolean failOnSerialize;

    public NonSerializableTestException() {
        this.failOnSerialize = false;
    }

    public NonSerializableTestException(boolean failOnSerialize) {
        this.failOnSerialize = failOnSerialize;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        if (failOnSerialize) {
            throw new NotSerializableException(HekateTestError.MESSAGE);
        }

        out.writeBoolean(false);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        if (!in.readBoolean()) {
            throw new InvalidObjectException(HekateTestError.MESSAGE);
        }
    }
}
