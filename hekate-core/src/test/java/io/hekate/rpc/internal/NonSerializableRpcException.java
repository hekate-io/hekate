package io.hekate.rpc.internal;

import io.hekate.HekateTestBase;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.NotSerializableException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class NonSerializableRpcException extends RpcServiceUnicastTest.TestRpcException implements Externalizable {
    private static final long serialVersionUID = 1L;

    private final boolean failOnSerialize;

    public NonSerializableRpcException() {
        super(HekateTestBase.TEST_ERROR_MESSAGE);

        this.failOnSerialize = false;
    }

    public NonSerializableRpcException(boolean failOnSerialize) {
        super(HekateTestBase.TEST_ERROR_MESSAGE);

        this.failOnSerialize = failOnSerialize;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        if (failOnSerialize) {
            throw new NotSerializableException(HekateTestBase.TEST_ERROR_MESSAGE);
        }

        out.writeBoolean(false);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        if (!in.readBoolean()) {
            throw new InvalidObjectException(HekateTestBase.TEST_ERROR_MESSAGE);
        }
    }
}
