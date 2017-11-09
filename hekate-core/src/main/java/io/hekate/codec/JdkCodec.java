package io.hekate.codec;

import io.hekate.util.format.ToString;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

class JdkCodec implements Codec<Object> {
    @Override
    public boolean isStateful() {
        return false;
    }

    @Override
    public Class<Object> baseType() {
        return Object.class;
    }

    @Override
    public Object decode(DataReader in) throws IOException {
        try (ObjectInputStream objIn = new ObjectInputStream(in.asStream())) {
            return objIn.readObject();
        } catch (ClassNotFoundException e) {
            throw new CodecException("Failed to deserialize message.", e);
        }
    }

    @Override
    public void encode(Object message, DataWriter out) throws IOException {
        try (ObjectOutputStream objOut = new ObjectOutputStream(out.asStream())) {
            objOut.writeObject(message);
        }
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
