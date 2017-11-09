package io.hekate.test;

import io.hekate.codec.JavaSerializable;
import java.net.Socket;

public class NonSerializable implements JavaSerializable {
    public final Socket nonSerializableFiled = new Socket();
}
