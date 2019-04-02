/*
 * Copyright 2019 The Hekate Project
 *
 * The Hekate Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

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
