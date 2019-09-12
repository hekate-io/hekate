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

package io.hekate.codec.fst;

import io.hekate.codec.Codec;
import io.hekate.codec.DataReader;
import io.hekate.codec.DataWriter;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.io.IOException;
import java.io.NotSerializableException;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.FSTObjectOutput;

class FstCodec implements Codec<Object> {
    @ToStringIgnore
    private final FSTConfiguration fst;

    public FstCodec(FSTConfiguration fst) {
        assert fst != null : "FST configuration is null.";

        this.fst = fst;
    }

    @Override
    public boolean isStateful() {
        return false;
    }

    @Override
    public Class<Object> baseType() {
        return Object.class;
    }

    @Override
    public void encode(Object obj, DataWriter out) throws IOException {
        try {
            FSTObjectOutput objOut = fst.getObjectOutput(out.asStream());

            objOut.writeObject(obj);
            objOut.flush();
        } catch (RuntimeException e) {
            // Workaround for FST throwing RuntimeException instead of NotSerializableException.
            if (e.getMessage() != null && e.getMessage().indexOf("does not implement Serializable") > 0) {
                NotSerializableException converted = new NotSerializableException(e.getMessage());

                converted.setStackTrace(e.getStackTrace());

                throw converted;
            } else {
                throw e;
            }
        }
    }

    @Override
    public Object decode(DataReader in) throws IOException {
        try {
            return fst.getObjectInput(in.asStream()).readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize object from stream.", e);
        }
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
