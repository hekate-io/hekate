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
