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

package io.hekate.rpc.internal;

import io.hekate.core.internal.util.Utils;
import io.hekate.util.format.ToString;
import java.util.Objects;

class RpcTypeKey {
    private final Class<?> type;

    private final String tag;

    public RpcTypeKey(Class<?> type, String tag) {
        this.type = type;
        this.tag = Utils.nullOrTrim(tag);
    }

    public Class<?> type() {
        return type;
    }

    public String tag() {
        return tag;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof RpcTypeKey)) {
            return false;
        }

        RpcTypeKey rpcTypeKey = (RpcTypeKey)o;

        if (!type.equals(rpcTypeKey.type)) {
            return false;
        }

        return Objects.equals(tag, rpcTypeKey.tag);
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();

        result = 31 * result + (tag != null ? tag.hashCode() : 0);

        return result;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
