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

package io.hekate.cluster.internal;

import io.hekate.cluster.ClusterHash;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;

public class DefaultClusterHash implements ClusterHash, Serializable {
    private static final ThreadLocal<MessageDigest> THREAD_LOCAL_DIGEST = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Failed to initialize SHA-256 message digest.", e);
        }
    });

    private static final long serialVersionUID = 1;

    private static final ClusterNode[] EMPTY_NODES = new ClusterNode[0];

    private final byte[] bytes;

    private transient int hash;

    public DefaultClusterHash(Collection<ClusterNode> nodes) {
        MessageDigest digest = THREAD_LOCAL_DIGEST.get();

        ClusterNode[] sorted = nodes.toArray(EMPTY_NODES);

        Arrays.sort(sorted);

        int len = sorted.length;

        byte[] buf = new byte[len * Long.BYTES * 2];

        for (int i = 0, idx = 0; i < len; i++) {
            ClusterNodeId id = sorted[i].id();

            idx = setBytes(id.hiBits(), idx, buf);
            idx = setBytes(id.loBits(), idx, buf);
        }

        this.bytes = digest.digest(buf);
    }

    public DefaultClusterHash(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public byte[] bytes() {
        int len = bytes.length;

        byte[] copy = new byte[len];

        System.arraycopy(bytes, 0, copy, 0, len);

        return copy;
    }

    private static int setBytes(long v, int idx, byte[] buf) {
        buf[idx] = (byte)(v >>> 56);
        buf[idx + 1] = (byte)(v >>> 48);
        buf[idx + 2] = (byte)(v >>> 40);
        buf[idx + 3] = (byte)(v >>> 32);
        buf[idx + 4] = (byte)(v >>> 24);
        buf[idx + 5] = (byte)(v >>> 16);
        buf[idx + 6] = (byte)(v >>> 8);
        buf[idx + 7] = (byte)v;

        return idx + 8;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof DefaultClusterHash)) {
            return false;
        }

        DefaultClusterHash that = (DefaultClusterHash)o;

        return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        int hash = this.hash;

        if (hash == 0) {
            this.hash = hash = Arrays.hashCode(bytes);
        }

        return hash;
    }

    @Override
    public String toString() {
        return Base64.getUrlEncoder().encodeToString(bytes);
    }
}
