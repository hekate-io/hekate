/*
 * Copyright 2017 The Hekate Project
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

package io.hekate.lock.internal;

import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;

class LockAffinityKey {
    private final String region;

    private final String name;

    @ToStringIgnore
    private final int hash;

    public LockAffinityKey(String region, String name) {
        this.region = region;
        this.name = name;

        int hash = region.hashCode();

        hash = 31 * hash + name.hashCode();

        this.hash = hash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof LockAffinityKey)) {
            return false;
        }

        LockAffinityKey that = (LockAffinityKey)o;

        return that.region.equals(region) && that.name.equals(name);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
