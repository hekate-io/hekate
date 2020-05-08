/*
 * Copyright 2020 The Hekate Project
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

package io.hekate.coordinate.internal;

import io.hekate.cluster.ClusterNodeId;
import java.util.Objects;

class CoordinationEpoch {
    private final ClusterNodeId coordinator;

    private final long id;

    public CoordinationEpoch(ClusterNodeId coordinator, long id) {
        this.coordinator = coordinator;
        this.id = id;
    }

    public ClusterNodeId coordinator() {
        return coordinator;
    }

    public long id() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof CoordinationEpoch)) {
            return false;
        }

        CoordinationEpoch that = (CoordinationEpoch)o;

        return id == that.id
            && Objects.equals(coordinator, that.coordinator);
    }

    @Override
    public int hashCode() {
        int result = coordinator != null ? coordinator.hashCode() : 0;

        result = 31 * result + (int)(id ^ (id >>> 32));

        return result;
    }

    @Override
    public String toString() {
        return coordinator.toString() + ':' + id;
    }
}
