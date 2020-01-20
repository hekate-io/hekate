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
