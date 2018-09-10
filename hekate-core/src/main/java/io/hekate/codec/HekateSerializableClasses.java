package io.hekate.codec;

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.internal.DefaultClusterHash;
import io.hekate.cluster.internal.DefaultClusterNode;
import io.hekate.cluster.internal.DefaultClusterNodeRuntime;
import io.hekate.cluster.internal.DefaultClusterTopology;
import io.hekate.codec.fst.FstCodecFactory;
import io.hekate.codec.kryo.KryoCodecFactory;
import io.hekate.core.ServiceProperty;
import io.hekate.core.service.internal.DefaultServiceInfo;
import io.hekate.messaging.MessagingChannelId;
import java.util.SortedSet;
import java.util.TreeSet;

import static java.util.Collections.unmodifiableSortedSet;
import static java.util.Comparator.comparing;

/**
 * Registry of serializable Hekate-internal classes that are automatically registered to {@link KryoCodecFactory} and {@link
 * FstCodecFactory}.
 */
public final class HekateSerializableClasses {
    private static final SortedSet<Class<?>> CLASSES;

    static {
        SortedSet<Class<?>> classes = new TreeSet<>(comparing(Class::getName));

        classes.add(MessagingChannelId.class);
        classes.add(DefaultClusterHash.class);
        classes.add(DefaultServiceInfo.class);
        classes.add(ServiceProperty.class);
        classes.add(ClusterNodeId.class);
        classes.add(ClusterAddress.class);
        classes.add(DefaultClusterNodeRuntime.class);
        classes.add(DefaultClusterNode.class);
        classes.add(DefaultClusterTopology.class);

        CLASSES = unmodifiableSortedSet(classes);
    }

    private HekateSerializableClasses() {
        // No-op.
    }

    /**
     * Returns a sorted set of serializable Hekate-internal classes.
     *
     * @return Serializable Hekate-internal classes.
     */
    public static SortedSet<Class<?>> get() {
        return CLASSES;
    }
}
