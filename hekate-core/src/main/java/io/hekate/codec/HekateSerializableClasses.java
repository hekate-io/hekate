/*
 * Copyright 2021 The Hekate Project
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
