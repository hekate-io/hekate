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

package io.hekate;

import io.hekate.network.NetworkTransportType;
import io.hekate.util.format.ToString;
import io.netty.channel.epoll.Epoll;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class HekateTestContext {
    private final NetworkTransportType transport;

    public HekateTestContext(NetworkTransportType transport) {
        this.transport = transport;
    }

    public HekateTestContext(HekateTestContext src) {
        this.transport = src.getTransport();
    }

    public static Stream<HekateTestContext> stream() {
        return get().stream();
    }

    public static Collection<HekateTestContext> get() {
        if (Epoll.isAvailable()) {
            return Arrays.asList(
                new HekateTestContext(NetworkTransportType.EPOLL),
                new HekateTestContext(NetworkTransportType.NIO)
            );
        } else {
            return Collections.singleton(
                new HekateTestContext(NetworkTransportType.NIO)
            );
        }
    }

    public static <T> List<T> map(Function<? super HekateTestContext, ? extends Stream<? extends T>> mapper) {
        return stream().flatMap(mapper).collect(toList());
    }

    public NetworkTransportType getTransport() {
        return transport;
    }

    @Override
    public String toString() {
        return ToString.formatProperties(this);
    }
}
