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

package io.hekate.network;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NetworkServerCallbackMock implements NetworkServerCallback {
    private final AtomicInteger starts = new AtomicInteger();

    private final AtomicInteger stops = new AtomicInteger();

    private final List<Throwable> errors = new CopyOnWriteArrayList<>();

    public void assertStarts(int starts) {
        assertEquals(starts, this.starts.get());
    }

    public void assertStops(int stops) {
        assertEquals(stops, this.stops.get());
    }

    public void assertErrors(int errors) {
        assertEquals(errors, this.errors.size());
    }

    public void assertNoErrors() {
        assertTrue(errors.isEmpty());
    }

    public List<Throwable> getErrors() {
        return errors;
    }

    @Override
    public void onStart(NetworkServer server) {
        starts.incrementAndGet();
    }

    @Override
    public void onStop(NetworkServer server) {
        stops.incrementAndGet();
    }

    @Override
    public NetworkServerFailure.Resolution onFailure(NetworkServer server, NetworkServerFailure failure) {
        errors.add(failure.cause());

        return failure.fail();
    }
}
