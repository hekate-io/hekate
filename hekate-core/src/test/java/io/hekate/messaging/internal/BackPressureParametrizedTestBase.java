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

package io.hekate.messaging.internal;

import java.util.Collection;
import java.util.stream.Stream;
import org.junit.runners.Parameterized.Parameters;

import static java.util.stream.Collectors.toList;

public abstract class BackPressureParametrizedTestBase extends BackPressureTestBase {
    public BackPressureParametrizedTestBase(BackPressureTestContext ctx) {
        super(ctx);
    }

    @Parameters(name = "{index}: {0}")
    public static Collection<BackPressureTestContext> getBackPressureTestContexts() {
        return getMessagingServiceTestContexts().stream().flatMap(ctx ->
            Stream.of(
                new BackPressureTestContext(ctx, 0, 1),
                new BackPressureTestContext(ctx, 2, 4)
            ))
            .collect(toList());
    }
}
