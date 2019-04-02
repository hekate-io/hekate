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

package io.hekate;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public abstract class HekateNodeParamTestBase extends HekateNodeTestBase {
    private final HekateTestContext ctx;

    public HekateNodeParamTestBase(HekateTestContext ctx) {
        this.ctx = ctx;
    }

    @Parameters(name = "{index}: {0}")
    public static Collection<HekateTestContext> getNodeTestContexts() {
        return HekateTestContext.all();
    }

    public static <T> List<T> mapTestContext(Function<? super HekateTestContext, ? extends Stream<? extends T>> mapper) {
        return HekateTestContext.map(mapper);
    }

    @Override
    protected HekateTestContext context() {
        return ctx;
    }
}
