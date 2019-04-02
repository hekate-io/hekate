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

package foo.bar;

import io.hekate.coordinate.CoordinationContext;
import io.hekate.coordinate.CoordinationHandler;
import io.hekate.coordinate.CoordinationRequest;
import io.hekate.coordinate.CoordinatorContext;

public class SomeCoordinationHandler implements CoordinationHandler {
    @Override
    public void prepare(CoordinationContext ctx) {
        // No-op.
    }

    @Override
    public void coordinate(CoordinatorContext ctx) {
        ctx.complete();
    }

    @Override
    public void process(CoordinationRequest request, CoordinationContext ctx) {
        request.reply(request.get());
    }
}
