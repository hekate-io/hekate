/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.javadoc.failover;

import io.hekate.failover.FailoverContext;
import io.hekate.failover.FailoverPolicy;
import io.hekate.failover.FailureResolution;
import java.io.IOException;

// Start:example
public class ExampleFailoverPolicy implements FailoverPolicy {
    @Override
    public FailureResolution apply(FailoverContext ctx) {
        // Retry only up to 3 times and only in case of I/O errors.
        if (ctx.attempt() < 3 && ctx.isCausedBy(IOException.class)) {
            return ctx.retry().withDelay(500); // Wait for 500ms before retrying.
        }

        // All attempts failed.
        return ctx.fail();
    }
}
// End:example
