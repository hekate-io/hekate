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

package io.hekate.failover;

import io.hekate.failover.internal.DefaultFailoverContext;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class FailoverPolicyBuilderTest extends FailoverPolicyTestBase {
    private final FailoverPolicyBuilder builder = new FailoverPolicyBuilder();

    @Test
    public void testErrorTypes() throws Exception {
        assertNull(builder.getErrorTypes());

        builder.setErrorTypes(Collections.singletonList(IOException.class));

        assertEquals(1, builder.getErrorTypes().size());
        assertTrue(builder.getErrorTypes().contains(IOException.class));

        builder.setErrorTypes(null);

        assertNull(builder.getErrorTypes());

        assertSame(builder, builder.withErrorTypes(IOException.class));

        assertEquals(1, builder.getErrorTypes().size());
        assertTrue(builder.getErrorTypes().contains(IOException.class));

        assertSame(builder, builder.withErrorTypes(RuntimeException.class, Error.class));

        assertEquals(3, builder.getErrorTypes().size());
        assertTrue(builder.getErrorTypes().contains(IOException.class));
        assertTrue(builder.getErrorTypes().contains(RuntimeException.class));
        assertTrue(builder.getErrorTypes().contains(Error.class));

        builder.setErrorTypes(null);

        assertSame(builder, builder.withErrorTypes(IOException.class));
    }

    @Test
    public void testRetryDelay() {
        assertNull(builder.getRetryDelay());

        FailoverDelaySupplier s1 = failover -> 100;
        FailoverDelaySupplier s2 = failover -> 200;

        builder.setRetryDelay(s1);

        assertSame(s1, builder.getRetryDelay());

        builder.setRetryDelay(null);

        assertNull(builder.getRetryDelay());

        assertSame(builder, builder.withRetryDelay(s2));

        assertSame(s2, builder.getRetryDelay());

        assertTrue(builder.withConstantRetryDelay(111).getRetryDelay() instanceof ConstantFailoverDelay);
    }

    @Test
    public void testRetryUntil() {
        assertNull(builder.getRetryUntil());

        FailoverCondition c1 = failover -> false;
        FailoverCondition c2 = failover -> false;

        builder.setRetryUntil(Arrays.asList(c1, c2));

        assertEquals(2, builder.getRetryUntil().size());
        assertTrue(builder.getRetryUntil().contains(c1));
        assertTrue(builder.getRetryUntil().contains(c2));

        builder.setRetryUntil(null);

        assertNull(builder.getRetryUntil());

        assertSame(builder, builder.withRetryUntil(c1));

        assertEquals(1, builder.getRetryUntil().size());
        assertTrue(builder.getRetryUntil().contains(c1));

        builder.setRetryUntil(null);

        assertTrue(builder.withMaxAttempts(111).getRetryUntil().get(0) instanceof MaxFailoverAttempts);
    }

    @Test
    public void testRoutingPolicy() {
        assertNull(builder.getRoutingPolicy());

        builder.setRoutingPolicy(FailoverRoutingPolicy.RE_ROUTE);

        assertSame(FailoverRoutingPolicy.RE_ROUTE, builder.getRoutingPolicy());

        builder.setRoutingPolicy(null);

        assertNull(builder.getRoutingPolicy());

        assertSame(FailoverRoutingPolicy.RE_ROUTE, builder.withAlwaysReRoute().getRoutingPolicy());
        assertSame(FailoverRoutingPolicy.RETRY_SAME_NODE, builder.withAlwaysRetrySameNode().getRoutingPolicy());
        assertSame(FailoverRoutingPolicy.PREFER_SAME_NODE, builder.withRetrySameNodeIfExists().getRoutingPolicy());
    }

    @Test
    public void testFaultIsAlwaysFail() {
        assertEquals(FailoverPolicy.alwaysFail().getClass(), builder.build().getClass());

        builder.setRetryUntil(Collections.singletonList(null));

        assertEquals(FailoverPolicy.alwaysFail().getClass(), builder.build().getClass());
    }

    @Test
    public void testToString() {
        assertTrue(builder.toString(), builder.toString().startsWith(FailoverPolicyBuilder.class.getSimpleName()));
    }

    @Test
    public void testApply() throws Exception {
        DefaultFailoverContext ctx = newContext(1);

        FailoverPolicyBuilder alwaysRetry = this.builder.withRetryUntil(failover -> true);

        assertTrue(alwaysRetry.build().apply(ctx).isRetry());
        assertTrue(alwaysRetry.withErrorTypes(Exception.class).build().apply(ctx).isRetry());

        alwaysRetry.setErrorTypes(null);

        assertFalse(alwaysRetry.withErrorTypes(IOException.class).build().apply(ctx).isRetry());

        assertEquals(987, alwaysRetry.withErrorTypes(Exception.class).withRetryDelay(f -> 987).build().apply(ctx).delay());

        assertTrue(alwaysRetry.build().toString().startsWith(FailoverPolicy.class.getSimpleName()));
    }
}
