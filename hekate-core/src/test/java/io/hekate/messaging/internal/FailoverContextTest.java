package io.hekate.messaging.internal;

import io.hekate.failover.FailoverContext;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class FailoverContextTest extends FailoverTestBase {
    public FailoverContextTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void test() throws Exception {
        List<FailoverContext> contexts = Collections.synchronizedList(new ArrayList<>());

        failures.set(3);

        toRemote.withFailover(ctx -> {
            contexts.add(ctx);

            return ctx.retry();
        }).request("test--1").response(3, TimeUnit.SECONDS);

        assertEquals(3, contexts.size());

        for (int i = 0; i < contexts.size(); i++) {
            FailoverContext ctx = contexts.get(i);

            assertEquals(i, ctx.getAttempt());
            assertSame(ClosedChannelException.class, ctx.getError().getClass());
            assertEquals(receiver.getInstance().getNode(), ctx.getFailedNode());
        }
    }
}
