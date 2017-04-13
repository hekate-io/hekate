package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNodeId;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.UnknownRouteException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FailoverSendTest extends FailoverTestBase {
    public FailoverSendTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testNoFailoverOfRoutingErrors() throws Exception {
        AtomicInteger failoverCalls = new AtomicInteger();

        ClusterNodeId unknown = newNodeId();

        try {
            get(sender.get().forNode(unknown)
                .withFailover(context -> {
                    failoverCalls.incrementAndGet();

                    return context.retry().withReRoute();
                })
                .send("error"));

            fail("Error was expected.");
        } catch (MessagingFutureException e) {
            assertTrue(getStacktrace(e), e.isCausedBy(UnknownRouteException.class));
        }

        assertEquals(0, failoverCalls.get());
    }
}
