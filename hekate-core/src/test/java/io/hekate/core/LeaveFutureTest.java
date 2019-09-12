package io.hekate.core;

import io.hekate.util.HekateFutureTestBase;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class LeaveFutureTest extends HekateFutureTestBase<Hekate, LeaveFuture, HekateFutureException> {
    @Test
    public void testCompleted() {
        assertTrue(LeaveFuture.completed(mock(Hekate.class)).isSuccess());
    }

    @Override
    protected LeaveFuture createFuture() {
        return new LeaveFuture();
    }

    @Override
    protected Class<HekateFutureException> errorType() {
        return HekateFutureException.class;
    }

    @Override
    protected Hekate createValue() {
        return mock(Hekate.class);
    }
}
