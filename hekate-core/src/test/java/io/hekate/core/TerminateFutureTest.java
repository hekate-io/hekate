package io.hekate.core;

import io.hekate.util.HekateFutureTestBase;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TerminateFutureTest extends HekateFutureTestBase<Hekate, TerminateFuture, HekateFutureException> {
    @Test
    public void testCompleted() {
        assertTrue(TerminateFuture.completed(mock(Hekate.class)).isSuccess());
    }

    @Override
    protected TerminateFuture createFuture() {
        return new TerminateFuture();
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
