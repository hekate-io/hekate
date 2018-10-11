package io.hekate.core;

import io.hekate.util.HekateFutureTestBase;

import static org.mockito.Mockito.mock;

public class TerminateFutureTest extends HekateFutureTestBase<Hekate, TerminateFuture, HekateFutureException> {
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
