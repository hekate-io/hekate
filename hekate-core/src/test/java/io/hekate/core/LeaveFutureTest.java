package io.hekate.core;

import io.hekate.util.HekateFutureTestBase;

import static org.mockito.Mockito.mock;

public class LeaveFutureTest extends HekateFutureTestBase<Hekate, LeaveFuture, HekateFutureException> {
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
