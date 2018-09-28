package io.hekate.core;

import io.hekate.util.HekateFutureTestBase;

import static org.mockito.Mockito.mock;

public class InitializationFutureTest extends HekateFutureTestBase<Hekate, InitializationFuture, HekateFutureException> {
    @Override
    protected InitializationFuture createFuture() {
        return new InitializationFuture();
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
