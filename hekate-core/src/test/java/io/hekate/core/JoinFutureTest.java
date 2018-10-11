package io.hekate.core;

import io.hekate.util.HekateFutureTestBase;

import static org.mockito.Mockito.mock;

public class JoinFutureTest extends HekateFutureTestBase<Hekate, JoinFuture, HekateFutureException> {
    @Override
    protected JoinFuture createFuture() {
        return new JoinFuture();
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
