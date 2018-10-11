package io.hekate.coordinate;

import io.hekate.util.HekateFutureTestBase;

import static org.mockito.Mockito.mock;

public class CoordinationFutureTest extends HekateFutureTestBase<CoordinationProcess, CoordinationFuture, CoordinationException> {
    @Override
    protected CoordinationFuture createFuture() {
        return new CoordinationFuture();
    }

    @Override
    protected Class<CoordinationException> errorType() {
        return CoordinationException.class;
    }

    @Override
    protected CoordinationProcess createValue() {
        return mock(CoordinationProcess.class);
    }
}
