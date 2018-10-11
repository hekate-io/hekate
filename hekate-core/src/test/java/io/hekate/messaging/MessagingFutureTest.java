package io.hekate.messaging;

import io.hekate.util.HekateFutureTestBase;

public class MessagingFutureTest extends HekateFutureTestBase<Object, MessagingFuture<Object>, MessagingFutureException> {
    @Override
    protected MessagingFuture<Object> createFuture() {
        return new MessagingFuture<>();
    }

    @Override
    protected Class<MessagingFutureException> errorType() {
        return MessagingFutureException.class;
    }

    @Override
    protected Object createValue() {
        return new Object();
    }
}
