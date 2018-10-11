package io.hekate.messaging.internal;

import io.hekate.messaging.unicast.Response;

interface MessageOperationCallback<T> {
    boolean completeAttempt(MessageOperationAttempt<T> attempt, Response<T> rsp, Throwable err);
}
