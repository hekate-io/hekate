package io.hekate.task.internal;

import io.hekate.messaging.Message;
import java.util.ArrayList;
import java.util.List;

class ApplyTaskCallback {
    private final int expect;

    private final Message<TaskProtocol> request;

    private final List<Object> results;

    private volatile boolean completed;

    public ApplyTaskCallback(int expect, Message<TaskProtocol> request) {
        this.expect = expect;
        this.request = request;

        results = new ArrayList<>(expect);
    }

    public boolean isCompleted() {
        return completed;
    }

    void onResult(Object result) {
        boolean ready = false;

        synchronized (this) {
            results.add(result);

            if (results.size() == expect) {
                completed = ready = true;
            }
        }

        if (ready) {
            request.reply(new TaskProtocol.ObjectResult(results));
        }
    }

    void onError(Throwable error) {
        boolean reply = false;

        synchronized (this) {
            // Make sure that we reply only once.
            if (!completed) {
                completed = reply = true;
            }
        }

        if (reply) {
            request.reply(new TaskProtocol.ErrorResult(error));
        }
    }
}
