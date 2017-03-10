/*
 * Copyright 2017 The Hekate Project
 *
 * The Hekate Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.hekate.task.internal;

import io.hekate.core.internal.util.Utils;
import io.hekate.messaging.unicast.ReplyFailure;
import io.hekate.task.ApplicableTask;
import io.hekate.task.RemoteTaskException;
import io.hekate.util.format.ToString;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

abstract class TaskProtocol {
    enum Type {
        RUN_TASK,

        CALL_TASK,

        APPLY_TASK,

        NULL_RESULT,

        ERROR_RESULT,

        OBJECT_RESULT
    }

    static class CallTask extends TaskProtocol {
        private final Callable<?> task;

        public CallTask(Callable<?> task) {
            this.task = task;
        }

        public Callable<?> getTask() {
            return task;
        }

        @Override
        public Type getType() {
            return Type.CALL_TASK;
        }
    }

    static class RunTask extends TaskProtocol {
        private final Runnable task;

        public RunTask(Runnable task) {
            this.task = task;
        }

        public Runnable getTask() {
            return task;
        }

        @Override
        public Type getType() {
            return Type.RUN_TASK;
        }
    }

    static class ApplyTask extends TaskProtocol implements Externalizable {
        private static final long serialVersionUID = 1;

        private ApplicableTask<Object, Object> task;

        private List<Object> args;

        public ApplyTask(ApplicableTask<Object, Object> task, List<Object> args) {
            this.task = task;
            this.args = args;
        }

        public ApplyTask() {
            // Constructor for serialization.
        }

        public ApplicableTask<Object, Object> getTask() {
            return task;
        }

        public List<Object> getArgs() {
            return args;
        }

        @Override
        public Type getType() {
            return Type.APPLY_TASK;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(task);

            out.writeInt(args.size());

            for (Object arg : args) {
                out.writeObject(arg);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            task = (ApplicableTask<Object, Object>)in.readObject();

            int size = in.readInt();

            args = new ArrayList<>(size);

            for (int i = 0; i < size; i++) {
                args.add(in.readObject());
            }
        }
    }

    static final class NullResult extends TaskProtocol {
        public static final NullResult INSTANCE = new NullResult();

        private NullResult() {
            // No-op.
        }

        @Override
        public Type getType() {
            return Type.NULL_RESULT;
        }
    }

    static class ErrorResult extends TaskProtocol implements ReplyFailure {
        private final String errorStackTrace;

        public ErrorResult(Throwable cause) {
            errorStackTrace = Utils.getStacktrace(cause);
        }

        public ErrorResult(String errorStackTrace) {
            this.errorStackTrace = errorStackTrace;
        }

        public String getErrorStackTrace() {
            return errorStackTrace;
        }

        @Override
        public Type getType() {
            return Type.ERROR_RESULT;
        }

        @Override
        public Throwable asError() {
            return new RemoteTaskException(getErrorStackTrace());
        }
    }

    static class ObjectResult extends TaskProtocol {
        private final Object result;

        public ObjectResult(Object result) {
            this.result = result;
        }

        public Object getResult() {
            return result;
        }

        @Override
        public Type getType() {
            return Type.OBJECT_RESULT;
        }
    }

    public abstract Type getType();

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
