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

import io.hekate.cluster.ClusterNode;
import io.hekate.messaging.unicast.FailureResponse;
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

        APPLY_SINGLE_TASK,

        APPLY_BULK_TASK,

        NULL_RESULT,

        ERROR_RESULT,

        OBJECT_RESULT
    }

    static class CallTask extends TaskProtocol {
        private final Callable<?> task;

        public CallTask(Callable<?> task) {
            this.task = task;
        }

        public Callable<?> task() {
            return task;
        }

        @Override
        public Type type() {
            return Type.CALL_TASK;
        }
    }

    static class RunTask extends TaskProtocol {
        private final Runnable task;

        public RunTask(Runnable task) {
            this.task = task;
        }

        public Runnable task() {
            return task;
        }

        @Override
        public Type type() {
            return Type.RUN_TASK;
        }
    }

    static class ApplyBulkTask extends TaskProtocol implements Externalizable {
        private static final long serialVersionUID = 1;

        private ApplicableTask<Object, Object> task;

        private List<Object> args;

        public ApplyBulkTask(ApplicableTask<Object, Object> task, List<Object> args) {
            this.task = task;
            this.args = args;
        }

        public ApplyBulkTask() {
            // Constructor for serialization.
        }

        public ApplicableTask<Object, Object> task() {
            return task;
        }

        public List<Object> args() {
            return args;
        }

        @Override
        public Type type() {
            return Type.APPLY_BULK_TASK;
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

    static class ApplySingleTask extends TaskProtocol implements Externalizable {
        private static final long serialVersionUID = 1;

        private ApplicableTask<Object, Object> task;

        private Object arg;

        public ApplySingleTask(ApplicableTask<Object, Object> task, Object arg) {
            this.task = task;
            this.arg = arg;
        }

        public ApplySingleTask() {
            // Constructor for serialization.
        }

        public ApplicableTask<Object, Object> task() {
            return task;
        }

        public Object arg() {
            return arg;
        }

        @Override
        public Type type() {
            return Type.APPLY_SINGLE_TASK;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(task);

            out.writeObject(arg);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            task = (ApplicableTask<Object, Object>)in.readObject();

            arg = in.readObject();
        }
    }

    static final class NullResult extends TaskProtocol {
        public static final NullResult INSTANCE = new NullResult();

        private NullResult() {
            // No-op.
        }

        @Override
        public Type type() {
            return Type.NULL_RESULT;
        }
    }

    static class ErrorResult extends TaskProtocol implements FailureResponse {
        private final Throwable cause;

        public ErrorResult(Throwable cause) {
            this.cause = cause;
        }

        public Throwable cause() {
            return cause;
        }

        @Override
        public Type type() {
            return Type.ERROR_RESULT;
        }

        @Override
        public Throwable asError(ClusterNode fromNode) {
            return new RemoteTaskException(fromNode.id(), cause);
        }
    }

    static class ObjectResult extends TaskProtocol {
        private final Object result;

        public ObjectResult(Object result) {
            this.result = result;
        }

        public Object result() {
            return result;
        }

        @Override
        public Type type() {
            return Type.OBJECT_RESULT;
        }
    }

    public abstract Type type();

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
