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

import io.hekate.codec.Codec;
import io.hekate.codec.DataReader;
import io.hekate.codec.DataWriter;
import io.hekate.task.internal.TaskProtocol.CallTask;
import io.hekate.task.internal.TaskProtocol.ErrorResult;
import io.hekate.task.internal.TaskProtocol.ObjectResult;
import io.hekate.task.internal.TaskProtocol.RunTask;
import java.io.IOException;
import java.util.concurrent.Callable;

class TaskProtocolCodec implements Codec<TaskProtocol> {
    private static final TaskProtocol.Type[] TYPES = TaskProtocol.Type.values();

    private final Codec<Object> delegate;

    public TaskProtocolCodec(Codec<Object> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void encode(TaskProtocol msg, DataWriter out) throws IOException {
        TaskProtocol.Type type = msg.getType();

        out.writeByte(type.ordinal());

        switch (type) {
            case RUN_TASK: {
                RunTask run = (RunTask)msg;

                delegate.encode(run.getTask(), out);

                break;
            }
            case CALL_TASK: {
                CallTask call = (CallTask)msg;

                delegate.encode(call.getTask(), out);

                break;
            }
            case APPLY_TASK: {
                delegate.encode(msg, out);

                break;
            }
            case NULL_RESULT:
                // No-op.
                break;
            case ERROR_RESULT: {
                ErrorResult error = (ErrorResult)msg;

                out.writeUTF(error.getErrorStackTrace());

                break;
            }
            case OBJECT_RESULT: {
                ObjectResult result = (ObjectResult)msg;

                delegate.encode(result.getResult(), out);

                break;
            }
            default: {
                throw new IllegalArgumentException("Unexpected message type: " + type);
            }
        }
    }

    @Override
    public boolean isStateful() {
        return delegate.isStateful();
    }

    @Override
    public TaskProtocol decode(DataReader in) throws IOException {
        TaskProtocol.Type type = TYPES[in.readByte()];

        switch (type) {
            case RUN_TASK: {
                Runnable task = (Runnable)delegate.decode(in);

                return new RunTask(task);
            }
            case CALL_TASK: {
                Callable<?> task = (Callable<?>)delegate.decode(in);

                return new CallTask(task);
            }
            case APPLY_TASK: {
                return (TaskProtocol)delegate.decode(in);
            }
            case NULL_RESULT: {
                return TaskProtocol.NullResult.INSTANCE;
            }
            case ERROR_RESULT: {
                String stackTrace = in.readUTF();

                return new ErrorResult(stackTrace);
            }
            case OBJECT_RESULT: {
                Object result = delegate.decode(in);

                return new ObjectResult(result);
            }
            default: {
                throw new IllegalArgumentException("Unexpected message type: " + type);
            }
        }
    }
}
