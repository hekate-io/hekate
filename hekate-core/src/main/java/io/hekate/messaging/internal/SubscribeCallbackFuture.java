/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.messaging.internal;

import io.hekate.messaging.unicast.Response;
import io.hekate.messaging.unicast.ResponseCallback;
import io.hekate.messaging.unicast.SubscribeFuture;
import java.util.ArrayList;
import java.util.List;

class SubscribeCallbackFuture<T> extends SubscribeFuture<T> implements ResponseCallback<T> {
    private List<T> result;

    @Override
    public void onComplete(Throwable err, Response<T> rsp) {
        if (err == null) {
            // No need to synchronize since streams are always processed by the same thread.
            if (result == null) {
                result = new ArrayList<>();
            }

            result.add(rsp.get());

            if (!rsp.isPartial()) {
                complete(result);
            }
        } else {
            completeExceptionally(err);
        }
    }
}
