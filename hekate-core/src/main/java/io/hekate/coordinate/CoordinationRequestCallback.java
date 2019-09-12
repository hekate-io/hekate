/*
 * Copyright 2019 The Hekate Project
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

package io.hekate.coordinate;

/**
 * Callback for coordination requests.
 *
 * @see CoordinationMember#request(Object, CoordinationRequestCallback)
 */
public interface CoordinationRequestCallback {
    /**
     * Gets called when a {@link CoordinationRequest#reply(Object) response} is received.
     *
     * @param response Response.
     * @param from Member who sent the response.
     */
    void onResponse(Object response, CoordinationMember from);

    /**
     * Gets called if coordination process was {@link CoordinationHandler#cancel(CoordinationContext) cancelled} before the response could
     * be received.
     */
    default void onCancel() {
        // No-op.
    }
}
