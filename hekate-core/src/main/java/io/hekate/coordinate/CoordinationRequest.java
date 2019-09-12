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
 * Request from a {@link CoordinationMember}.
 *
 * <p>
 * <b>Important!!!</b> Each request must be explicitly replied via {@link #reply(Object)} method. Not replying to requests can lead to
 * memory leaks since each coordination member keeps an in-memory structure to track sent requests and releases this memory only when
 * requests get replied.
 * </p>
 *
 * @see CoordinationHandler#process(CoordinationRequest, CoordinationContext)
 */
public interface CoordinationRequest {
    /**
     * Returns the member who sent this request.
     *
     * @return Member who sent this request.
     */
    CoordinationMember from();

    /**
     * Returns the request message.
     *
     * @return Request message.
     */
    Object get();

    /**
     * Returns the request message.
     *
     * @param type Message type.
     * @param <T> Message type.
     *
     * @return Request message.
     */
    <T> T get(Class<T> type);

    /**
     * Sends the response.
     *
     * @param response Response.
     */
    void reply(Object response);
}
