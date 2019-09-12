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

package io.hekate.messaging.operation;

import io.hekate.cluster.ClusterNode;
import io.hekate.messaging.MessagingChannel;

/**
 * Marker interface for application-specific error responses.
 *
 * <p>
 * Messages can implement this interface in order to indicated that particular {@link Response} represents an application-specific error
 * that should be treated as a messaging failure. If {@link MessagingChannel} receives a reply of this type then it will call the
 * {@link #asError(ClusterNode)} method and will fail the messaging operation with the resulting error.
 * </p>
 */
public interface FailureResponse {
    /**
     * Converts this reply to an error.
     *
     * @param fromNode From which node this message was received.
     *
     * @return Error.
     */
    Throwable asError(ClusterNode fromNode);
}
