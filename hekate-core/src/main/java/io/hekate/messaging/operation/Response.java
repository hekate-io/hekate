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

import io.hekate.cluster.ClusterTopology;
import io.hekate.messaging.MessageBase;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.loadbalance.LoadBalancer;

/**
 * Response of a {@link Request} operation.
 *
 * @param <T> Payload type.
 *
 * @see MessagingChannel#newRequest(Object)
 */
public interface Response<T> extends MessageBase<T> {
    /**
     * Returns the request.
     *
     * @return Request.
     *
     * @see #request(Class)
     */
    T request();

    /**
     * Casts the request of this message to the specified type.
     *
     * @param type Request type.
     * @param <P> Request type.
     *
     * @return request.
     *
     * @throws ClassCastException If payload can't be cast to the specified type.
     * @see #request()
     */
    <P extends T> P request(Class<P> type);

    /**
     * Cluster topology that was used by the {@link LoadBalancer} to submit the request.
     *
     * @return Cluster topology that was used by the {@link LoadBalancer} to submit the request.
     */
    ClusterTopology topology();
}
