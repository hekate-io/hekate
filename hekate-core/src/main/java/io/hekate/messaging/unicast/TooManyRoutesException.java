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

package io.hekate.messaging.unicast;

import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.messaging.MessagingChannel;

/**
 * Signals that there were multiple destination nodes available for a unicast operation and messaging channel couldn't resolve which one
 * to use.
 *
 * <p>
 * <b>Note:</b> for unicast operations it is possible to specify a single recipient by narrowing down the channel's cluster view by
 * {@link MessagingChannel#filter(ClusterNodeFilter) filtering} or by {@link MessagingChannel#withLoadBalancer(LoadBalancer) specifying} a
 * {@link LoadBalancer}.
 * </p>
 *
 * @see MessagingChannel#send(Object, SendCallback)
 * @see MessagingChannel#request(Object, RequestCallback)
 */
public class TooManyRoutesException extends LoadBalancingException {
    private static final long serialVersionUID = 1;

    /**
     * Constructs new instance.
     *
     * @param message Error message.
     */
    public TooManyRoutesException(String message) {
        super(message);
    }
}
