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

package io.hekate.core.service;

import io.hekate.network.NetworkServerFailure;
import java.net.InetSocketAddress;

/**
 * Callback for {@link NetworkServiceManager}.
 */
public interface NetworkBindCallback {
    /**
     * Successfully bound to the provided address.
     *
     * @param address Address.
     */
    default void onBind(InetSocketAddress address) {
        // No-op.
    }

    /**
     * Network service failure.
     *
     * @param failure Failure.
     *
     * @return Failure resolution.
     */
    default NetworkServerFailure.Resolution onFailure(NetworkServerFailure failure) {
        return failure.fail();
    }
}
