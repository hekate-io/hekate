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

package io.hekate.network.netty;

import io.netty.channel.Channel;
import io.netty.channel.DefaultChannelPromise;

class DeferredMessage extends DefaultChannelPromise {
    private final Object source;

    public DeferredMessage(Object payload, Channel channel) {
        super(channel);

        this.source = payload;
    }

    public Object payload() {
        return source;
    }

    public Object source() {
        return source;
    }

    public boolean isPreEncoded() {
        return false;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[message=" + source + "]";
    }
}
