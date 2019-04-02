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

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.ReferenceCounted;

class DeferredEncodedMessage extends DeferredMessage implements ReferenceCounted {
    private final ByteBuf buf;

    public DeferredEncodedMessage(ByteBuf buf, Object source, Channel channel) {
        super(source, channel);

        this.buf = buf;
    }

    @Override
    public ByteBuf payload() {
        return buf;
    }

    @Override
    public boolean isPreEncoded() {
        return true;
    }

    @Override
    public int refCnt() {
        return buf.refCnt();
    }

    @Override
    public ReferenceCounted retain() {
        buf.retain();

        return this;
    }

    @Override
    public ReferenceCounted retain(int increment) {
        buf.retain(increment);

        return this;
    }

    @Override
    public ReferenceCounted touch() {
        buf.touch();

        return this;
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        buf.touch(hint);

        return this;
    }

    @Override
    public boolean release() {
        return buf.release();
    }

    @Override
    public boolean release(int decrement) {
        return buf.release(decrement);
    }
}
