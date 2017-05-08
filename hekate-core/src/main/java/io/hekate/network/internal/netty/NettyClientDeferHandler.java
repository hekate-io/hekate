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

package io.hekate.network.internal.netty;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Queue;
import org.slf4j.Logger;

class NettyClientDeferHandler<T> extends ChannelDuplexHandler {
    private static class DeferredMessage {
        private final Object message;

        private final ChannelPromise promise;

        public DeferredMessage(Object message, ChannelPromise promise) {
            this.message = message;
            this.promise = promise;
        }

        public Object message() {
            return message;
        }

        public ChannelPromise promise() {
            return promise;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[message=" + message + ']';
        }
    }

    private final String id;

    private final Logger log;

    private final boolean debug;

    private final boolean trace;

    private Queue<DeferredMessage> deferred = new ArrayDeque<>();

    private boolean needToFlush;

    private Throwable deferredError;

    public NettyClientDeferHandler(String id, Logger log) {
        this.id = id;
        this.log = log;

        this.debug = log.isDebugEnabled();
        this.trace = log.isTraceEnabled();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof NetworkProtocol) {
            if (debug) {
                log.debug("Writing message directly to the channel [address={}, message={}]", id, msg);
            }

            needToFlush = true;

            super.write(ctx, msg, promise);
        } else {
            if (debug) {
                log.debug("Deferring message sending since handshake has not been completed yet [address={}, message={}]", id, msg);
            }

            deferred.add(new DeferredMessage(msg, promise));
        }
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        if (needToFlush) {
            needToFlush = false;

            super.flush(ctx);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        super.userEventTriggered(ctx, evt);

        if (evt instanceof HandshakeDoneEvent) {
            writeDeferred(ctx);
        }
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise future) throws Exception {
        if (trace) {
            log.trace("Deferred handler got channel close event [address={}]", id);
        }

        discardDeferred(ctx);

        super.close(ctx, future);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (trace) {
            log.trace("Deferred handler got channel inactive event [address={}]", id);
        }

        discardDeferred(ctx);

        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (trace) {
            log.trace("Deferred handler got exception caught event [address={}, cause={}]", id, cause.toString());
        }

        if (deferredError == null) {
            deferredError = cause;
        }

        super.exceptionCaught(ctx, cause);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        if (debug) {
            log.debug("Deferred handler unregistered [address={}]", id);
        }

        super.handlerRemoved(ctx);
    }

    private void discardDeferred(ChannelHandlerContext ctx) {
        if (deferred == null) {
            if (trace) {
                log.trace("Skipped discard deferred notification [address={}]", id);
            }
        } else {
            if (debug) {
                log.debug("Discarding deferred messages [address={}, size={}]", id, deferred.size());
            }

            ctx.pipeline().remove(this);

            Throwable error;

            if (deferredError == null) {
                error = new ClosedChannelException();
            } else {
                error = deferredError;
            }

            while (!deferred.isEmpty()) {
                DeferredMessage deferredMsg = deferred.poll();

                try {
                    deferredMsg.promise().tryFailure(error);
                } finally {
                    ReferenceCountUtil.release(deferredMsg.message());
                }
            }

            deferred = null;
        }
    }

    private void writeDeferred(ChannelHandlerContext ctx) {
        Queue<DeferredMessage> localDeferred = this.deferred;

        if (localDeferred == null) {
            if (trace) {
                log.trace("Skipped write deferred notification [address={}]", id);
            }
        } else {
            this.deferred = null;

            ctx.pipeline().remove(this);

            if (!localDeferred.isEmpty()) {
                if (debug) {
                    log.debug("Writing deferred messages [address={}]", id);
                }

                while (!localDeferred.isEmpty()) {
                    DeferredMessage deferredMsg = localDeferred.poll();

                    if (debug) {
                        log.debug("Writing deferred message [address={}, message={}]", id, deferredMsg.message());
                    }

                    ctx.writeAndFlush(deferredMsg.message(), deferredMsg.promise());
                }
            }
        }
    }
}
