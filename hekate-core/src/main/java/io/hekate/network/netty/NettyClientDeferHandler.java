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

package io.hekate.network.netty;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Queue;
import org.slf4j.Logger;

class NettyClientDeferHandler extends ChannelDuplexHandler {
    private final String id;

    private final Logger log;

    private final boolean debug;

    private final boolean trace;

    private Queue<DeferredMessage> deferQueue = new ArrayDeque<>();

    private Throwable deferredError;

    private boolean needToFlush;

    public NettyClientDeferHandler(String id, Logger log) {
        this.id = id;
        this.log = log;

        this.debug = log.isDebugEnabled();
        this.trace = log.isTraceEnabled();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof DeferredMessage && deferQueue != null) {
            DeferredMessage def = (DeferredMessage)msg;

            if (deferredError == null) {
                if (debug) {
                    log.debug("Deferring message sending since handshake is not completed yet [to={}, message={}]", id, def.source());
                }

                deferQueue.add(def);
            } else if (promise.tryFailure(deferredError)) {
                ReferenceCountUtil.release(def.payload());
            }
        } else {
            if (deferredError == null) {
                if (debug) {
                    log.debug("Writing message directly to the channel [to={}, message={}]", id, msg);
                }

                needToFlush = true;

                super.write(ctx, msg, promise);
            } else if (promise.tryFailure(deferredError)) {
                ReferenceCountUtil.release(msg);
            }
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
            log.trace("Deferred handler got channel close event [to={}]", id);
        }

        discardDeferred();

        super.close(ctx, future);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (trace) {
            log.trace("Deferred handler got channel inactive event [to={}]", id);
        }

        discardDeferred();

        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (trace) {
            log.trace("Deferred handler got exception caught event [to={}, cause={}]", id, cause.toString());
        }

        if (deferredError == null) {
            deferredError = cause;

            discardDeferred();
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        if (debug) {
            log.debug("Deferred handler unregistered [to={}]", id);
        }

        super.handlerRemoved(ctx);
    }

    private void discardDeferred() {
        Queue<DeferredMessage> deferred = this.deferQueue;

        if (deferred == null) {
            if (trace) {
                log.trace("Skipped discard deferred notification [to={}]", id);
            }
        } else {
            if (!deferred.isEmpty()) {
                if (debug) {
                    log.debug("Discarding deferred messages [to={}, size={}]", id, deferred.size());
                }

                while (!deferred.isEmpty()) {
                    DeferredMessage msg = deferred.poll();

                    if (msg != null) {
                        if (deferredError == null) {
                            deferredError = new ClosedChannelException();
                        }

                        try {
                            msg.promise().tryFailure(deferredError);
                        } finally {
                            ReferenceCountUtil.release(msg.payload());
                        }
                    }
                }
            }

            this.deferQueue = null;
        }
    }

    private void writeDeferred(ChannelHandlerContext ctx) {
        Queue<DeferredMessage> deferred = this.deferQueue;

        if (deferred == null) {
            if (trace) {
                log.trace("Skipped write deferred notification [to={}]", id);
            }
        } else {
            if (!deferred.isEmpty()) {
                if (debug) {
                    log.debug("Writing deferred messages [to={}]", id);
                }

                while (!deferred.isEmpty()) {
                    DeferredMessage msg = deferred.poll();

                    if (msg != null) {
                        if (debug) {
                            log.debug("Writing deferred message [to={}, message={}]", id, msg.source());
                        }

                        ctx.writeAndFlush(msg.payload(), msg.promise());
                    }
                }
            }

            this.deferQueue = null;

            ctx.pipeline().remove(this);
        }
    }
}
