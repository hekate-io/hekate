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

import io.hekate.codec.Codec;
import io.hekate.codec.CodecException;
import io.hekate.codec.CodecFactory;
import io.hekate.network.netty.NetworkProtocol.HandshakeAccept;
import io.hekate.network.netty.NetworkProtocol.HandshakeReject;
import io.hekate.network.netty.NetworkProtocol.HandshakeRequest;
import io.hekate.network.netty.NetworkProtocol.Heartbeat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

class NetworkProtocolCodec {
    private class Encoder extends MessageToByteEncoder<Object> {
        private final ByteBufDataWriter writer = new ByteBufDataWriter();

        @Override
        public boolean acceptOutboundMessage(Object msg) throws Exception {
            return true;
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            if (msg instanceof DeferredMessage) {
                DeferredMessage defMsg = (DeferredMessage)msg;

                if (defMsg.isPreEncoded()) {
                    // Write pre-encoded message.
                    ctx.write(defMsg.payload(), promise);
                } else {
                    super.write(ctx, defMsg.payload(), promise);
                }
            } else if (msg instanceof ByteBuf) {
                ctx.write(msg, promise);
            } else {
                super.write(ctx, msg, promise);
            }
        }

        @Override
        protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
            ByteBufDataWriter writer = this.writer;

            writer.setOut(out);

            try {
                doEncode(msg, writer, codec);
            } finally {
                writer.setOut(null);
            }
        }
    }

    private class Decoder extends ByteToMessageDecoder {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
            // Check if we have enough bytes in the buffer.
            if (in.readableBytes() < HEADER_LENGTH) {
                // Retry once we have more data.
                return;
            }

            int startIndex = in.readerIndex();

            // Peek length header (note that we are not updating the buffer's read index).
            int length = in.getInt(startIndex);

            // Magic length value:
            //   negative - for internal messages
            //   positive - for user messages
            boolean internalMsg = length < 0;

            // Make sure that length is always positive.
            if (internalMsg) {
                length = -length;
            }

            // Check if we have enough bytes to decode.
            if (in.readableBytes() < length) {
                // Retry once we have more data.
                return;
            }

            // Skip the length header (we already have it).
            in.skipBytes(HEADER_LENGTH);

            // Decode message.
            int msgOffset = startIndex + HEADER_LENGTH;
            int msgLength = length - HEADER_LENGTH;

            Object msg;

            if (internalMsg) {
                // Decode internal message.
                msg = decodeInternal(in, msgOffset, msgLength);
            } else {
                // Decode user-defined message.
                msg = decodeUser(in, msgOffset, msgLength);
            }

            // Advance buffer's read position.
            in.readerIndex(startIndex + length);

            if (msg != null) {
                out.add(msg);
            }
        }

        private Object decodeUser(ByteBuf in, int offset, int length) throws IOException {
            // Make sure that buffer will not be recycled until message is processed.
            in.retain();

            try {
                return message(in, offset, length);
            } catch (RuntimeException | Error | IOException e) {
                in.release();

                throw e;
            }
        }

        private Object decodeInternal(ByteBuf in, int offset, int length) throws IOException {
            NetworkProtocol.Type type = TYPES[in.readByte()];

            switch (type) {
                case HANDSHAKE_REQUEST: {
                    String protocol = NettyMessage.utf(in);
                    int threadAffinity = in.readInt();

                    CodecFactory<Object> codecFactory = allCodecs.get(protocol);

                    if (codecFactory != null) {
                        Codec<Object> codec = codecFactory.createCodec();

                        initCodec(codec);

                        Object payload = null;

                        if (in.readBoolean()) {
                            // Handshake payload is always decoded on NIO thread.
                            payload = message(in, offset, length).decode();
                        }

                        return new HandshakeRequest(protocol, payload, threadAffinity, codec);
                    }

                    return new HandshakeRequest(protocol, null, threadAffinity);
                }
                case HANDSHAKE_ACCEPT: {
                    int hbInterval = in.readInt();
                    int hbLossThreshold = in.readInt();
                    boolean hbDisabled = in.readBoolean();

                    return new HandshakeAccept(hbInterval, hbLossThreshold, hbDisabled);
                }
                case HANDSHAKE_REJECT: {
                    String reason = NettyMessage.utf(in);

                    return new HandshakeReject(reason);
                }
                case HEARTBEAT: {
                    return Heartbeat.INSTANCE;
                }
                default: {
                    throw new IllegalStateException("Unexpected message type: " + type);
                }
            }
        }

        private NettyMessage message(ByteBuf in, int offset, int length) throws IOException {
            int start = in.readerIndex();

            // Slice buffer from the current position to the end of message.
            // Need to do it since input buffer can contain data of more than one message.
            ByteBuf body = in.slice(start, length - (start - offset));

            NettyMessage message = new NettyMessage(body, codec);

            if (codec.isStateful()) {
                // Stateful codecs must always perform decoding on NIO thread.
                message.decode();
            }

            return message;
        }
    }

    private static final NetworkProtocol.Type[] TYPES = NetworkProtocol.Type.values();

    private static final int HEADER_LENGTH = Integer.BYTES;

    private final Encoder encoder = new Encoder();

    private final Decoder decoder = new Decoder();

    private final Map<String, CodecFactory<Object>> allCodecs;

    private Codec<Object> codec;

    public NetworkProtocolCodec(Codec<Object> codec) {
        this.codec = codec;

        this.allCodecs = Collections.emptyMap();
    }

    public NetworkProtocolCodec(Map<String, CodecFactory<Object>> allCodecs) {
        this.allCodecs = allCodecs;
    }

    public ChannelInboundHandler decoder() {
        return decoder;
    }

    public ChannelOutboundHandler encoder() {
        return encoder;
    }

    static ByteBuf preEncode(Object msg, Codec<Object> codec, ByteBufAllocator allocator) throws CodecException {
        ByteBuf buf = allocator.buffer();

        try {
            ByteBufDataWriter writer = new ByteBufDataWriter(buf);

            doEncode(msg, writer, codec);

            return buf;
        } catch (CodecException e) {
            buf.release();

            throw e;
        }
    }

    private void initCodec(Codec<Object> codec) {
        this.codec = codec;
    }

    private static void doEncode(Object msg, ByteBufDataWriter out, Codec<Object> codec) throws CodecException {
        ByteBuf buf = out.buffer();

        try {
            // Header indexes.
            int headStartIdx = buf.writerIndex();
            int headEndIdx = headStartIdx + HEADER_LENGTH;

            // Placeholder for the header.
            buf.ensureWritable(HEADER_LENGTH).writerIndex(headEndIdx);

            boolean internalMsg;

            if (msg instanceof NetworkProtocol) {
                // Encode internal message.
                internalMsg = true;

                NetworkProtocol netMsg = (NetworkProtocol)msg;

                encodeInternal(out, codec, netMsg);
            } else {
                // Encode user-defined message.
                internalMsg = false;

                codec.encode(msg, out);
            }

            // Calculate real message length.
            int len = buf.writerIndex() - headStartIdx;

            // Magic length value:
            //   negative - for protocol messages
            //   positive - for user messages
            if (internalMsg) {
                len = -len;
            }

            // Update length header.
            buf.setInt(headStartIdx, len);
        } catch (CodecException e) {
            throw e;
        } catch (Throwable t) {
            throw new CodecException("Failed to encode message [message=" + msg + ']', t);
        }
    }

    private static void encodeInternal(ByteBufDataWriter out, Codec<Object> codec, NetworkProtocol netMsg) throws IOException {
        NetworkProtocol.Type type = netMsg.type();

        out.writeByte(type.ordinal());

        switch (type) {
            case HANDSHAKE_REQUEST: {
                HandshakeRequest request = (HandshakeRequest)netMsg;

                out.writeUTF(request.protocol());
                out.writeInt(request.threadAffinity());

                Object payload = request.payload();

                if (payload == null) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);

                    codec.encode(payload, out);
                }

                break;
            }
            case HANDSHAKE_ACCEPT: {
                HandshakeAccept accept = (HandshakeAccept)netMsg;

                out.writeInt(accept.hbInterval());
                out.writeInt(accept.hbLossThreshold());
                out.writeBoolean(accept.isHbDisabled());

                break;
            }
            case HANDSHAKE_REJECT: {
                HandshakeReject reject = (HandshakeReject)netMsg;

                out.writeUTF(reject.reason());

                break;
            }
            case HEARTBEAT: {
                // No-op.
                break;
            }
            default: {
                throw new IllegalStateException("Unexpected message type: " + netMsg);
            }
        }
    }
}
