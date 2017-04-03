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

import io.hekate.codec.Codec;
import io.hekate.codec.CodecException;
import io.hekate.codec.CodecFactory;
import io.hekate.network.internal.netty.NetworkProtocol.HandshakeAccept;
import io.hekate.network.internal.netty.NetworkProtocol.HandshakeReject;
import io.hekate.network.internal.netty.NetworkProtocol.HandshakeRequest;
import io.hekate.network.internal.netty.NetworkProtocol.Heartbeat;
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
            if (msg instanceof ByteBuf) {
                // Write pre-encoded message.
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
            if (in.readableBytes() < LENGTH_HEADER_LENGTH) {
                // Retry once we have more data.
                return;
            }

            int startIndex = in.readerIndex();

            // Peek length header (note we are not updating the buffer's read index).
            int length = in.getInt(startIndex);

            // Magic length value:
            //   negative - for protocol messages
            //   positive - for user messages
            boolean protocolMsg = length < 0;

            // Make sure that length is always positive.
            if (protocolMsg) {
                length = -length;
            }

            // Check if we have enough bytes to decode a message.
            if (in.readableBytes() < length) {
                // Retry once we have more data.
                return;
            }

            // Skip length header.
            in.skipBytes(LENGTH_HEADER_LENGTH);

            // Decode message.
            Object msg = decode(protocolMsg, in, startIndex + LENGTH_HEADER_LENGTH, length - LENGTH_HEADER_LENGTH);

            // Advance buffer's read position.
            in.readerIndex(startIndex + length);

            if (msg != null) {
                out.add(msg);
            }
        }

        private Object decode(boolean protocolMsg, ByteBuf in, int offset, int length) throws IOException {
            if (protocolMsg) {
                NetworkProtocol.Type type = TYPES[in.readByte()];

                switch (type) {
                    case HANDSHAKE_REQUEST: {
                        String protocol = NettyMessage.readUtf(in);

                        CodecFactory<Object> codecFactory = allCodecs.get(protocol);

                        if (codecFactory != null) {
                            Codec<Object> codec = codecFactory.createCodec();

                            initCodec(codec);

                            Object payload = null;

                            if (in.readBoolean()) {
                                // Handshake payload is always decoded on NIO thread.
                                payload = message(in, offset, length).decode();
                            }

                            return new HandshakeRequest(protocol, payload, codec);
                        }

                        return new HandshakeRequest(protocol, null);
                    }
                    case HANDSHAKE_ACCEPT: {
                        int hbInterval = in.readInt();
                        int hbLossThreshold = in.readInt();
                        boolean hbDisabled = in.readBoolean();

                        return new HandshakeAccept(hbInterval, hbLossThreshold, hbDisabled);
                    }
                    case HANDSHAKE_REJECT: {
                        String reason = NettyMessage.readUtf(in);

                        return new HandshakeReject(reason);
                    }
                    case HEARTBEAT: {
                        return Heartbeat.INSTANCE;
                    }
                    default: {
                        throw new IllegalStateException("Unexpected message type: " + type);
                    }
                }
            } else {
                // Make sure that buffer will not be recycled until message is processed.
                in.retain();

                try {
                    return message(in, offset, length);
                } catch (RuntimeException | Error | IOException e) {
                    in.release();

                    throw e;
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

    private static final int LENGTH_HEADER_LENGTH = 4;

    private static final byte[] LENGTH_PLACEHOLDER = new byte[LENGTH_HEADER_LENGTH];

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

    public ChannelInboundHandler getDecoder() {
        return decoder;
    }

    public ChannelOutboundHandler getEncoder() {
        return encoder;
    }

    static ByteBuf preEncode(Object msg, Codec<Object> codec, ByteBufAllocator allocator) throws CodecException {
        ByteBuf buf = null;

        try {
            buf = allocator.buffer();

            ByteBufDataWriter writer = new ByteBufDataWriter(buf);

            doEncode(msg, writer, codec);

            return buf;
        } catch (CodecException e) {
            if (buf != null) {
                buf.release();
            }

            throw e;
        }
    }

    private void initCodec(Codec<Object> codec) {
        this.codec = codec;
    }

    private static void doEncode(Object msg, ByteBufDataWriter writer, Codec<Object> codec) throws CodecException {
        try {
            int startIdx = writer.buffer().writerIndex();

            // Placeholder for message length header.
            writer.buffer().writeBytes(LENGTH_PLACEHOLDER);

            boolean protocolMsg = false;

            if (msg instanceof NetworkProtocol) {
                protocolMsg = true;

                NetworkProtocol netMsg = (NetworkProtocol)msg;

                NetworkProtocol.Type type = netMsg.getType();

                writer.writeByte(type.ordinal());

                switch (type) {
                    case HANDSHAKE_REQUEST: {
                        HandshakeRequest request = (HandshakeRequest)netMsg;

                        writer.writeUTF(request.getProtocol());

                        Object payload = request.getPayload();

                        if (payload == null) {
                            writer.writeBoolean(false);
                        } else {
                            writer.writeBoolean(true);

                            codec.encode(payload, writer);
                        }

                        break;
                    }
                    case HANDSHAKE_ACCEPT: {
                        HandshakeAccept accept = (HandshakeAccept)netMsg;

                        writer.writeInt(accept.getHbInterval());
                        writer.writeInt(accept.getHbLossThreshold());
                        writer.writeBoolean(accept.isHbDisabled());

                        break;
                    }
                    case HANDSHAKE_REJECT: {
                        HandshakeReject reject = (HandshakeReject)netMsg;

                        writer.writeUTF(reject.getReason());

                        break;
                    }
                    case HEARTBEAT: {
                        // No-op.
                        break;
                    }
                    default: {
                        throw new IllegalStateException("Unexpected message type: " + msg);
                    }
                }
            } else {
                codec.encode(msg, writer);
            }

            // Calculate real message length.
            int len = writer.buffer().writerIndex() - startIdx;

            // Magic length value:
            //   negative - for protocol messages
            //   positive - for user messages
            if (protocolMsg) {
                len = -len;
            }

            // Update length header.
            writer.buffer().setInt(startIdx, len);
        } catch (CodecException e) {
            throw e;
        } catch (Throwable t) {
            throw new CodecException("Failed to encode message [message=" + msg + ", codec=" + codec + ']', t);
        }
    }
}
