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

package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.codec.Codec;
import io.hekate.codec.DataReader;
import io.hekate.codec.DataWriter;
import io.hekate.messaging.MessageMetaData;
import io.hekate.messaging.MessagingChannelId;
import io.hekate.messaging.internal.MessagingProtocol.AffinityNotification;
import io.hekate.messaging.internal.MessagingProtocol.AffinityRequest;
import io.hekate.messaging.internal.MessagingProtocol.AffinitySubscribeRequest;
import io.hekate.messaging.internal.MessagingProtocol.AffinityVoidRequest;
import io.hekate.messaging.internal.MessagingProtocol.Connect;
import io.hekate.messaging.internal.MessagingProtocol.ErrorResponse;
import io.hekate.messaging.internal.MessagingProtocol.FinalResponse;
import io.hekate.messaging.internal.MessagingProtocol.Notification;
import io.hekate.messaging.internal.MessagingProtocol.Request;
import io.hekate.messaging.internal.MessagingProtocol.RequestBase;
import io.hekate.messaging.internal.MessagingProtocol.ResponseChunk;
import io.hekate.messaging.internal.MessagingProtocol.SubscribeRequest;
import io.hekate.messaging.internal.MessagingProtocol.VoidRequest;
import io.hekate.messaging.internal.MessagingProtocol.VoidResponse;
import io.hekate.network.NetworkMessage;
import io.hekate.util.format.ToString;
import java.io.EOFException;
import java.io.IOException;

import static io.hekate.codec.CodecUtils.readClusterAddress;
import static io.hekate.codec.CodecUtils.readNodeId;
import static io.hekate.codec.CodecUtils.writeClusterAddress;
import static io.hekate.codec.CodecUtils.writeNodeId;

class MessagingProtocolCodec<T> implements Codec<MessagingProtocol> {
    private static final MessagingProtocol.Type[] TYPES_CACHE = MessagingProtocol.Type.values();

    private static final int FLAG_BYTES = 1;

    private static final int MASK_TYPE = 0b0000_1111;

    private static final int MASK_RETRANSMIT = 0b1000_0000;

    private static final int MASK_HAS_TIMEOUT = 0b0100_0000;

    private static final int MASK_HAS_METADATA = 0b0010_0000;

    private static final NetworkMessage.Preview<MessagingProtocol.Type> TYPE_PREVIEW = rd -> getType(rd.readByte());

    private static final NetworkMessage.PreviewInt REQUEST_ID_PREVIEW = rd -> {
        int skipped = rd.skipBytes(FLAG_BYTES);

        if (skipped < FLAG_BYTES) {
            throw new EOFException("Failed to skip bytes [expected=" + FLAG_BYTES + ", skipped=" + skipped + ']');
        }

        return rd.readVarInt();
    };

    private static final NetworkMessage.PreviewInt AFFINITY_PREVIEW = rd -> {
        int skipped = rd.skipBytes(FLAG_BYTES);

        if (skipped < FLAG_BYTES) {
            throw new EOFException("Failed to skip bytes [expected=" + FLAG_BYTES + ", skipped=" + skipped + ']');
        }

        return rd.readInt();
    };

    private static final NetworkMessage.PreviewBoolean HAS_TIMEOUT_PREVIEW = rd -> hasTimeout(rd.readByte());

    private final Codec<T> delegate;

    public MessagingProtocolCodec(Codec<T> delegate) {
        this.delegate = delegate;
    }

    public static MessagingProtocol.Type previewType(NetworkMessage<MessagingProtocol> msg) throws IOException {
        return msg.preview(TYPE_PREVIEW);
    }

    public static int previewAffinity(NetworkMessage<MessagingProtocol> msg) throws IOException {
        return msg.previewInt(AFFINITY_PREVIEW);
    }

    public static boolean previewHasTimeout(NetworkMessage<MessagingProtocol> msg) throws IOException {
        return msg.previewBoolean(HAS_TIMEOUT_PREVIEW);
    }

    public static int previewRequestId(NetworkMessage<MessagingProtocol> msg) throws IOException {
        return msg.previewInt(REQUEST_ID_PREVIEW);
    }

    @Override
    public boolean isStateful() {
        return delegate.isStateful();
    }

    @Override
    public Class<MessagingProtocol> baseType() {
        return MessagingProtocol.class;
    }

    @Override
    public void encode(MessagingProtocol msg, DataWriter out) throws IOException {
        MessagingProtocol.Type type = msg.messageType();

        int flags = 0;

        flags = appendType(flags, type);

        switch (type) {
            case CONNECT: {
                Connect connect = (Connect)msg;

                out.writeByte(flags);

                writeNodeId(connect.to(), out);
                writeClusterAddress(connect.from(), out);

                encodeSourceId(connect.channelId(), out);

                break;
            }
            case AFFINITY_NOTIFICATION: {
                AffinityNotification<T> notification = msg.cast();

                flags = appendHasTimeout(flags, notification.hasTimeout());
                flags = appendIsRetransmit(flags, notification.isRetransmit());
                flags = appendHasMetaData(flags, notification.hasMetaData());

                out.writeByte(flags);
                out.writeInt(notification.affinity());

                if (notification.hasTimeout()) {
                    out.writeVarLong(notification.timeout());
                }

                if (notification.hasMetaData()) {
                    encodeMetaData(notification.metaData(), out);
                }

                delegate.encode(notification.payload(), out);

                break;
            }
            case NOTIFICATION: {
                Notification<T> notification = msg.cast();

                flags = appendHasTimeout(flags, notification.hasTimeout());
                flags = appendIsRetransmit(flags, notification.isRetransmit());
                flags = appendHasMetaData(flags, notification.hasMetaData());

                out.writeByte(flags);

                if (notification.hasTimeout()) {
                    out.writeVarLong(notification.timeout());
                }

                if (notification.hasMetaData()) {
                    encodeMetaData(notification.metaData(), out);
                }

                delegate.encode(notification.payload(), out);

                break;
            }
            case AFFINITY_REQUEST: {
                AffinityRequest<T> request = msg.cast();

                flags = appendHasTimeout(flags, request.hasTimeout());
                flags = appendIsRetransmit(flags, request.isRetransmit());
                flags = appendHasMetaData(flags, request.hasMetaData());

                out.writeByte(flags);
                out.writeInt(request.affinity());
                out.writeVarInt(request.requestId());

                if (request.hasTimeout()) {
                    out.writeVarLong(request.timeout());
                }

                if (request.hasMetaData()) {
                    encodeMetaData(request.metaData(), out);
                }

                delegate.encode(request.payload(), out);

                break;
            }
            case REQUEST:
            case VOID_REQUEST:
            case SUBSCRIBE: {
                RequestBase<T> request = msg.cast();

                flags = appendHasTimeout(flags, request.hasTimeout());
                flags = appendIsRetransmit(flags, request.isRetransmit());
                flags = appendHasMetaData(flags, request.hasMetaData());

                out.writeByte(flags);
                out.writeVarInt(request.requestId());

                if (request.hasTimeout()) {
                    out.writeVarLong(request.timeout());
                }

                if (request.hasMetaData()) {
                    encodeMetaData(request.metaData(), out);
                }

                delegate.encode(request.payload(), out);

                break;
            }
            case AFFINITY_VOID_REQUEST: {
                AffinityVoidRequest<T> request = msg.cast();

                flags = appendHasTimeout(flags, request.hasTimeout());
                flags = appendIsRetransmit(flags, request.isRetransmit());
                flags = appendHasMetaData(flags, request.hasMetaData());

                out.writeByte(flags);
                out.writeInt(request.affinity());
                out.writeVarInt(request.requestId());

                if (request.hasTimeout()) {
                    out.writeVarLong(request.timeout());
                }

                if (request.hasMetaData()) {
                    encodeMetaData(request.metaData(), out);
                }

                delegate.encode(request.payload(), out);

                break;
            }
            case AFFINITY_SUBSCRIBE: {
                AffinitySubscribeRequest<T> request = msg.cast();

                flags = appendHasTimeout(flags, request.hasTimeout());
                flags = appendIsRetransmit(flags, request.isRetransmit());
                flags = appendHasMetaData(flags, request.hasMetaData());

                out.writeByte(flags);
                out.writeInt(request.affinity());
                out.writeVarInt(request.requestId());

                if (request.hasTimeout()) {
                    out.writeVarLong(request.timeout());
                }

                if (request.hasMetaData()) {
                    encodeMetaData(request.metaData(), out);
                }

                delegate.encode(request.payload(), out);

                break;
            }
            case RESPONSE_CHUNK:
            case FINAL_RESPONSE: {
                ResponseChunk<T> response = msg.cast();

                flags = appendHasMetaData(flags, response.hasMetaData());

                out.writeByte(flags);
                out.writeVarInt(response.requestId());

                if (response.hasMetaData()) {
                    encodeMetaData(response.metaData(), out);
                }

                delegate.encode(response.payload(), out);

                break;
            }
            case VOID_RESPONSE: {
                VoidResponse response = msg.cast();

                out.writeByte(flags);
                out.writeVarInt(response.requestId());

                break;
            }
            case ERROR_RESPONSE:
                ErrorResponse response = msg.cast();

                out.writeByte(flags);
                out.writeVarInt(response.requestId());
                out.writeUTF(response.stackTrace());

                break;
            default: {
                throw new IllegalArgumentException("Unexpected message type: " + type);
            }
        }
    }

    @Override
    public MessagingProtocol decode(DataReader in) throws IOException {
        byte flags = in.readByte();

        MessagingProtocol.Type type = getType(flags);

        switch (type) {
            case CONNECT: {
                ClusterNodeId to = readNodeId(in);
                ClusterAddress from = readClusterAddress(in);

                MessagingChannelId sourceId = decodeSourceId(in);

                return new Connect(to, from, sourceId);
            }
            case AFFINITY_NOTIFICATION: {
                boolean retransmit = isRetransmit(flags);
                int affinity = in.readInt();
                long timeout = hasTimeout(flags) ? in.readVarLong() : 0;
                MessageMetaData metaData = hasMetaData(flags) ? decodeMetaData(in) : null;

                T payload = decodeNotificationPayload(in);

                return new AffinityNotification<>(affinity, retransmit, timeout, payload, metaData);
            }
            case NOTIFICATION: {
                boolean retransmit = isRetransmit(flags);
                long timeout = hasTimeout(flags) ? in.readVarLong() : 0;
                MessageMetaData metaData = hasMetaData(flags) ? decodeMetaData(in) : null;

                T payload = decodeNotificationPayload(in);

                return new Notification<>(retransmit, timeout, payload, metaData);
            }
            case AFFINITY_REQUEST: {
                boolean retransmit = isRetransmit(flags);
                int affinity = in.readInt();
                int requestId = in.readVarInt();
                long timeout = hasTimeout(flags) ? in.readVarLong() : 0;
                MessageMetaData metaData = hasMetaData(flags) ? decodeMetaData(in) : null;

                T payload = decodeRequestPayload(requestId, in);

                return new AffinityRequest<>(affinity, requestId, retransmit, timeout, payload, metaData);
            }
            case REQUEST: {
                boolean retransmit = isRetransmit(flags);
                int requestId = in.readVarInt();
                long timeout = hasTimeout(flags) ? in.readVarLong() : 0;
                MessageMetaData metaData = hasMetaData(flags) ? decodeMetaData(in) : null;

                T payload = decodeRequestPayload(requestId, in);

                return new Request<>(requestId, retransmit, timeout, payload, metaData);
            }
            case AFFINITY_VOID_REQUEST: {
                boolean retransmit = isRetransmit(flags);
                int affinity = in.readInt();
                int requestId = in.readVarInt();
                long timeout = hasTimeout(flags) ? in.readVarLong() : 0;
                MessageMetaData metaData = hasMetaData(flags) ? decodeMetaData(in) : null;

                T payload = decodeRequestPayload(requestId, in);

                return new AffinityVoidRequest<>(affinity, requestId, retransmit, timeout, payload, metaData);
            }
            case VOID_REQUEST: {
                boolean retransmit = isRetransmit(flags);
                int requestId = in.readVarInt();
                long timeout = hasTimeout(flags) ? in.readVarLong() : 0;
                MessageMetaData metaData = hasMetaData(flags) ? decodeMetaData(in) : null;

                T payload = decodeRequestPayload(requestId, in);

                return new VoidRequest<>(requestId, retransmit, timeout, payload, metaData);
            }
            case AFFINITY_SUBSCRIBE: {
                boolean retransmit = isRetransmit(flags);
                int affinity = in.readInt();
                int requestId = in.readVarInt();
                long timeout = hasTimeout(flags) ? in.readVarLong() : 0;
                MessageMetaData metaData = hasMetaData(flags) ? decodeMetaData(in) : null;

                T payload = decodeRequestPayload(requestId, in);

                return new AffinitySubscribeRequest<>(affinity, requestId, retransmit, timeout, payload, metaData);
            }
            case SUBSCRIBE: {
                boolean retransmit = isRetransmit(flags);
                int requestId = in.readVarInt();
                long timeout = hasTimeout(flags) ? in.readVarLong() : 0;
                MessageMetaData metaData = hasMetaData(flags) ? decodeMetaData(in) : null;

                T payload = decodeRequestPayload(requestId, in);

                return new SubscribeRequest<>(requestId, retransmit, timeout, payload, metaData);
            }
            case RESPONSE_CHUNK: {
                int requestId = in.readVarInt();

                MessageMetaData metaData = hasMetaData(flags) ? decodeMetaData(in) : null;

                T payload = decodeResponsePayload(requestId, in);

                return new ResponseChunk<>(requestId, payload, metaData);
            }
            case FINAL_RESPONSE: {
                int requestId = in.readVarInt();

                MessageMetaData metaData = hasMetaData(flags) ? decodeMetaData(in) : null;

                T payload = decodeResponsePayload(requestId, in);

                return new FinalResponse<>(requestId, payload, metaData);
            }
            case VOID_RESPONSE: {
                int requestId = in.readVarInt();

                return new MessagingProtocol.VoidResponse(requestId);
            }
            case ERROR_RESPONSE: {
                int requestId = in.readVarInt();
                String stackTrace = in.readUTF();

                return new ErrorResponse(requestId, stackTrace);
            }
            default: {
                throw new IllegalArgumentException("Unexpected message type: " + type);
            }
        }
    }

    private void encodeMetaData(MessageMetaData metaData, DataWriter out) throws IOException {
        metaData.writeTo(out);
    }

    private MessageMetaData decodeMetaData(DataReader in) throws IOException {
        return MessageMetaData.readFrom(in);
    }

    private void encodeSourceId(MessagingChannelId id, DataWriter out) throws IOException {
        out.writeLong(id.hiBits());
        out.writeLong(id.loBits());
    }

    private MessagingChannelId decodeSourceId(DataReader in) throws IOException {
        long hiBits = in.readLong();
        long loBits = in.readLong();

        return new MessagingChannelId(hiBits, loBits);
    }

    private T decodeRequestPayload(int requestId, DataReader in) throws RequestPayloadDecodeException {
        try {
            return delegate.decode(in);
        } catch (Throwable t) {
            throw new RequestPayloadDecodeException(requestId, t);
        }
    }

    private T decodeResponsePayload(int requestId, DataReader in) throws ResponsePayloadDecodeException {
        try {
            return delegate.decode(in);
        } catch (Throwable t) {
            throw new ResponsePayloadDecodeException(requestId, t);
        }
    }

    private T decodeNotificationPayload(DataReader in) throws NotificationPayloadDecodeException {
        try {
            return delegate.decode(in);
        } catch (Throwable t) {
            throw new NotificationPayloadDecodeException(t);
        }
    }

    private static int appendType(int flags, MessagingProtocol.Type type) {
        return (byte)(flags | (type.ordinal() & MASK_TYPE));
    }

    private static MessagingProtocol.Type getType(byte flags) {
        return TYPES_CACHE[flags & MASK_TYPE];
    }

    private static boolean isRetransmit(byte flags) {
        return (flags & MASK_RETRANSMIT) != 0;
    }

    private static boolean hasTimeout(byte flags) {
        return (flags & MASK_HAS_TIMEOUT) != 0;
    }

    private static boolean hasMetaData(byte flags) {
        return (flags & MASK_HAS_METADATA) != 0;
    }

    private static int appendIsRetransmit(int flags, boolean retransmit) {
        if (retransmit) {
            return flags | MASK_RETRANSMIT;
        } else {
            return flags;
        }
    }

    private static int appendHasTimeout(int flags, boolean hasTimeout) {
        if (hasTimeout) {
            return flags | MASK_HAS_TIMEOUT;
        } else {
            return flags;
        }
    }

    private static int appendHasMetaData(int flags, boolean hasMetaData) {
        if (hasMetaData) {
            return flags | MASK_HAS_METADATA;
        } else {
            return flags;
        }
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
