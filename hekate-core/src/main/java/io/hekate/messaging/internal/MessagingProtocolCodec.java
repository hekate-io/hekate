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

package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNodeId;
import io.hekate.codec.Codec;
import io.hekate.codec.CodecUtils;
import io.hekate.codec.DataReader;
import io.hekate.codec.DataWriter;
import io.hekate.messaging.MessagingChannelId;
import io.hekate.messaging.internal.MessagingProtocol.AffinityNotification;
import io.hekate.messaging.internal.MessagingProtocol.AffinityRequest;
import io.hekate.messaging.internal.MessagingProtocol.AffinitySubscribe;
import io.hekate.messaging.internal.MessagingProtocol.Connect;
import io.hekate.messaging.internal.MessagingProtocol.FinalResponse;
import io.hekate.messaging.internal.MessagingProtocol.Notification;
import io.hekate.messaging.internal.MessagingProtocol.Request;
import io.hekate.messaging.internal.MessagingProtocol.ResponseChunk;
import io.hekate.messaging.internal.MessagingProtocol.Subscribe;
import io.hekate.network.NetworkMessage;
import java.io.IOException;

class MessagingProtocolCodec<T> implements Codec<MessagingProtocol> {
    private static final int FLAG_BYTES = 1;

    private static final int MASK_TYPE = 15; // 00001111

    private static final int MASK_RETRANSMIT = 128; // 10000000

    private static final MessagingProtocol.Type[] TYPES_CACHE = MessagingProtocol.Type.values();

    private static final NetworkMessage.Preview<MessagingProtocol.Type> TYPE_PREVIEW = rd -> getType(rd.readByte());

    private static final NetworkMessage.PreviewInt REQUEST_ID_PREVIEW = rd -> {
        rd.skipBytes(FLAG_BYTES);

        return rd.readInt();
    };

    private static final NetworkMessage.PreviewInt AFFINITY_PREVIEW = rd -> {
        rd.skipBytes(FLAG_BYTES);

        return rd.readInt();
    };

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
    public MessagingProtocol decode(DataReader in) throws IOException {
        byte flags = in.readByte();

        MessagingProtocol.Type type = getType(flags);

        switch (type) {
            case CONNECT: {
                ClusterNodeId to = CodecUtils.readNodeId(in);

                MessagingChannelId sourceId = decodeSourceId(in);

                return new Connect(to, sourceId);
            }
            case AFFINITY_NOTIFICATION: {
                boolean retransmit = isRetransmit(flags);

                int affinity = in.readInt();

                T payload = delegate.decode(in);

                return new AffinityNotification<>(affinity, retransmit, payload);
            }
            case NOTIFICATION: {
                boolean retransmit = isRetransmit(flags);

                T payload = delegate.decode(in);

                return new Notification<>(retransmit, payload);
            }
            case AFFINITY_REQUEST: {
                boolean retransmit = isRetransmit(flags);

                int affinity = in.readInt();
                int requestId = in.readInt();

                T payload = delegate.decode(in);

                return new AffinityRequest<>(affinity, requestId, retransmit, payload);
            }
            case REQUEST: {
                boolean retransmit = isRetransmit(flags);

                int requestId = in.readInt();

                T payload = delegate.decode(in);

                return new Request<>(requestId, retransmit, payload);
            }
            case AFFINITY_SUBSCRIBE: {
                boolean retransmit = isRetransmit(flags);

                int affinity = in.readInt();
                int requestId = in.readInt();

                T payload = delegate.decode(in);

                return new AffinitySubscribe<>(affinity, requestId, retransmit, payload);
            }
            case SUBSCRIBE: {
                boolean retransmit = isRetransmit(flags);

                int requestId = in.readInt();

                T payload = delegate.decode(in);

                return new Subscribe<>(requestId, retransmit, payload);
            }
            case RESPONSE_CHUNK: {
                int requestId = in.readInt();

                T payload = delegate.decode(in);

                return new ResponseChunk<>(requestId, payload);
            }
            case FINAL_RESPONSE: {
                int requestId = in.readInt();

                T payload = delegate.decode(in);

                return new FinalResponse<>(requestId, payload);
            }
            default: {
                throw new IllegalArgumentException("Unexpected message type: " + type);
            }
        }
    }

    @Override
    public void encode(MessagingProtocol msg, DataWriter out) throws IOException {
        MessagingProtocol.Type type = msg.type();

        int flags = 0;

        flags = setType(flags, type);
        flags = setRetransmit(flags, msg.isRetransmit());

        out.writeByte(flags);

        switch (type) {
            case CONNECT: {
                Connect connect = (Connect)msg;

                CodecUtils.writeNodeId(connect.to(), out);

                encodeSourceId(connect.channelId(), out);

                break;
            }
            case AFFINITY_NOTIFICATION: {
                AffinityNotification<T> notification = msg.cast();

                out.writeInt(notification.affinity());

                delegate.encode(notification.get(), out);

                break;
            }
            case NOTIFICATION: {
                Notification<T> notification = msg.cast();

                delegate.encode(notification.get(), out);

                break;
            }
            case AFFINITY_REQUEST: {
                AffinityRequest<T> request = msg.cast();

                out.writeInt(request.affinity());
                out.writeInt(request.requestId());

                delegate.encode(request.get(), out);

                break;
            }
            case REQUEST: {
                Request<T> request = msg.cast();

                out.writeInt(request.requestId());

                delegate.encode(request.get(), out);

                break;
            }
            case AFFINITY_SUBSCRIBE: {
                AffinitySubscribe<T> request = msg.cast();

                out.writeInt(request.affinity());
                out.writeInt(request.requestId());

                delegate.encode(request.get(), out);

                break;
            }
            case SUBSCRIBE: {
                Subscribe<T> request = msg.cast();

                out.writeInt(request.requestId());

                delegate.encode(request.get(), out);

                break;
            }
            case RESPONSE_CHUNK: {
                ResponseChunk<T> response = msg.cast();

                out.writeInt(response.requestId());

                delegate.encode(response.get(), out);

                break;
            }
            case FINAL_RESPONSE: {
                FinalResponse<T> response = msg.cast();

                out.writeInt(response.requestId());

                delegate.encode(response.get(), out);

                break;
            }
            default: {
                throw new IllegalArgumentException("Unexpected message type: " + type);
            }
        }
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

    private static int setType(int flags, MessagingProtocol.Type type) {
        return (byte)(flags | type.ordinal() & MASK_TYPE);
    }

    private static MessagingProtocol.Type getType(byte flags) {
        return TYPES_CACHE[flags & MASK_TYPE];
    }

    private static int setRetransmit(int flags, boolean retransmit) {
        if (retransmit) {
            return flags | MASK_RETRANSMIT;
        } else {
            return flags;
        }
    }

    private static boolean isRetransmit(byte flags) {
        return (flags & MASK_RETRANSMIT) != 0;
    }
}
