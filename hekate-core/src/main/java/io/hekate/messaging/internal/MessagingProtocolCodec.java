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

import io.hekate.cluster.ClusterUuid;
import io.hekate.codec.Codec;
import io.hekate.codec.CodecUtils;
import io.hekate.codec.DataReader;
import io.hekate.codec.DataWriter;
import io.hekate.messaging.MessagingChanneUuid;
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
    private static final MessagingProtocol.Type[] TYPES_CACHE = MessagingProtocol.Type.values();

    private static final NetworkMessage.Preview<MessagingProtocol.Type> TYPE_PREVIEW = rd -> TYPES_CACHE[rd.readByte()];

    private static final NetworkMessage.PreviewInt REQUEST_ID_PREVIEW = rd -> {
        // Skip type.
        rd.skipBytes(1);

        return rd.readInt();
    };

    private static final NetworkMessage.PreviewInt AFFINITY_PREVIEW = rd -> {
        // Skip type.
        rd.skipBytes(1);

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
    public MessagingProtocol decode(DataReader in) throws IOException {
        MessagingProtocol.Type msgType = TYPES_CACHE[in.readByte()];

        switch (msgType) {
            case CONNECT: {
                ClusterUuid to = CodecUtils.readNodeId(in);

                MessagingChanneUuid sourceId = decodeSourceId(in);

                return new Connect(to, sourceId);
            }
            case AFFINITY_NOTIFICATION: {
                int affinity = in.readInt();

                T payload = delegate.decode(in);

                return new AffinityNotification<>(affinity, payload);
            }
            case NOTIFICATION: {
                T payload = delegate.decode(in);

                return new Notification<>(payload);
            }
            case AFFINITY_REQUEST: {
                int affinity = in.readInt();
                int requestId = in.readInt();

                T payload = delegate.decode(in);

                return new AffinityRequest<>(affinity, requestId, payload);
            }
            case REQUEST: {
                int requestId = in.readInt();

                T payload = delegate.decode(in);

                return new Request<>(requestId, payload);
            }
            case AFFINITY_SUBSCRIBE: {
                int affinity = in.readInt();
                int requestId = in.readInt();

                T payload = delegate.decode(in);

                return new AffinitySubscribe<>(affinity, requestId, payload);
            }
            case SUBSCRIBE: {
                int requestId = in.readInt();

                T payload = delegate.decode(in);

                return new Subscribe<>(requestId, payload);
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
                throw new IllegalArgumentException("Unexpected message type: " + msgType);
            }
        }
    }

    @Override
    public void encode(MessagingProtocol message, DataWriter out) throws IOException {
        MessagingProtocol.Type msgType = message.getType();

        out.writeByte(msgType.ordinal());

        switch (msgType) {
            case CONNECT: {
                Connect connect = (Connect)message;

                CodecUtils.writeNodeId(connect.getTo(), out);

                encodeSourceId(connect.getChannelId(), out);

                break;
            }
            case AFFINITY_NOTIFICATION: {
                AffinityNotification<T> notification = message.cast();

                out.writeInt(notification.getAffinity());

                delegate.encode(notification.get(), out);

                break;
            }
            case NOTIFICATION: {
                Notification<T> notification = message.cast();

                delegate.encode(notification.get(), out);

                break;
            }
            case AFFINITY_REQUEST: {
                AffinityRequest<T> request = message.cast();

                out.writeInt(request.getAffinity());
                out.writeInt(request.getRequestId());

                delegate.encode(request.get(), out);

                break;
            }
            case REQUEST: {
                Request<T> request = message.cast();

                out.writeInt(request.getRequestId());

                delegate.encode(request.get(), out);

                break;
            }
            case AFFINITY_SUBSCRIBE: {
                AffinitySubscribe<T> request = message.cast();

                out.writeInt(request.getAffinity());
                out.writeInt(request.getRequestId());

                delegate.encode(request.get(), out);

                break;
            }
            case SUBSCRIBE: {
                Subscribe<T> request = message.cast();

                out.writeInt(request.getRequestId());

                delegate.encode(request.get(), out);

                break;
            }
            case RESPONSE_CHUNK: {
                ResponseChunk<T> response = message.cast();

                out.writeInt(response.getRequestId());

                delegate.encode(response.get(), out);

                break;
            }
            case FINAL_RESPONSE: {
                FinalResponse<T> response = message.cast();

                out.writeInt(response.getRequestId());

                delegate.encode(response.get(), out);

                break;
            }
            default: {
                throw new IllegalArgumentException("Unexpected message type: " + msgType);
            }
        }
    }

    private void encodeSourceId(MessagingChanneUuid sourceId, DataWriter out) throws IOException {
        out.writeLong(sourceId.getHiBits());
        out.writeLong(sourceId.getLoBits());
    }

    private MessagingChanneUuid decodeSourceId(DataReader in) throws IOException {
        long sourceHiBits = in.readLong();
        long sourceLoBits = in.readLong();

        return new MessagingChanneUuid(sourceHiBits, sourceLoBits);
    }
}
