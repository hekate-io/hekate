/*
 * Copyright 2022 The Hekate Project
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

package io.hekate.cluster.internal.gossip;

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterNodeRuntime;
import io.hekate.cluster.internal.DefaultClusterNode;
import io.hekate.cluster.internal.DefaultClusterNodeRuntime;
import io.hekate.cluster.internal.gossip.GossipProtocol.HeartbeatReply;
import io.hekate.cluster.internal.gossip.GossipProtocol.HeartbeatRequest;
import io.hekate.cluster.internal.gossip.GossipProtocol.JoinAccept;
import io.hekate.cluster.internal.gossip.GossipProtocol.JoinReject;
import io.hekate.cluster.internal.gossip.GossipProtocol.JoinRequest;
import io.hekate.cluster.internal.gossip.GossipProtocol.LongTermConnect;
import io.hekate.cluster.internal.gossip.GossipProtocol.Update;
import io.hekate.cluster.internal.gossip.GossipProtocol.UpdateDigest;
import io.hekate.codec.Codec;
import io.hekate.codec.CodecUtils;
import io.hekate.codec.DataReader;
import io.hekate.codec.DataWriter;
import io.hekate.core.ServiceInfo;
import io.hekate.core.ServiceProperty;
import io.hekate.core.service.internal.DefaultServiceInfo;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static io.hekate.codec.CodecUtils.readAddress;
import static io.hekate.codec.CodecUtils.readClusterAddress;
import static io.hekate.codec.CodecUtils.writeAddress;
import static io.hekate.codec.CodecUtils.writeClusterAddress;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

public class GossipProtocolCodec implements Codec<GossipProtocol> {
    private static final GossipProtocol.Type[] MESSAGE_TYPES = GossipProtocol.Type.values();

    private static final GossipNodeStatus[] NODE_STATUSES = GossipNodeStatus.values();

    private static final ServiceProperty.Type[] SERVICE_PROP_TYPES = ServiceProperty.Type.values();

    private final Map<Integer, String> readStringDict = new HashMap<>();

    private final Map<String, Integer> writeStringDict = new HashMap<>();

    private final AtomicReference<ClusterNodeId> localNodeIdRef;

    private ClusterNodeId localNodeId;

    public GossipProtocolCodec(AtomicReference<ClusterNodeId> localNodeIdRef) {
        this.localNodeIdRef = localNodeIdRef;
    }

    @Override
    public boolean isStateful() {
        return true;
    }

    @Override
    public Class<GossipProtocol> baseType() {
        return GossipProtocol.class;
    }

    @Override
    public void encode(GossipProtocol msg, DataWriter out) throws IOException {
        try {
            GossipProtocol.Type msgType = msg.type();

            out.writeByte(msgType.ordinal());

            switch (msgType) {
                case LONG_TERM_CONNECT: {
                    LongTermConnect connect = (LongTermConnect)msg;

                    writeClusterAddress(connect.to(), out);
                    writeClusterAddress(connect.from(), out);

                    break;
                }
                case GOSSIP_UPDATE: {
                    Update update = (Update)msg;

                    writeClusterAddress(update.to(), out);
                    writeClusterAddress(update.from(), out);

                    encodeGossip(update.gossip(), out);

                    break;
                }
                case GOSSIP_UPDATE_DIGEST: {
                    UpdateDigest digest = (UpdateDigest)msg;

                    writeClusterAddress(digest.to(), out);
                    writeClusterAddress(digest.from(), out);

                    encodeGossipDigest(digest.digest(), out);

                    break;
                }
                case JOIN_REQUEST: {
                    JoinRequest request = (JoinRequest)msg;

                    writeAddress(request.toAddress(), out);

                    encodeNode(request.fromNode(), out);

                    out.writeUTF(request.namespace());

                    break;
                }
                case JOIN_ACCEPT: {
                    JoinAccept accept = (JoinAccept)msg;

                    writeClusterAddress(accept.to(), out);
                    writeClusterAddress(accept.from(), out);

                    encodeGossip(accept.gossip(), out);

                    break;
                }
                case JOIN_REJECT: {
                    JoinReject reject = (JoinReject)msg;

                    writeClusterAddress(reject.to(), out);
                    writeClusterAddress(reject.from(), out);
                    writeAddress(reject.rejectedAddress(), out);

                    JoinReject.RejectType rejectType = reject.rejectType();

                    out.writeInt(rejectType.ordinal());

                    if (rejectType == JoinReject.RejectType.FATAL) {
                        out.writeUTF(reject.reason());
                    }

                    break;
                }
                case HEARTBEAT_REQUEST: {
                    HeartbeatRequest request = (HeartbeatRequest)msg;

                    writeClusterAddress(request.to(), out);
                    writeClusterAddress(request.from(), out);

                    break;
                }
                case HEARTBEAT_REPLY: {
                    HeartbeatReply reply = (HeartbeatReply)msg;

                    writeClusterAddress(reply.to(), out);
                    writeClusterAddress(reply.from(), out);

                    break;
                }
                default: {
                    throw new IllegalStateException("Unexpected message type: " + msgType);
                }
            }
        } finally {
            writeStringDict.clear();
        }
    }

    @Override
    public GossipProtocol decode(DataReader in) throws IOException {
        try {
            GossipProtocol.Type msgType = MESSAGE_TYPES[in.readByte()];

            GossipProtocol result;

            switch (msgType) {
                case LONG_TERM_CONNECT: {
                    ClusterAddress to = readClusterAddress(in);
                    ClusterAddress from = readClusterAddress(in);

                    result = new LongTermConnect(from, to);

                    break;
                }
                case GOSSIP_UPDATE: {
                    ClusterAddress to = readClusterAddress(in);
                    ClusterAddress from = readClusterAddress(in);

                    Gossip gossip = decodeGossip(in);

                    result = new Update(from, to, gossip);

                    break;
                }
                case GOSSIP_UPDATE_DIGEST: {
                    ClusterAddress to = readClusterAddress(in);
                    ClusterAddress from = readClusterAddress(in);

                    GossipDigest digest = decodeGossipDigest(in);

                    result = new UpdateDigest(from, to, digest);

                    break;
                }
                case JOIN_REQUEST: {
                    InetSocketAddress to = readAddress(in);
                    ClusterNode fromNode = decodeNode(in);
                    String namespace = in.readUTF();

                    result = new JoinRequest(fromNode, namespace, to);

                    break;
                }
                case JOIN_ACCEPT: {
                    ClusterAddress to = readClusterAddress(in);
                    ClusterAddress from = readClusterAddress(in);

                    Gossip gossip = decodeGossip(in);

                    result = new JoinAccept(from, to, gossip);

                    break;
                }
                case JOIN_REJECT: {
                    ClusterAddress to = readClusterAddress(in);
                    ClusterAddress from = readClusterAddress(in);
                    InetSocketAddress rejectedAddr = readAddress(in);

                    JoinReject.RejectType rejectType = JoinReject.RejectType.values()[in.readInt()];

                    switch (rejectType) {
                        case TEMPORARY: {
                            result = JoinReject.retryLater(from, to, rejectedAddr);

                            break;
                        }
                        case PERMANENT: {
                            result = JoinReject.permanent(from, to, rejectedAddr);

                            break;
                        }
                        case FATAL: {
                            String reason = in.readUTF();

                            result = JoinReject.fatal(from, to, rejectedAddr, reason);

                            break;
                        }
                        case CONFLICT: {
                            result = JoinReject.conflict(from, to, rejectedAddr);

                            break;
                        }
                        default: {
                            throw new IllegalArgumentException("Unexpected reject reason type: " + rejectType);
                        }
                    }

                    break;
                }
                case HEARTBEAT_REQUEST: {
                    ClusterAddress to = readClusterAddress(in);
                    ClusterAddress from = readClusterAddress(in);

                    result = new HeartbeatRequest(from, to);

                    break;
                }
                case HEARTBEAT_REPLY: {
                    ClusterAddress to = readClusterAddress(in);
                    ClusterAddress from = readClusterAddress(in);

                    result = new HeartbeatReply(from, to);

                    break;
                }
                default: {
                    throw new IllegalStateException("Unexpected message type: " + msgType);
                }
            }

            return result;
        } finally {
            readStringDict.clear();
        }
    }

    private void encodeGossip(Gossip gossip, DataWriter out) throws IOException {
        // Epoch.
        out.writeVarLong(gossip.epoch());

        // Max join order.
        out.writeVarInt(gossip.maxJoinOrder());

        // Members.
        Map<ClusterNodeId, GossipNodeState> members = gossip.members();

        int membersSize = members.size();

        out.writeVarIntUnsigned(membersSize);

        if (membersSize > 0) {
            Set<ClusterNodeId> seen = gossip.seen();

            for (GossipNodeState member : members.values()) {
                encodeNodeState(member, seen, out);
            }
        }

        // Removed.
        encodeNodeIdSet(gossip.removed(), out);
    }

    private Gossip decodeGossip(DataReader in) throws IOException {
        // Epoch.
        long epoch = in.readVarLong();

        // Max join order.
        int maxJoinOrder = in.readVarInt();

        // Members.
        Map<ClusterNodeId, GossipNodeState> members;

        Set<ClusterNodeId> seen;

        int membersSize = in.readVarIntUnsigned();

        if (membersSize == 1) {
            seen = new HashSet<>(1, 1.f);

            GossipNodeState nodeState = decodeNodeState(seen, in);

            members = singletonMap(nodeState.id(), nodeState);

            seen = unmodifiableSet(seen);
        } else if (membersSize > 0) {
            seen = new HashSet<>(membersSize, 1.0f);

            members = new HashMap<>(membersSize, 1.0f);

            for (int i = 0; i < membersSize; i++) {
                GossipNodeState nodeState = decodeNodeState(seen, in);

                members.put(nodeState.id(), nodeState);
            }

            members = unmodifiableMap(members);
            seen = unmodifiableSet(seen);
        } else {
            members = emptyMap();
            seen = emptySet();
        }

        // Removed.
        Set<ClusterNodeId> removed = decodeNodeIdSet(in);

        return new Gossip(epoch, members, removed, seen, maxJoinOrder);
    }

    private void encodeGossipDigest(GossipDigest digest, DataWriter out) throws IOException {
        // Epoch.
        out.writeVarLong(digest.epoch());

        // Members.
        Map<ClusterNodeId, GossipNodeInfo> members = digest.membersInfo();

        int membersSize = members.size();

        out.writeVarIntUnsigned(membersSize);

        if (membersSize > 0) {
            Set<ClusterNodeId> seen = digest.seen();

            for (GossipNodeInfo node : members.values()) {
                encodeNodeInfo(node, seen, out);
            }
        }

        // Removed.
        encodeNodeIdSet(digest.removed(), out);
    }

    private GossipDigest decodeGossipDigest(DataReader in) throws IOException {
        // Epoch.
        long epoch = in.readVarLong();

        // Members.
        Map<ClusterNodeId, GossipNodeInfo> members;

        Set<ClusterNodeId> seen;

        int membersSize = in.readVarIntUnsigned();

        if (membersSize == 1) {
            seen = new HashSet<>(1, 1.0f);

            GossipNodeInfo nodeInfo = decodeNodeInfo(seen, in);

            members = singletonMap(nodeInfo.id(), nodeInfo);

            seen = unmodifiableSet(seen);
        } else if (membersSize > 0) {
            seen = new HashSet<>(membersSize, 1.0f);

            members = new HashMap<>(membersSize, 1.0f);

            for (int i = 0; i < membersSize; i++) {
                GossipNodeInfo nodeInfo = decodeNodeInfo(seen, in);

                members.put(nodeInfo.id(), nodeInfo);
            }

            members = unmodifiableMap(members);
            seen = unmodifiableSet(seen);
        } else {
            members = emptyMap();
            seen = emptySet();
        }

        // Removed.
        Set<ClusterNodeId> removed = decodeNodeIdSet(in);

        return new GossipDigest(epoch, members, removed, seen);
    }

    private void encodeNodeState(GossipNodeState member, Set<ClusterNodeId> seen, DataWriter out)
        throws IOException {
        // Node.
        encodeNode(member.node(), out);

        // Status.
        out.writeByte(member.status().ordinal());

        // Version.
        out.writeVarLong(member.version());

        // Seen.
        out.writeBoolean(seen.contains(member.id()));

        // Suspected.
        encodeNodeIdSet(member.suspected(), out);
    }

    private GossipNodeState decodeNodeState(Set<ClusterNodeId> seen, DataReader in) throws IOException {
        // Node.
        ClusterNode node = decodeNode(in);

        // Status.
        GossipNodeStatus status = NODE_STATUSES[in.readByte()];

        // Version.
        long version = in.readVarLong();

        // Seen.
        if (in.readBoolean()) {
            seen.add(node.id());
        }

        // Suspected.
        Set<ClusterNodeId> suspected = decodeNodeIdSet(in);

        return new GossipNodeState(node, status, version, suspected);
    }

    private void encodeNodeInfo(GossipNodeInfo node, Set<ClusterNodeId> seen, DataWriter out)
        throws IOException {
        ClusterNodeId id = node.id();

        CodecUtils.writeNodeId(id, out);

        out.writeVarLong(node.version());
        out.writeByte(node.status().ordinal());
        out.writeBoolean(seen.contains(id));
    }

    private GossipNodeInfo decodeNodeInfo(Set<ClusterNodeId> seen, DataReader in) throws IOException {
        ClusterNodeId id = CodecUtils.readNodeId(in);

        long version = in.readVarLong();

        GossipNodeStatus status = NODE_STATUSES[in.readByte()];

        if (in.readBoolean()) {
            seen.add(id);
        }

        return new GossipNodeInfo(id, status, version);
    }

    private void encodeNode(ClusterNode node, DataWriter out) throws IOException {
        // Address.
        writeClusterAddress(node.address(), out);

        // Node name.
        if (node.name().isEmpty()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(node.name());
        }

        // Order.
        out.writeVarInt(node.joinOrder());

        // Roles.
        encodeStringSet(node.roles(), out);

        // Properties.
        Map<String, String> props = node.properties();

        int propsSize = props.size();

        out.writeVarIntUnsigned(propsSize);

        if (propsSize > 0) {
            for (Map.Entry<String, String> e : props.entrySet()) {
                String key = e.getKey();
                String value = e.getValue();

                if (key == null) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);

                    writeStringWithDictionary(key, out);
                }

                if (value == null) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);

                    writeStringWithDictionary(value, out);
                }
            }
        }

        // Services.
        Map<String, ServiceInfo> services = node.services();

        int servicesSize = services.size();

        out.writeVarIntUnsigned(servicesSize);

        if (servicesSize > 0) {
            for (ServiceInfo service : services.values()) {
                writeStringWithDictionary(service.type(), out);

                Map<String, ServiceProperty<?>> serviceProps = service.properties();

                int servicePropsSize = serviceProps.size();

                out.writeVarIntUnsigned(servicePropsSize);

                if (servicePropsSize > 0) {
                    for (Map.Entry<String, ServiceProperty<?>> e : serviceProps.entrySet()) {
                        encodeServiceProperty(e.getValue(), out);
                    }
                }
            }
        }

        // System info.
        ClusterNodeRuntime sysInfo = node.runtime();

        out.writeVarInt(sysInfo.cpus());
        out.writeVarLong(sysInfo.maxMemory());

        writeStringWithDictionary(sysInfo.osName(), out);
        writeStringWithDictionary(sysInfo.osArch(), out);
        writeStringWithDictionary(sysInfo.osVersion(), out);
        writeStringWithDictionary(sysInfo.jvmVersion(), out);
        writeStringWithDictionary(sysInfo.jvmName(), out);
        writeStringWithDictionary(sysInfo.jvmVendor(), out);
        writeStringWithDictionary(sysInfo.pid(), out);
    }

    private ClusterNode decodeNode(DataReader in) throws IOException {
        // Address.
        ClusterAddress addr = readClusterAddress(in);

        // Node name.
        String nodeName = null;

        if (in.readBoolean()) {
            nodeName = in.readUTF();
        }

        // Order.
        int order = in.readVarInt();

        ClusterNodeId localNodeId = this.localNodeId;

        if (localNodeId == null) {
            localNodeId = this.localNodeId = localNodeIdRef.get();
        }

        // Roles.
        Set<String> roles = decodeStringSet(in);

        // Properties.
        Map<String, String> props;

        int propsSize = in.readVarIntUnsigned();

        if (propsSize > 0) {
            props = new HashMap<>(propsSize, 1.0f);

            for (int i = 0; i < propsSize; i++) {
                String key = null;
                String val = null;

                if (in.readBoolean()) {
                    key = readStringWithDictionary(in);
                }

                if (in.readBoolean()) {
                    val = readStringWithDictionary(in);
                }

                props.put(key, val);
            }

            props = unmodifiableMap(props);
        } else {
            props = emptyMap();
        }

        // Services.
        Map<String, ServiceInfo> services;

        int servicesSize = in.readVarIntUnsigned();

        if (servicesSize > 0) {
            services = new HashMap<>(servicesSize, 1.0f);

            for (int i = 0; i < servicesSize; i++) {
                String type = readStringWithDictionary(in);

                int propSize = in.readVarIntUnsigned();

                Map<String, ServiceProperty<?>> serviceProps;

                if (propSize > 0) {
                    serviceProps = new HashMap<>(propSize, 1.0f);

                    for (int j = 0; j < propSize; j++) {
                        ServiceProperty<?> prop = decodeServiceProperty(in);

                        serviceProps.put(prop.name(), prop);
                    }

                    serviceProps = unmodifiableMap(serviceProps);
                } else {
                    serviceProps = emptyMap();
                }

                services.put(type, new DefaultServiceInfo(type, serviceProps));
            }

            services = unmodifiableMap(services);
        } else {
            services = emptyMap();
        }

        // System info.
        int cpus = in.readVarInt();
        long maxMemory = in.readVarLong();

        String osName = readStringWithDictionary(in);
        String osArch = readStringWithDictionary(in);
        String osVersion = readStringWithDictionary(in);
        String jvmVersion = readStringWithDictionary(in);
        String jvmName = readStringWithDictionary(in);
        String jvmVendor = readStringWithDictionary(in);
        String pid = readStringWithDictionary(in);

        ClusterNodeRuntime systemInfo = new DefaultClusterNodeRuntime(
            cpus,
            maxMemory,
            osName,
            osArch,
            osVersion,
            jvmVersion,
            jvmName,
            jvmVendor,
            pid
        );

        boolean localNode = addr.id().equals(localNodeId);

        return new DefaultClusterNode(addr, nodeName, localNode, order, roles, props, services, systemInfo);
    }

    private void encodeServiceProperty(ServiceProperty<?> prop, DataWriter out) throws IOException {
        writeStringWithDictionary(prop.name(), out);

        out.writeByte(prop.type().ordinal());

        switch (prop.type()) {
            case STRING: {
                writeStringWithDictionary((String)prop.value(), out);

                break;
            }
            case INTEGER: {
                out.writeVarInt((Integer)prop.value());

                break;
            }
            case LONG: {
                out.writeVarLong((Long)prop.value());

                break;
            }
            case BOOLEAN: {
                out.writeBoolean((Boolean)prop.value());

                break;
            }
            default: {
                throw new IllegalArgumentException("Unsupported property type: " + prop);
            }
        }
    }

    private ServiceProperty<?> decodeServiceProperty(DataReader in) throws IOException {
        String name = readStringWithDictionary(in);

        ServiceProperty.Type type = SERVICE_PROP_TYPES[in.readByte()];

        switch (type) {
            case STRING: {
                return ServiceProperty.forString(name, readStringWithDictionary(in));
            }
            case INTEGER: {
                return ServiceProperty.forInteger(name, in.readVarInt());
            }
            case LONG: {
                return ServiceProperty.forLong(name, in.readVarLong());
            }
            case BOOLEAN: {
                return ServiceProperty.forBoolean(name, in.readBoolean());
            }
            default: {
                throw new IllegalArgumentException("Unsupported property type: " + type);
            }
        }
    }

    private void encodeNodeIdSet(Set<ClusterNodeId> set, DataWriter out) throws IOException {
        int size = set.size();

        out.writeVarIntUnsigned(size);

        if (size > 0) {
            for (ClusterNodeId id : set) {
                CodecUtils.writeNodeId(id, out);
            }
        }
    }

    private Set<ClusterNodeId> decodeNodeIdSet(DataReader in) throws IOException {
        int size = in.readVarIntUnsigned();

        if (size == 1) {
            return singleton(CodecUtils.readNodeId(in));
        } else if (size > 0) {
            Set<ClusterNodeId> set = new HashSet<>(size, 1.0f);

            for (int i = 0; i < size; i++) {
                set.add(CodecUtils.readNodeId(in));
            }

            return unmodifiableSet(set);
        } else {
            return emptySet();
        }
    }

    private void encodeStringSet(Set<String> set, DataWriter out) throws IOException {
        int size = set.size();

        out.writeVarIntUnsigned(size);

        if (size > 0) {
            for (String str : set) {
                writeStringWithDictionary(str, out);
            }
        }
    }

    private Set<String> decodeStringSet(DataReader in) throws IOException {
        int size = in.readVarIntUnsigned();

        if (size == 1) {
            return singleton(readStringWithDictionary(in));
        } else if (size > 0) {
            Set<String> set = new HashSet<>(size, 1.0f);

            for (int i = 0; i < size; i++) {
                set.add(readStringWithDictionary(in));
            }

            return unmodifiableSet(set);
        } else {
            return emptySet();
        }
    }

    private void writeStringWithDictionary(String str, DataWriter out) throws IOException {
        Integer code = writeStringDict.get(str);

        if (code == null) {
            code = writeStringDict.size() + 1;

            writeStringDict.put(str, code);

            // Use negative code as a flag for reader.
            // So that it would understand that this is the first time when this string appears
            // on the read stream and that string content should be read after the code.
            out.writeVarInt(-code);

            out.writeUTF(str);
        } else {
            out.writeVarInt(code);
        }
    }

    private String readStringWithDictionary(DataReader in) throws IOException {
        int code = in.readVarInt();

        String str;

        if (code < 0) {
            str = in.readUTF();

            readStringDict.put(-code, str);
        } else {
            str = readStringDict.get(code);
        }

        return str;
    }
}
