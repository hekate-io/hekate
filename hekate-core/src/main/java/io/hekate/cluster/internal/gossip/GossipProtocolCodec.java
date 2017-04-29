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

package io.hekate.cluster.internal.gossip;

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterNodeRuntime;
import io.hekate.cluster.internal.DefaultClusterNode;
import io.hekate.cluster.internal.DefaultClusterNodeRuntime;
import io.hekate.cluster.internal.gossip.GossipProtocol.Connect;
import io.hekate.cluster.internal.gossip.GossipProtocol.HeartbeatReply;
import io.hekate.cluster.internal.gossip.GossipProtocol.HeartbeatRequest;
import io.hekate.cluster.internal.gossip.GossipProtocol.JoinAccept;
import io.hekate.cluster.internal.gossip.GossipProtocol.JoinReject;
import io.hekate.cluster.internal.gossip.GossipProtocol.JoinRequest;
import io.hekate.cluster.internal.gossip.GossipProtocol.Update;
import io.hekate.cluster.internal.gossip.GossipProtocol.UpdateDigest;
import io.hekate.codec.Codec;
import io.hekate.codec.CodecUtils;
import io.hekate.codec.DataReader;
import io.hekate.codec.DataWriter;
import io.hekate.core.ServiceInfo;
import io.hekate.core.service.internal.DefaultServiceInfo;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class GossipProtocolCodec implements Codec<GossipProtocol> {
    public static final String PROTOCOL_ID = "hekate.cluster";

    private static final GossipProtocol.Type[] TYPES = GossipProtocol.Type.values();

    private static final GossipNodeStatus[] NODE_STATUSES = GossipNodeStatus.values();

    private final Map<Integer, String> readStringDict = new HashMap<>();

    private final Map<String, Integer> writeStringDict = new HashMap<>();

    private final AtomicReference<ClusterNodeId> localNodeIdRef;

    private ClusterNodeId localNodeId;

    public GossipProtocolCodec(AtomicReference<ClusterNodeId> localNodeIdRef) {
        assert localNodeIdRef != null : "Local node ID is null.";

        this.localNodeIdRef = localNodeIdRef;
    }

    @Override
    public boolean isStateful() {
        return true;
    }

    @Override
    public void encode(GossipProtocol msg, DataWriter out) throws IOException {
        try {
            GossipProtocol.Type msgType = msg.type();

            out.writeByte(msgType.ordinal());

            switch (msgType) {
                case CONNECT: {
                    Connect connect = (Connect)msg;

                    CodecUtils.writeNodeId(connect.nodeId(), out);

                    break;
                }
                case GOSSIP_UPDATE: {
                    Update update = (Update)msg;

                    CodecUtils.writeClusterAddress(update.to(), out);
                    CodecUtils.writeClusterAddress(update.from(), out);

                    encodeGossip(update.gossip(), out);

                    break;
                }
                case GOSSIP_UPDATE_DIGEST: {
                    UpdateDigest digest = (UpdateDigest)msg;

                    CodecUtils.writeClusterAddress(digest.to(), out);
                    CodecUtils.writeClusterAddress(digest.from(), out);

                    encodeGossipDigest(digest.digest(), out);

                    break;
                }
                case JOIN_REQUEST: {
                    JoinRequest request = (JoinRequest)msg;

                    CodecUtils.writeAddress(request.toAddress(), out);

                    ClusterNode node = request.fromNode();

                    encodeNode(node, out);

                    out.writeUTF(request.cluster());

                    break;
                }
                case JOIN_ACCEPT: {
                    JoinAccept accept = (JoinAccept)msg;

                    CodecUtils.writeClusterAddress(accept.to(), out);
                    CodecUtils.writeClusterAddress(accept.from(), out);

                    encodeGossip(accept.gossip(), out);

                    break;
                }
                case JOIN_REJECT: {
                    JoinReject reject = (JoinReject)msg;

                    CodecUtils.writeClusterAddress(reject.to(), out);
                    CodecUtils.writeClusterAddress(reject.from(), out);
                    CodecUtils.writeAddress(reject.rejectedAddress(), out);

                    JoinReject.RejectType rejectType = reject.rejectType();

                    out.writeInt(rejectType.ordinal());

                    if (rejectType == JoinReject.RejectType.FATAL) {
                        out.writeUTF(reject.reason());
                    }

                    break;
                }
                case HEARTBEAT_REQUEST: {
                    HeartbeatRequest request = (HeartbeatRequest)msg;

                    CodecUtils.writeClusterAddress(request.to(), out);
                    CodecUtils.writeClusterAddress(request.from(), out);

                    break;
                }
                case HEARTBEAT_REPLY: {
                    HeartbeatReply reply = (HeartbeatReply)msg;

                    CodecUtils.writeClusterAddress(reply.to(), out);
                    CodecUtils.writeClusterAddress(reply.from(), out);

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
            GossipProtocol.Type msgType = TYPES[in.readByte()];

            GossipProtocol result;

            switch (msgType) {
                case CONNECT: {
                    ClusterNodeId nodeId = CodecUtils.readNodeId(in);

                    result = new Connect(nodeId);

                    break;
                }
                case GOSSIP_UPDATE: {
                    ClusterAddress to = CodecUtils.readClusterAddress(in);
                    ClusterAddress from = CodecUtils.readClusterAddress(in);

                    Gossip gossip = decodeGossip(in);

                    result = new Update(from, to, gossip);

                    break;
                }
                case GOSSIP_UPDATE_DIGEST: {
                    ClusterAddress to = CodecUtils.readClusterAddress(in);
                    ClusterAddress from = CodecUtils.readClusterAddress(in);

                    GossipDigest digest = decodeGossipDigest(in);

                    result = new UpdateDigest(from, to, digest);

                    break;
                }
                case JOIN_REQUEST: {
                    InetSocketAddress to = CodecUtils.readAddress(in);

                    ClusterNode fromNode = decodeNode(in);

                    String cluster = in.readUTF();

                    result = new JoinRequest(fromNode, cluster, to);

                    break;
                }
                case JOIN_ACCEPT: {
                    ClusterAddress to = CodecUtils.readClusterAddress(in);
                    ClusterAddress from = CodecUtils.readClusterAddress(in);

                    Gossip gossip = decodeGossip(in);

                    result = new JoinAccept(from, to, gossip);

                    break;
                }
                case JOIN_REJECT: {
                    ClusterAddress to = CodecUtils.readClusterAddress(in);
                    ClusterAddress from = CodecUtils.readClusterAddress(in);
                    InetSocketAddress rejectedAddr = CodecUtils.readAddress(in);

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
                        default: {
                            throw new IllegalArgumentException("Unexpected reject reason type: " + rejectType);
                        }
                    }

                    break;
                }
                case HEARTBEAT_REQUEST: {
                    ClusterAddress to = CodecUtils.readClusterAddress(in);
                    ClusterAddress from = CodecUtils.readClusterAddress(in);

                    result = new HeartbeatRequest(from, to);

                    break;
                }
                case HEARTBEAT_REPLY: {
                    ClusterAddress to = CodecUtils.readClusterAddress(in);
                    ClusterAddress from = CodecUtils.readClusterAddress(in);

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
        // Version.
        out.writeLong(gossip.version());

        // Max join order.
        out.writeInt(gossip.maxJoinOrder());

        // Members.
        Map<ClusterNodeId, GossipNodeState> members = gossip.members();

        int membersSize = members.size();

        out.writeInt(membersSize);

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
        // Version.
        long version = in.readLong();

        // Max join order.
        int maxJoinOrder = in.readInt();

        // Members.
        Map<ClusterNodeId, GossipNodeState> members;

        Set<ClusterNodeId> seen;

        int membersSize = in.readInt();

        if (membersSize == 1) {
            seen = new HashSet<>(1, 1.f);

            GossipNodeState nodeState = decodeNodeState(seen, in);

            members = Collections.singletonMap(nodeState.id(), nodeState);

            seen = Collections.unmodifiableSet(seen);
        } else if (membersSize > 0) {
            seen = new HashSet<>(membersSize, 1.0f);

            members = new HashMap<>(membersSize, 1.0f);

            for (int i = 0; i < membersSize; i++) {
                GossipNodeState nodeState = decodeNodeState(seen, in);

                members.put(nodeState.id(), nodeState);
            }

            members = Collections.unmodifiableMap(members);
            seen = Collections.unmodifiableSet(seen);
        } else {
            members = Collections.emptyMap();
            seen = Collections.emptySet();
        }

        // Removed.
        Set<ClusterNodeId> removed = decodeNodeIdSet(in);

        return new Gossip(version, members, removed, seen, maxJoinOrder);
    }

    private void encodeGossipDigest(GossipDigest digest, DataWriter out) throws IOException {
        // Version.
        out.writeLong(digest.version());

        // Members.
        Map<ClusterNodeId, GossipNodeInfo> members = digest.membersInfo();

        int membersSize = members.size();

        out.writeInt(membersSize);

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
        // Version.
        long version = in.readLong();

        // Members.
        Map<ClusterNodeId, GossipNodeInfo> members;

        Set<ClusterNodeId> seen;

        int membersSize = in.readInt();

        if (membersSize == 1) {
            seen = new HashSet<>(1, 1.0f);

            GossipNodeInfo nodeInfo = decodeNodeInfo(seen, in);

            members = Collections.singletonMap(nodeInfo.id(), nodeInfo);

            seen = Collections.unmodifiableSet(seen);
        } else if (membersSize > 0) {
            seen = new HashSet<>(membersSize, 1.0f);

            members = new HashMap<>(membersSize, 1.0f);

            for (int i = 0; i < membersSize; i++) {
                GossipNodeInfo nodeInfo = decodeNodeInfo(seen, in);

                members.put(nodeInfo.id(), nodeInfo);
            }

            members = Collections.unmodifiableMap(members);
            seen = Collections.unmodifiableSet(seen);
        } else {
            members = Collections.emptyMap();
            seen = Collections.emptySet();
        }

        // Removed.
        Set<ClusterNodeId> removed = decodeNodeIdSet(in);

        return new GossipDigest(version, members, removed, seen);
    }

    private void encodeNodeState(GossipNodeState member, Set<ClusterNodeId> seen, DataWriter out)
        throws IOException {
        // Node.
        encodeNode(member.node(), out);

        // Status.
        out.writeByte(member.status().ordinal());

        // Version.
        out.writeLong(member.version());

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
        long version = in.readLong();

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

        out.writeLong(node.version());
        out.writeByte(node.status().ordinal());
        out.writeBoolean(seen.contains(id));
    }

    private GossipNodeInfo decodeNodeInfo(Set<ClusterNodeId> seen, DataReader in) throws IOException {
        ClusterNodeId id = CodecUtils.readNodeId(in);

        long version = in.readLong();

        GossipNodeStatus status = NODE_STATUSES[in.readByte()];

        if (in.readBoolean()) {
            seen.add(id);
        }

        return new GossipNodeInfo(id, status, version);
    }

    private void encodeNode(ClusterNode node, DataWriter out) throws IOException {
        // Address.
        CodecUtils.writeClusterAddress(node.address(), out);

        // Node name.
        if (node.name().isEmpty()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(node.name());
        }

        // Order.
        out.writeInt(node.joinOrder());

        // Roles.
        encodeStringSet(node.roles(), out);

        // Properties.
        Map<String, String> props = node.properties();

        int propsSize = props.size();

        out.writeInt(propsSize);

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

        out.writeInt(servicesSize);

        if (servicesSize > 0) {
            for (ServiceInfo service : services.values()) {
                writeStringWithDictionary(service.type(), out);

                Map<String, Set<String>> serviceProps = service.properties();

                int servicePropsSize = serviceProps.size();

                out.writeInt(servicePropsSize);

                if (servicePropsSize > 0) {
                    for (Map.Entry<String, Set<String>> e : serviceProps.entrySet()) {
                        writeStringWithDictionary(e.getKey(), out);

                        encodeStringSet(e.getValue(), out);
                    }
                }
            }
        }

        // System info.
        ClusterNodeRuntime sysInfo = node.runtime();

        out.writeInt(sysInfo.cpus());
        out.writeLong(sysInfo.maxMemory());

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
        ClusterAddress addr = CodecUtils.readClusterAddress(in);

        // Node name.
        String nodeName = null;

        if (in.readBoolean()) {
            nodeName = in.readUTF();
        }

        // Order.
        int order = in.readInt();

        ClusterNodeId localNodeId = this.localNodeId;

        if (localNodeId == null) {
            localNodeId = this.localNodeId = localNodeIdRef.get();
        }

        // Roles.
        Set<String> roles = decodeStringSet(in);

        // Properties.
        Map<String, String> props;

        int propsSize = in.readInt();

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

            props = Collections.unmodifiableMap(props);
        } else {
            props = Collections.emptyMap();
        }

        // Services.
        Map<String, ServiceInfo> services;

        int servicesSize = in.readInt();

        if (servicesSize > 0) {
            services = new HashMap<>(servicesSize, 1.0f);

            for (int i = 0; i < servicesSize; i++) {
                String type = readStringWithDictionary(in);

                int propSize = in.readInt();

                Map<String, Set<String>> serviceProps;

                if (propSize > 0) {
                    serviceProps = new HashMap<>(propSize, 1.0f);

                    for (int j = 0; j < propSize; j++) {
                        String key = readStringWithDictionary(in);

                        Set<String> values = decodeStringSet(in);

                        serviceProps.put(key, values);
                    }

                    serviceProps = Collections.unmodifiableMap(serviceProps);
                } else {
                    serviceProps = Collections.emptyMap();
                }

                services.put(type, new DefaultServiceInfo(type, serviceProps));
            }

            services = Collections.unmodifiableMap(services);
        } else {
            services = Collections.emptyMap();
        }

        // System info.
        int cpus = in.readInt();
        long maxMemory = in.readLong();

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

    private void encodeNodeIdSet(Set<ClusterNodeId> set, DataWriter out) throws IOException {
        int size = set.size();

        out.writeInt(size);

        if (size > 0) {
            for (ClusterNodeId id : set) {
                CodecUtils.writeNodeId(id, out);
            }
        }
    }

    private Set<ClusterNodeId> decodeNodeIdSet(DataReader in) throws IOException {
        int size = in.readInt();

        if (size == 1) {
            return Collections.singleton(CodecUtils.readNodeId(in));
        } else if (size > 0) {
            Set<ClusterNodeId> set = new HashSet<>(size, 1.0f);

            for (int i = 0; i < size; i++) {
                set.add(CodecUtils.readNodeId(in));
            }

            return Collections.unmodifiableSet(set);
        } else {
            return Collections.emptySet();
        }
    }

    private void encodeStringSet(Set<String> set, DataWriter out) throws IOException {
        int size = set.size();

        out.writeInt(size);

        if (size > 0) {
            for (String str : set) {
                writeStringWithDictionary(str, out);
            }
        }
    }

    private Set<String> decodeStringSet(DataReader in) throws IOException {
        int size = in.readInt();

        if (size == 1) {
            return Collections.singleton(readStringWithDictionary(in));
        } else if (size > 0) {
            Set<String> set = new HashSet<>(size, 1.0f);

            for (int i = 0; i < size; i++) {
                set.add(readStringWithDictionary(in));
            }

            return Collections.unmodifiableSet(set);
        } else {
            return Collections.emptySet();
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
            out.writeInt(-code);

            out.writeUTF(str);
        } else {
            out.writeInt(code);
        }
    }

    private String readStringWithDictionary(DataReader in) throws IOException {
        int code = in.readInt();

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
