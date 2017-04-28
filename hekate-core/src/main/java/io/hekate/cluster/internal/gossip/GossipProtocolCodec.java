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
import io.hekate.cluster.ClusterJvmInfo;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterUuid;
import io.hekate.cluster.internal.DefaultClusterJvmInfo;
import io.hekate.cluster.internal.DefaultClusterNode;
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

    private final AtomicReference<ClusterUuid> localNodeIdRef;

    private ClusterUuid localNodeId;

    public GossipProtocolCodec(AtomicReference<ClusterUuid> localNodeIdRef) {
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
            GossipProtocol.Type msgType = msg.getType();

            out.writeByte(msgType.ordinal());

            switch (msgType) {
                case CONNECT: {
                    Connect connect = (Connect)msg;

                    CodecUtils.writeNodeId(connect.getNodeId(), out);

                    break;
                }
                case GOSSIP_UPDATE: {
                    Update update = (Update)msg;

                    CodecUtils.writeClusterAddress(update.getTo(), out);
                    CodecUtils.writeClusterAddress(update.getFrom(), out);

                    encodeGossip(update.getGossip(), out);

                    break;
                }
                case GOSSIP_UPDATE_DIGEST: {
                    UpdateDigest digest = (UpdateDigest)msg;

                    CodecUtils.writeClusterAddress(digest.getTo(), out);
                    CodecUtils.writeClusterAddress(digest.getFrom(), out);

                    encodeGossipDigest(digest.getDigest(), out);

                    break;
                }
                case JOIN_REQUEST: {
                    JoinRequest request = (JoinRequest)msg;

                    CodecUtils.writeAddress(request.getToAddress(), out);

                    ClusterNode node = request.getFromNode();

                    encodeNode(node, out);

                    out.writeUTF(request.getCluster());

                    break;
                }
                case JOIN_ACCEPT: {
                    JoinAccept accept = (JoinAccept)msg;

                    CodecUtils.writeClusterAddress(accept.getTo(), out);
                    CodecUtils.writeClusterAddress(accept.getFrom(), out);

                    encodeGossip(accept.getGossip(), out);

                    break;
                }
                case JOIN_REJECT: {
                    JoinReject reject = (JoinReject)msg;

                    CodecUtils.writeClusterAddress(reject.getTo(), out);
                    CodecUtils.writeClusterAddress(reject.getFrom(), out);
                    CodecUtils.writeAddress(reject.getRejectedAddress(), out);

                    JoinReject.RejectType rejectType = reject.getRejectType();

                    out.writeInt(rejectType.ordinal());

                    if (rejectType == JoinReject.RejectType.FATAL) {
                        out.writeUTF(reject.getReason());
                    }

                    break;
                }
                case HEARTBEAT_REQUEST: {
                    HeartbeatRequest request = (HeartbeatRequest)msg;

                    CodecUtils.writeClusterAddress(request.getTo(), out);
                    CodecUtils.writeClusterAddress(request.getFrom(), out);

                    break;
                }
                case HEARTBEAT_REPLY: {
                    HeartbeatReply reply = (HeartbeatReply)msg;

                    CodecUtils.writeClusterAddress(reply.getTo(), out);
                    CodecUtils.writeClusterAddress(reply.getFrom(), out);

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
                    ClusterUuid nodeId = CodecUtils.readNodeId(in);

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
        out.writeLong(gossip.getVersion());

        // Max join order.
        out.writeInt(gossip.getMaxJoinOrder());

        // Members.
        Map<ClusterUuid, GossipNodeState> members = gossip.getMembers();

        int membersSize = members.size();

        out.writeInt(membersSize);

        if (membersSize > 0) {
            Set<ClusterUuid> seen = gossip.getSeen();

            for (GossipNodeState member : members.values()) {
                encodeNodeState(member, seen, out);
            }
        }

        // Removed.
        encodeNodeIdSet(gossip.getRemoved(), out);
    }

    private Gossip decodeGossip(DataReader in) throws IOException {
        // Version.
        long version = in.readLong();

        // Max join order.
        int maxJoinOrder = in.readInt();

        // Members.
        Map<ClusterUuid, GossipNodeState> members;

        Set<ClusterUuid> seen;

        int membersSize = in.readInt();

        if (membersSize == 1) {
            seen = new HashSet<>(1, 1.f);

            GossipNodeState nodeState = decodeNodeState(seen, in);

            members = Collections.singletonMap(nodeState.getId(), nodeState);

            seen = Collections.unmodifiableSet(seen);
        } else if (membersSize > 0) {
            seen = new HashSet<>(membersSize, 1.0f);

            members = new HashMap<>(membersSize, 1.0f);

            for (int i = 0; i < membersSize; i++) {
                GossipNodeState nodeState = decodeNodeState(seen, in);

                members.put(nodeState.getId(), nodeState);
            }

            members = Collections.unmodifiableMap(members);
            seen = Collections.unmodifiableSet(seen);
        } else {
            members = Collections.emptyMap();
            seen = Collections.emptySet();
        }

        // Removed.
        Set<ClusterUuid> removed = decodeNodeIdSet(in);

        return new Gossip(version, members, removed, seen, maxJoinOrder);
    }

    private void encodeGossipDigest(GossipDigest digest, DataWriter out) throws IOException {
        // Version.
        out.writeLong(digest.getVersion());

        // Members.
        Map<ClusterUuid, GossipNodeInfo> members = digest.getMembersInfo();

        int membersSize = members.size();

        out.writeInt(membersSize);

        if (membersSize > 0) {
            Set<ClusterUuid> seen = digest.getSeen();

            for (GossipNodeInfo node : members.values()) {
                encodeNodeInfo(node, seen, out);
            }
        }

        // Removed.
        encodeNodeIdSet(digest.getRemoved(), out);
    }

    private GossipDigest decodeGossipDigest(DataReader in) throws IOException {
        // Version.
        long version = in.readLong();

        // Members.
        Map<ClusterUuid, GossipNodeInfo> members;

        Set<ClusterUuid> seen;

        int membersSize = in.readInt();

        if (membersSize == 1) {
            seen = new HashSet<>(1, 1.0f);

            GossipNodeInfo nodeInfo = decodeNodeInfo(seen, in);

            members = Collections.singletonMap(nodeInfo.getId(), nodeInfo);

            seen = Collections.unmodifiableSet(seen);
        } else if (membersSize > 0) {
            seen = new HashSet<>(membersSize, 1.0f);

            members = new HashMap<>(membersSize, 1.0f);

            for (int i = 0; i < membersSize; i++) {
                GossipNodeInfo nodeInfo = decodeNodeInfo(seen, in);

                members.put(nodeInfo.getId(), nodeInfo);
            }

            members = Collections.unmodifiableMap(members);
            seen = Collections.unmodifiableSet(seen);
        } else {
            members = Collections.emptyMap();
            seen = Collections.emptySet();
        }

        // Removed.
        Set<ClusterUuid> removed = decodeNodeIdSet(in);

        return new GossipDigest(version, members, removed, seen);
    }

    private void encodeNodeState(GossipNodeState member, Set<ClusterUuid> seen, DataWriter out)
        throws IOException {
        // Node.
        encodeNode(member.getNode(), out);

        // Status.
        out.writeByte(member.getStatus().ordinal());

        // Version.
        out.writeLong(member.getVersion());

        // Seen.
        out.writeBoolean(seen.contains(member.getId()));

        // Suspected.
        encodeNodeIdSet(member.getSuspected(), out);
    }

    private GossipNodeState decodeNodeState(Set<ClusterUuid> seen, DataReader in) throws IOException {
        // Node.
        ClusterNode node = decodeNode(in);

        // Status.
        GossipNodeStatus status = NODE_STATUSES[in.readByte()];

        // Version.
        long version = in.readLong();

        // Seen.
        if (in.readBoolean()) {
            seen.add(node.getId());
        }

        // Suspected.
        Set<ClusterUuid> suspected = decodeNodeIdSet(in);

        return new GossipNodeState(node, status, version, suspected);
    }

    private void encodeNodeInfo(GossipNodeInfo node, Set<ClusterUuid> seen, DataWriter out)
        throws IOException {
        ClusterUuid id = node.getId();

        CodecUtils.writeNodeId(id, out);

        out.writeLong(node.getVersion());
        out.writeByte(node.getStatus().ordinal());
        out.writeBoolean(seen.contains(id));
    }

    private GossipNodeInfo decodeNodeInfo(Set<ClusterUuid> seen, DataReader in) throws IOException {
        ClusterUuid id = CodecUtils.readNodeId(in);

        long version = in.readLong();

        GossipNodeStatus status = NODE_STATUSES[in.readByte()];

        if (in.readBoolean()) {
            seen.add(id);
        }

        return new GossipNodeInfo(id, status, version);
    }

    private void encodeNode(ClusterNode node, DataWriter out) throws IOException {
        // Address.
        CodecUtils.writeClusterAddress(node.getAddress(), out);

        // Node name.
        if (node.getName().isEmpty()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(node.getName());
        }

        // Order.
        out.writeInt(node.getJoinOrder());

        // Roles.
        encodeStringSet(node.getRoles(), out);

        // Properties.
        Map<String, String> props = node.getProperties();

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
        Map<String, ServiceInfo> services = node.getServices();

        int servicesSize = services.size();

        out.writeInt(servicesSize);

        if (servicesSize > 0) {
            for (ServiceInfo service : services.values()) {
                writeStringWithDictionary(service.getType(), out);

                Map<String, Set<String>> serviceProps = service.getProperties();

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
        ClusterJvmInfo sysInfo = node.getJvmInfo();

        out.writeInt(sysInfo.getCpus());
        out.writeLong(sysInfo.getMaxMemory());

        writeStringWithDictionary(sysInfo.getOsName(), out);
        writeStringWithDictionary(sysInfo.getOsArch(), out);
        writeStringWithDictionary(sysInfo.getOsVersion(), out);
        writeStringWithDictionary(sysInfo.getJvmVersion(), out);
        writeStringWithDictionary(sysInfo.getJvmName(), out);
        writeStringWithDictionary(sysInfo.getJvmVendor(), out);
        writeStringWithDictionary(sysInfo.getPid(), out);
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

        ClusterUuid localNodeId = this.localNodeId;

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

        ClusterJvmInfo systemInfo = new DefaultClusterJvmInfo(
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

        boolean localNode = addr.getId().equals(localNodeId);

        return new DefaultClusterNode(addr, nodeName, localNode, order, roles, props, services, systemInfo);
    }

    private void encodeNodeIdSet(Set<ClusterUuid> set, DataWriter out) throws IOException {
        int size = set.size();

        out.writeInt(size);

        if (size > 0) {
            for (ClusterUuid id : set) {
                CodecUtils.writeNodeId(id, out);
            }
        }
    }

    private Set<ClusterUuid> decodeNodeIdSet(DataReader in) throws IOException {
        int size = in.readInt();

        if (size == 1) {
            return Collections.singleton(CodecUtils.readNodeId(in));
        } else if (size > 0) {
            Set<ClusterUuid> set = new HashSet<>(size, 1.0f);

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
