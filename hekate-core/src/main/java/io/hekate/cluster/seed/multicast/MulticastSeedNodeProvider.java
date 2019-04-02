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

package io.hekate.cluster.seed.multicast;

import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.AddressUtils;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.core.internal.util.Utils;
import io.hekate.core.jmx.JmxSupport;
import io.hekate.network.netty.NettyUtils;
import io.hekate.util.async.Waiting;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.InternetProtocolFamily;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.concurrent.ScheduledFuture;
import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;

/**
 * Multicast-based seed node provider.
 *
 * <p>
 * This provider uses <a href="https://en.wikipedia.org/wiki/IP_multicast" target="_blank">IP multicasting</a> for seed nodes discovery.
 * When this provider starts it periodically sends multicast messages and awaits for responses from other nodes up to the configurable
 * timeout value.
 * </p>
 *
 * <p>
 * Configuration of this provider is represented by the {@link MulticastSeedNodeProviderConfig} class. Please see its documentation for
 * information about available configuration options.
 * </p>
 *
 * @see ClusterServiceFactory#setSeedNodeProvider(SeedNodeProvider)
 * @see MulticastSeedNodeProviderConfig
 */
public class MulticastSeedNodeProvider implements SeedNodeProvider, JmxSupport<MulticastSeedNodeProviderJmx> {
    private enum MessageTYpe {
        DISCOVERY,

        SEED_NODE_INFO
    }

    private static class SeedNode {
        private final InetSocketAddress address;

        private final String cluster;

        public SeedNode(InetSocketAddress address, String cluster) {
            this.address = address;
            this.cluster = cluster;
        }

        public InetSocketAddress address() {
            return address;
        }

        public String cluster() {
            return cluster;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (!(o instanceof SeedNode)) {
                return false;
            }

            SeedNode seedNode = (SeedNode)o;

            if (!Objects.equals(address, seedNode.address)) {
                return false;
            }

            return Objects.equals(cluster, seedNode.cluster);
        }

        @Override
        public int hashCode() {
            int result = address != null ? address.hashCode() : 0;

            result = 31 * result + (cluster != null ? cluster.hashCode() : 0);

            return result;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[cluster=" + cluster + ", address=" + address + ']';
        }
    }

    private static final Logger log = LoggerFactory.getLogger(MulticastSeedNodeProvider.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    @ToStringIgnore
    private final Object mux = new Object();

    private final InetSocketAddress group;

    private final InternetProtocolFamily ipVer;

    private final int ttl;

    private final long interval;

    private final long waitTime;

    private final boolean loopBackDisabled;

    @ToStringIgnore
    private SeedNode localNode;

    @ToStringIgnore
    private Set<SeedNode> seedNodes;

    @ToStringIgnore
    private NioEventLoopGroup eventLoop;

    @ToStringIgnore
    private DatagramChannel listener;

    @ToStringIgnore
    private DatagramChannel sender;

    @ToStringIgnore
    private ScheduledFuture<?> discoveryFuture;

    /**
     * Constructs new instance with all configuration options set to their default values (see {@link MulticastSeedNodeProviderConfig}).
     *
     * @throws UnknownHostException If failed to resolve multicast group address.
     */
    public MulticastSeedNodeProvider() throws UnknownHostException {
        this(new MulticastSeedNodeProviderConfig());
    }

    /**
     * Constructs new instance.
     *
     * @param cfg Configuration.
     *
     * @throws UnknownHostException If failed to resolve multicast group address.
     */
    public MulticastSeedNodeProvider(MulticastSeedNodeProviderConfig cfg) throws UnknownHostException {
        ConfigCheck check = ConfigCheck.get(getClass());

        check.notNull(cfg, "configuration");
        check.positive(cfg.getPort(), "port");
        check.nonNegative(cfg.getTtl(), "TTL");
        check.notEmpty(cfg.getGroup(), "multicast group");
        check.positive(cfg.getInterval(), "discovery interval");
        check.positive(cfg.getWaitTime(), "wait time");
        check.that(cfg.getInterval() < cfg.getWaitTime(), "discovery interval must be greater than wait time "
            + "[discovery-interval=" + cfg.getInterval() + ", wait-time=" + cfg.getWaitTime() + ']');

        InetAddress groupAddress = InetAddress.getByName(cfg.getGroup());

        check.isTrue(groupAddress.isMulticastAddress(), "address is not a multicast address [address=" + groupAddress + ']');

        group = new InetSocketAddress(groupAddress, cfg.getPort());
        ttl = cfg.getTtl();
        interval = cfg.getInterval();
        waitTime = cfg.getWaitTime();
        loopBackDisabled = cfg.isLoopBackDisabled();

        ipVer = group.getAddress() instanceof Inet6Address ? InternetProtocolFamily.IPv6 : InternetProtocolFamily.IPv4;
    }

    @Override
    public List<InetSocketAddress> findSeedNodes(String cluster) throws HekateException {
        synchronized (mux) {
            if (isRegistered()) {
                return seedNodes.stream().filter(s -> s.cluster().equals(cluster)).map(SeedNode::address).collect(toList());
            }
        }

        return Collections.emptyList();
    }

    @Override
    public void startDiscovery(String cluster, InetSocketAddress address) throws HekateException {
        log.info("Starting seed nodes discovery [cluster={}, {}]", cluster, ToString.formatProperties(this));

        SeedNode thisNode = new SeedNode(address, cluster);

        try {
            NetworkInterface nif = selectMulticastInterface(address);

            try {
                synchronized (mux) {
                    if (isRegistered()) {
                        throw new IllegalStateException("Multicast seed node provider is already registered with another address "
                            + "[existing=" + localNode + ']');
                    }

                    ByteBuf discoveryMsg = prepareDiscovery(thisNode);

                    ByteBuf seedNodeInfoBytes = prepareSeedNodeInfo(thisNode);

                    localNode = thisNode;

                    seedNodes = new HashSet<>();

                    eventLoop = new NioEventLoopGroup(1, new HekateThreadFactory("SeedNodeMulticast"));

                    // Prepare common bootstrap options.
                    Bootstrap bootstrap = new Bootstrap();

                    bootstrap.option(ChannelOption.SO_REUSEADDR, true);
                    bootstrap.option(ChannelOption.IP_MULTICAST_TTL, ttl);
                    bootstrap.option(ChannelOption.IP_MULTICAST_IF, nif);

                    if (loopBackDisabled) {
                        bootstrap.option(ChannelOption.IP_MULTICAST_LOOP_DISABLED, true);

                        if (DEBUG) {
                            log.debug("Setting {} option to true", ChannelOption.IP_MULTICAST_LOOP_DISABLED);
                        }
                    }

                    bootstrap.group(eventLoop);
                    bootstrap.channelFactory(() -> new NioDatagramChannel(ipVer));

                    // Create a sender channel (not joined to a multicast group).
                    bootstrap.localAddress(0);
                    bootstrap.handler(createSenderHandler(thisNode));

                    ChannelFuture senderBind = bootstrap.bind();

                    DatagramChannel localSender = (DatagramChannel)senderBind.channel();

                    sender = localSender;

                    senderBind.get();

                    // Create a listener channel and join to a multicast group.
                    bootstrap.localAddress(group.getPort());

                    bootstrap.handler(createListenerHandler(thisNode, seedNodeInfoBytes));

                    ChannelFuture listenerBind = bootstrap.bind();

                    listener = (DatagramChannel)listenerBind.channel();

                    listenerBind.get();

                    log.info("Joining to a multicast group "
                        + "[address={}, port={}, interface={}, ttl={}]", AddressUtils.host(group), group.getPort(), nif.getName(), ttl);

                    listener.joinGroup(group, nif).get();

                    // Create a periodic task for discovery messages sending.
                    discoveryFuture = eventLoop.scheduleWithFixedDelay(() -> {
                        if (DEBUG) {
                            log.debug("Sending discovery message [from={}]", thisNode);
                        }

                        DatagramPacket discovery = new DatagramPacket(discoveryMsg.copy(), group);

                        localSender.writeAndFlush(discovery);
                    }, 0, interval, TimeUnit.MILLISECONDS);
                }
            } catch (ExecutionException e) {
                cleanup();

                throw new HekateException("Failed to start a multicast seed nodes discovery [node=" + thisNode + ']', e.getCause());
            }

            log.info("Will wait for seed nodes [timeout={}(ms)]", waitTime);

            Thread.sleep(waitTime);
        } catch (InterruptedException e) {
            cleanup();

            Thread.currentThread().interrupt();

            throw new HekateException("Thread was interrupted while awaiting for multicast discovery [node=" + thisNode + ']', e);
        }

        log.info("Done waiting for seed nodes.");
    }

    @Override
    public void suspendDiscovery() {
        suspendDiscoveryAsync().awaitUninterruptedly();
    }

    @Override
    public void stopDiscovery(String cluster, InetSocketAddress address) throws HekateException {
        log.info("Stopping seed nodes discovery [cluster={}, address={}]", cluster, address);

        cleanup();
    }

    @Override
    public long cleanupInterval() {
        return 0;
    }

    @Override
    public void registerRemote(String cluster, InetSocketAddress node) throws HekateException {
        // No-op.
    }

    @Override
    public void unregisterRemote(String cluster, InetSocketAddress node) throws HekateException {
        // No-op.
    }

    /**
     * Returns the multicast group (see {@link MulticastSeedNodeProviderConfig#setGroup(String)}).
     *
     * @return Multicast group.
     */
    public InetSocketAddress group() {
        return group;
    }

    /**
     * Returns the multicast TTL (see {@link MulticastSeedNodeProviderConfig#setTtl(int)}).
     *
     * @return Multicast group.
     */
    public int ttl() {
        return ttl;
    }

    /**
     * Returns the multicast interval (see {@link MulticastSeedNodeProviderConfig#setInterval(long)}).
     *
     * @return Multicast interval.
     */
    public long interval() {
        return interval;
    }

    /**
     * Returns the time to await for responses from remote nodes (see {@link MulticastSeedNodeProviderConfig#setWaitTime(long)}).
     *
     * @return Time to await for responses from remote nodes.
     */
    public long waitTime() {
        return waitTime;
    }

    @Override
    public MulticastSeedNodeProviderJmx jmx() {
        return new MulticastSeedNodeProviderJmx() {
            @Override
            public String getGroup() {
                return group().toString();
            }

            @Override
            public int getTtl() {
                return ttl();
            }

            @Override
            public long getInterval() {
                return interval();
            }

            @Override
            public long getWaitTime() {
                return waitTime();
            }
        };
    }

    private void cleanup() {
        Waiting suspended;
        Waiting closed = Waiting.NO_WAIT;
        Waiting stopped;

        synchronized (mux) {
            suspended = suspendDiscoveryAsync();

            if (listener != null && listener.isOpen()) {
                if (DEBUG) {
                    log.debug("Closing multicast listener channel [channel={}]", listener);
                }

                closed = listener.close()::await;
            }

            stopped = NettyUtils.shutdown(eventLoop);

            localNode = null;
            seedNodes = null;
            discoveryFuture = null;
            listener = null;
            eventLoop = null;
        }

        // Await for termination out of synchronized context in order to prevent deadlocks.
        suspended.awaitUninterruptedly();
        closed.awaitUninterruptedly();
        stopped.awaitUninterruptedly();
    }

    private Waiting suspendDiscoveryAsync() {
        Waiting waiting = Waiting.NO_WAIT;

        synchronized (mux) {
            if (discoveryFuture != null) {
                if (DEBUG) {
                    log.debug("Canceling discovery task.");
                }

                discoveryFuture.cancel(false);

                discoveryFuture = null;
            }

            if (sender != null) {
                if (DEBUG) {
                    log.debug("Closing multicast sender channel [channel={}]", sender);
                }

                waiting = sender.close()::await;

                sender = null;
            }
        }

        return waiting;
    }

    private SimpleChannelInboundHandler<DatagramPacket> createSenderHandler(SeedNode thisNode) {
        return new SimpleChannelInboundHandler<DatagramPacket>() {
            @Override
            public void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception {
                ByteBuf buf = msg.content();

                if (buf.readableBytes() > 4 && buf.readInt() == Utils.MAGIC_BYTES) {
                    MessageTYpe msgType = MessageTYpe.values()[buf.readByte()];

                    if (msgType == MessageTYpe.SEED_NODE_INFO) {
                        String cluster = decodeUtf(buf);

                        InetSocketAddress address = decodeAddress(buf);

                        SeedNode otherNode = new SeedNode(address, cluster);

                        if (!thisNode.equals(otherNode)) {
                            if (DEBUG) {
                                log.debug("Received seed node info message [node={}]", otherNode);
                            }

                            boolean discovered = false;

                            synchronized (mux) {
                                if (isRegistered()) {
                                    if (!seedNodes.contains(otherNode)) {
                                        discovered = true;

                                        seedNodes.add(otherNode);
                                    }
                                }
                            }

                            if (discovered) {
                                log.info("Seed node discovered [address={}]", otherNode.address());
                            }
                        }
                    }
                }
            }
        };
    }

    private SimpleChannelInboundHandler<DatagramPacket> createListenerHandler(SeedNode thisNode, ByteBuf seedNodeInfo) {
        return new SimpleChannelInboundHandler<DatagramPacket>() {
            @Override
            public void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception {
                ByteBuf buf = msg.content();

                if (buf.readableBytes() > 4 && buf.readInt() == Utils.MAGIC_BYTES) {
                    MessageTYpe msgType = MessageTYpe.values()[buf.readByte()];

                    if (msgType == MessageTYpe.DISCOVERY) {
                        String cluster = decodeUtf(buf);
                        InetSocketAddress address = decodeAddress(buf);

                        if (thisNode.cluster().equals(cluster) && !address.equals(thisNode.address())) {
                            onDiscoveryMessage(address);

                            DatagramPacket response = new DatagramPacket(seedNodeInfo.copy(), msg.sender());

                            ctx.writeAndFlush(response);
                        }
                    }
                }
            }
        };
    }

    // Package level for testing purposes.
    void onDiscoveryMessage(InetSocketAddress address) {
        if (DEBUG) {
            log.debug("Received discovery message [from={}]", address);
        }
    }

    private boolean isRegistered() {
        assert Thread.holdsLock(mux) : "Thread must hold lock on mutex.";

        return localNode != null;
    }

    private ByteBuf prepareSeedNodeInfo(SeedNode node) {
        ByteBuf out = Unpooled.buffer();

        out.writeInt(Utils.MAGIC_BYTES);
        out.writeByte(MessageTYpe.SEED_NODE_INFO.ordinal());

        encodeUtf(node.cluster(), out);
        encodeAddress(node.address(), out);

        return out;
    }

    private ByteBuf prepareDiscovery(SeedNode node) {
        ByteBuf out = Unpooled.buffer();

        out.writeInt(Utils.MAGIC_BYTES);
        out.writeByte(MessageTYpe.DISCOVERY.ordinal());

        encodeUtf(node.cluster(), out);
        encodeAddress(node.address(), out);

        return out;
    }

    private void encodeUtf(String str, ByteBuf out) {
        byte[] clusterIdBytes = str.getBytes(StandardCharsets.UTF_8);

        out.writeInt(clusterIdBytes.length);
        out.writeBytes(clusterIdBytes);
    }

    private String decodeUtf(ByteBuf buf) {
        int len = buf.readInt();

        byte[] strBytes = new byte[len];

        buf.readBytes(strBytes);

        return new String(strBytes, StandardCharsets.UTF_8);
    }

    private void encodeAddress(InetSocketAddress socketAddress, ByteBuf out) {
        byte[] addrBytes = socketAddress.getAddress().getAddress();

        out.writeByte(addrBytes.length);
        out.writeBytes(addrBytes);
        out.writeInt(socketAddress.getPort());
    }

    private InetSocketAddress decodeAddress(ByteBuf buf) throws UnknownHostException {
        byte[] addrBytes = new byte[buf.readByte()];

        buf.readBytes(addrBytes);

        int port = buf.readInt();

        return new InetSocketAddress(InetAddress.getByAddress(addrBytes), port);
    }

    // Package level for testing purposes.
    NetworkInterface selectMulticastInterface(InetSocketAddress node) throws HekateException {
        InetAddress address = node.getAddress();

        try {
            if (DEBUG) {
                log.debug("Resolving a network interface [address={}]", address);
            }

            NetworkInterface nif = NetworkInterface.getByInetAddress(address);

            if (nif == null && address.isLoopbackAddress()) {
                if (DEBUG) {
                    log.debug("Failed to resolve a network interface for a loopback address. Will try to find a loopback interface.");
                }

                nif = findLoopbackInterface(address);
            }

            if (nif == null) {
                throw new HekateException("Failed to resolve a network interface by address [address=" + address + ']');
            }

            if (!nif.supportsMulticast()) {
                throw new HekateException("Network interface doesn't support multicasting [name=" + nif.getName()
                    + ", interface-address=" + address + ']');
            }

            return nif;
        } catch (IOException e) {
            throw new HekateException("Failed to resolve multicast network interface [interface-address=" + address + ']', e);
        }
    }

    private NetworkInterface findLoopbackInterface(InetAddress address) throws SocketException, HekateException {
        NetworkInterface lo = null;

        for (NetworkInterface nif : AddressUtils.activeNetworks()) {
            if (nif.isUp() && nif.isLoopback()) {
                for (InetAddress nifAddress : Collections.list(nif.getInetAddresses())) {
                    if (!nifAddress.isLinkLocalAddress() && nifAddress.isLoopbackAddress()) {
                        if (lo != null) {
                            throw new HekateException("Failed to resolve a loopback network interface. "
                                + "Multiple loopback interfaces were detected [address=" + address + ", interface1=" + lo.getName()
                                + ", interface2=" + nif.getName() + ']');
                        }

                        lo = nif;

                        break;
                    }
                }
            }
        }

        return lo;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
