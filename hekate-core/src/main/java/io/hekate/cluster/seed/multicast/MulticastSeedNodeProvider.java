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

package io.hekate.cluster.seed.multicast;

import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.AddressUtils;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.core.internal.util.Utils;
import io.hekate.core.jmx.JmxSupport;
import io.hekate.core.report.ConfigReportSupport;
import io.hekate.core.report.ConfigReporter;
import io.hekate.network.netty.NettyUtils;
import io.hekate.util.async.Waiting;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.channel.ChannelOption.IP_MULTICAST_IF;
import static io.netty.channel.ChannelOption.IP_MULTICAST_LOOP_DISABLED;
import static io.netty.channel.ChannelOption.IP_MULTICAST_TTL;
import static io.netty.channel.ChannelOption.SO_REUSEADDR;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.list;
import static java.util.stream.Collectors.toList;

/**
 * Multicast-based seed node provider.
 *
 * <p>
 * This provider uses <a href="https://en.wikipedia.org/wiki/IP_multicast" target="_blank">IP multicast</a> to discover seed nodes.
 * When this provider starts it periodically sends multicast messages and awaits for responses from other nodes (up to a configurable
 * timeout).
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
public class MulticastSeedNodeProvider implements SeedNodeProvider, JmxSupport<MulticastSeedNodeProviderJmx>, ConfigReportSupport {
    private enum MessageTYpe {
        DISCOVERY,

        SEED_NODE_INFO
    }

    private static class SeedNode {
        private final String cluster;

        private final InetSocketAddress address;

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

            SeedNode other = (SeedNode)o;

            return Objects.equals(address, other.address)
                && Objects.equals(cluster, other.cluster);
        }

        @Override
        public int hashCode() {
            return Objects.hash(address, cluster);
        }

        @Override
        public String toString() {
            return ToString.format(this);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(MulticastSeedNodeProvider.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final InetSocketAddress group;

    private final InternetProtocolFamily ipVer;

    private final int ttl;

    private final long interval;

    private final long waitTime;

    private final boolean loopBackDisabled;

    @ToStringIgnore
    private final Object mux = new Object();

    @ToStringIgnore
    private SeedNode localNode;

    @ToStringIgnore
    private Set<SeedNode> discovered;

    @ToStringIgnore
    private NioEventLoopGroup eventLoop;

    @ToStringIgnore
    private DatagramChannel receiver;

    @ToStringIgnore
    private DatagramChannel sender;

    @ToStringIgnore
    private ScheduledFuture<?> discoveryFuture;

    /**
     * Constructs a new instance with all configuration options set to their default values (see {@link MulticastSeedNodeProviderConfig}).
     *
     * @throws UnknownHostException If failed to resolve multicast group address.
     */
    public MulticastSeedNodeProvider() throws UnknownHostException {
        this(new MulticastSeedNodeProviderConfig());
    }

    /**
     * Constructs a new instance.
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

        InetAddress address = InetAddress.getByName(cfg.getGroup());

        check.isTrue(address.isMulticastAddress(), "address is not a multicast address [address=" + address + ']');

        group = new InetSocketAddress(address, cfg.getPort());

        if (group.getAddress() instanceof Inet6Address) {
            ipVer = InternetProtocolFamily.IPv6;
        } else {
            ipVer = InternetProtocolFamily.IPv4;
        }

        ttl = cfg.getTtl();
        interval = cfg.getInterval();
        waitTime = cfg.getWaitTime();
        loopBackDisabled = cfg.isLoopBackDisabled();
    }

    @Override
    public void report(ConfigReporter report) {
        report.section("multicast", r -> {
            r.value("group", group);
            r.value("ttl", ttl);
            r.value("interval", interval);
            r.value("wait-time", waitTime);
            r.value("loopback-disabled", loopBackDisabled);
        });
    }

    @Override
    public List<InetSocketAddress> findSeedNodes(String namespace) throws HekateException {
        synchronized (mux) {
            if (isRegistered()) {
                return discovered.stream()
                    .filter(node -> node.cluster().equals(namespace))
                    .map(SeedNode::address)
                    .collect(toList());
            }
        }

        return emptyList();
    }

    @Override
    public void startDiscovery(String namespace, InetSocketAddress address) throws HekateException {
        log.info("Starting seed node discovery [namespace={}, {}]", namespace, ToString.formatProperties(this));

        SeedNode self = new SeedNode(address, namespace);

        try {
            NetworkInterface nif = selectMulticastInterface(address);

            try {
                synchronized (mux) {
                    if (isRegistered()) {
                        String err = format("Multicast seed node provider is already registered with another address [existing=%s]", self);

                        throw new IllegalStateException(err);
                    }

                    localNode = self;
                    discovered = new HashSet<>();
                    eventLoop = new NioEventLoopGroup(1, new HekateThreadFactory("SeedNodeMulticast"));

                    // Prepare common bootstrap options.
                    Bootstrap boot = new Bootstrap();

                    // Threads and factories.
                    boot.group(eventLoop);
                    boot.channelFactory(() ->
                        new NioDatagramChannel(ipVer)
                    );

                    // TCP options.
                    boot.option(SO_REUSEADDR, true);
                    boot.option(IP_MULTICAST_TTL, ttl);
                    boot.option(IP_MULTICAST_IF, nif);

                    if (loopBackDisabled) {
                        boot.option(IP_MULTICAST_LOOP_DISABLED, true);

                        if (DEBUG) {
                            log.debug("Setting {} option to true", IP_MULTICAST_LOOP_DISABLED);
                        }
                    }

                    // Create a sender channel (not joined to a multicast group).
                    boot.localAddress(0);
                    boot.handler(createSenderHandler(self));

                    DatagramChannel sender = bindSender(boot);

                    // Create a receiver channel and join to a multicast group.
                    boot.localAddress(group.getPort());
                    boot.handler(createReceiveHandler(self));

                    bindReceiver(boot);

                    // Join multicast group.
                    log.info(
                        "Joining multicast group [address={}, port={}, interface={}, ttl={}]",
                        AddressUtils.host(group),
                        group.getPort(),
                        nif.getName(),
                        ttl
                    );

                    receiver.joinGroup(group, nif).get();

                    // Schedule a periodic task to send discovery messages.
                    discoveryFuture = eventLoop.scheduleWithFixedDelay(() -> {
                        if (DEBUG) {
                            log.debug("Sending discovery message [from={}]", self);
                        }

                        sender.writeAndFlush(discoveryRequest(self));
                    }, 0, interval, TimeUnit.MILLISECONDS);
                }
            } catch (ExecutionException e) {
                cleanup();

                throw new HekateException("Failed to start a multicast seed nodes discovery [node=" + self + ']', e.getCause());
            }

            log.info("Will wait for seed nodes [timeout={}(ms)]", waitTime);

            Thread.sleep(waitTime);
        } catch (InterruptedException e) {
            cleanup();

            Thread.currentThread().interrupt();

            throw new HekateException("Thread was interrupted while awaiting for multicast discovery [node=" + self + ']', e);
        }

        log.info("Done waiting for seed nodes.");
    }

    @Override
    public void suspendDiscovery() {
        suspendDiscoveryAsync().awaitUninterruptedly();
    }

    @Override
    public void stopDiscovery(String namespace, InetSocketAddress address) throws HekateException {
        log.info("Stopping seed nodes discovery [namespace={}, address={}]", namespace, address);

        cleanup();
    }

    @Override
    public long cleanupInterval() {
        return 0;
    }

    @Override
    public void registerRemote(String namespace, InetSocketAddress node) throws HekateException {
        // No-op.
    }

    @Override
    public void unregisterRemote(String namespace, InetSocketAddress node) throws HekateException {
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

    /**
     * Returns {@code true} if receiving of multicast messages on the loopback address is disabled
     * (see {@link MulticastSeedNodeProviderConfig#setLoopBackDisabled(boolean)}).
     *
     * @return {@code true} if receiving of multicast messages on the loopback address is disabled.
     */
    public boolean isLoopBackDisabled() {
        return loopBackDisabled;
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

            if (receiver != null && receiver.isOpen()) {
                if (DEBUG) {
                    log.debug("Closing multicast listener channel [channel={}]", receiver);
                }

                closed = receiver.close()::await;
            }

            stopped = NettyUtils.shutdown(eventLoop);

            localNode = null;
            discovered = null;
            discoveryFuture = null;
            receiver = null;
            eventLoop = null;
        }

        // Await for termination outside of synchronized context in order to prevent deadlocks.
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

    private DatagramChannel bindSender(Bootstrap boot) throws InterruptedException, ExecutionException {
        ChannelFuture future = boot.bind();

        sender = (DatagramChannel)future.channel();

        future.get();

        return sender;
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
                                    if (!MulticastSeedNodeProvider.this.discovered.contains(otherNode)) {
                                        discovered = true;

                                        MulticastSeedNodeProvider.this.discovered.add(otherNode);
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

    private void bindReceiver(Bootstrap boot) throws InterruptedException, ExecutionException {
        ChannelFuture future = boot.bind();

        receiver = (DatagramChannel)future.channel();

        future.get();
    }

    private SimpleChannelInboundHandler<DatagramPacket> createReceiveHandler(SeedNode self) {
        return new SimpleChannelInboundHandler<DatagramPacket>() {
            @Override
            public void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception {
                ByteBuf buf = msg.content();

                if (buf.readableBytes() > 4 && buf.readInt() == Utils.MAGIC_BYTES) {
                    MessageTYpe msgType = MessageTYpe.values()[buf.readByte()];

                    if (msgType == MessageTYpe.DISCOVERY) {
                        String cluster = decodeUtf(buf);
                        InetSocketAddress address = decodeAddress(buf);

                        if (self.cluster().equals(cluster) && !address.equals(self.address())) {
                            onDiscoveryRequest(address);

                            ctx.writeAndFlush(discoveryResponse(msg.sender(), self));
                        }
                    }
                }
            }
        };
    }

    // Package level for testing purposes.
    void onDiscoveryRequest(InetSocketAddress address) {
        if (DEBUG) {
            log.debug("Received discovery message [from={}]", address);
        }
    }

    private boolean isRegistered() {
        return localNode != null;
    }

    private DatagramPacket discoveryRequest(SeedNode node) {
        ByteBuf buf = Unpooled.buffer();

        buf.writeInt(Utils.MAGIC_BYTES);
        buf.writeByte(MessageTYpe.DISCOVERY.ordinal());

        encodeUtf(node.cluster(), buf);
        encodeAddress(node.address(), buf);

        return new DatagramPacket(buf, group);
    }

    private DatagramPacket discoveryResponse(InetSocketAddress to, SeedNode node) {
        ByteBuf buf = Unpooled.buffer();

        buf.writeInt(Utils.MAGIC_BYTES);
        buf.writeByte(MessageTYpe.SEED_NODE_INFO.ordinal());

        encodeUtf(node.cluster(), buf);
        encodeAddress(node.address(), buf);

        return new DatagramPacket(buf, to);
    }

    private void encodeUtf(String str, ByteBuf buf) {
        byte[] clusterIdBytes = str.getBytes(StandardCharsets.UTF_8);

        buf.writeInt(clusterIdBytes.length);
        buf.writeBytes(clusterIdBytes);
    }

    private String decodeUtf(ByteBuf buf) {
        int len = buf.readInt();

        byte[] strBytes = new byte[len];

        buf.readBytes(strBytes);

        return new String(strBytes, StandardCharsets.UTF_8);
    }

    private void encodeAddress(InetSocketAddress addr, ByteBuf out) {
        byte[] addrBytes = addr.getAddress().getAddress();

        out.writeByte(addrBytes.length);
        out.writeBytes(addrBytes);
        out.writeInt(addr.getPort());
    }

    private InetSocketAddress decodeAddress(ByteBuf buf) throws UnknownHostException {
        byte[] addrBytes = new byte[buf.readByte()];

        buf.readBytes(addrBytes);

        return new InetSocketAddress(
            InetAddress.getByAddress(addrBytes),
            buf.readInt()
        );
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
                for (InetAddress nifAddress : list(nif.getInetAddresses())) {
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
