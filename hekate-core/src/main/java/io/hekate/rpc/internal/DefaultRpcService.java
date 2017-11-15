package io.hekate.rpc.internal;

import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecService;
import io.hekate.core.HekateException;
import io.hekate.core.ServiceInfo;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.Utils;
import io.hekate.core.service.ConfigurableService;
import io.hekate.core.service.ConfigurationContext;
import io.hekate.core.service.DependencyContext;
import io.hekate.core.service.DependentService;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.InitializingService;
import io.hekate.core.service.TerminatingService;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessageInterceptor;
import io.hekate.messaging.MessagingBackPressureConfig;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingConfigProvider;
import io.hekate.messaging.MessagingService;
import io.hekate.rpc.Rpc;
import io.hekate.rpc.RpcClientBuilder;
import io.hekate.rpc.RpcClientConfig;
import io.hekate.rpc.RpcClientConfigProvider;
import io.hekate.rpc.RpcInterfaceInfo;
import io.hekate.rpc.RpcServerConfig;
import io.hekate.rpc.RpcServerConfigProvider;
import io.hekate.rpc.RpcServerInfo;
import io.hekate.rpc.RpcService;
import io.hekate.rpc.RpcServiceFactory;
import io.hekate.rpc.internal.RpcProtocol.CallRequest;
import io.hekate.rpc.internal.RpcProtocol.CompactCallRequest;
import io.hekate.util.StateGuard;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.core.internal.util.StreamUtils.nullSafe;
import static io.hekate.rpc.internal.RpcUtils.methodProperty;
import static io.hekate.rpc.internal.RpcUtils.nameProperty;
import static io.hekate.rpc.internal.RpcUtils.taggedMethodProperty;
import static io.hekate.rpc.internal.RpcUtils.taggedNameProperty;
import static java.util.Collections.singleton;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

public class DefaultRpcService implements RpcService, ConfigurableService, DependentService, InitializingService, TerminatingService,
    MessagingConfigProvider {
    private static class RpcKey {
        private final Class<?> type;

        private final String tag;

        public RpcKey(Class<?> type, String tag) {
            this.type = type;
            this.tag = Utils.nullOrTrim(tag);
        }

        public Class<?> type() {
            return type;
        }

        public String tag() {
            return tag;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (!(o instanceof RpcKey)) {
                return false;
            }

            RpcKey rpcKey = (RpcKey)o;

            if (!type.equals(rpcKey.type)) {
                return false;
            }

            return tag != null ? tag.equals(rpcKey.tag) : rpcKey.tag == null;
        }

        @Override
        public int hashCode() {
            int result = type.hashCode();

            result = 31 * result + (tag != null ? tag.hashCode() : 0);

            return result;
        }

        @Override
        public String toString() {
            return ToString.format(this);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DefaultRpcService.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final String CHANNEL_NAME = "hekate.rpc";

    private final int workerThreads;

    private final int nioThreads;

    private final long idleSocketTimeout;

    private final MessagingBackPressureConfig backPressure;

    @ToStringIgnore
    private final List<RpcServerConfig> serverConfigs = new LinkedList<>();

    @ToStringIgnore
    private final StateGuard guard = new StateGuard(RpcService.class);

    @ToStringIgnore
    private final Map<RpcKey, RpcClientBuilder<?>> clients = new ConcurrentHashMap<>();

    @ToStringIgnore
    private final List<RpcClientConfig> clientConfigs = new LinkedList<>();

    @ToStringIgnore
    private final List<RpcMethodHandler> rpcMethodIndex = new ArrayList<>();

    private List<RpcServerInfo> servers;

    @ToStringIgnore
    private MessagingService messaging;

    @ToStringIgnore
    private CodecFactory<Object> codec;

    @ToStringIgnore
    private MessagingChannel<RpcProtocol> channel;

    public DefaultRpcService(RpcServiceFactory factory) {
        assert factory != null : "Factory is null.";

        workerThreads = factory.getWorkerThreads();
        nioThreads = factory.getNioThreads();
        idleSocketTimeout = factory.getIdleSocketTimeout();
        backPressure = new MessagingBackPressureConfig(factory.getBackPressure());

        nullSafe(factory.getClients()).forEach(clientConfigs::add);
        nullSafe(factory.getServers()).forEach(serverConfigs::add);

        nullSafe(factory.getClientProviders()).forEach(provider ->
            nullSafe(provider.getRpcClientConfig()).forEach(clientConfigs::add)
        );

        nullSafe(factory.getServerProviders()).forEach(provider ->
            nullSafe(provider.getRpcServerConfig()).forEach(serverConfigs::add)
        );
    }

    @Override
    public void resolve(DependencyContext ctx) {
        messaging = ctx.require(MessagingService.class);
        codec = ctx.require(CodecService.class).codecFactory();
    }

    @Override
    public void configure(ConfigurationContext ctx) {
        // Collect client configurations from providers.
        nullSafe(ctx.findComponents(RpcClientConfigProvider.class)).forEach(provider ->
            nullSafe(provider.getRpcClientConfig()).forEach(clientConfigs::add)
        );

        // Validate client configurations.
        clientConfigs.forEach(cfg ->
            ConfigCheck.get(RpcClientConfig.class).notNull(cfg.getRpcInterface(), "RPC interface")
        );

        // Collect server configurations from providers.
        nullSafe(ctx.findComponents(RpcServerConfigProvider.class)).forEach(provider ->
            nullSafe(provider.getRpcServerConfig()).forEach(serverConfigs::add)
        );

        // Register RPC servers.
        List<RpcServerInfo> serversInfo = new ArrayList<>();

        ConfigCheck check = ConfigCheck.get(RpcServerConfig.class);

        Set<RpcKey> uniqueRpcTypes = new HashSet<>();

        serverConfigs.forEach(cfg -> {
            check.notNull(cfg.getHandler(), "Handler");

            List<RpcInterface<?>> rpcs = RpcTypeAnalyzer.analyze(cfg.getHandler());

            if (rpcs.isEmpty()) {
                throw check.fail("RPC handler must implement at least one @" + Rpc.class.getSimpleName() + "-annotated public "
                    + "interface [handler=" + cfg.getHandler() + ']');
            }

            // Gather tags.
            List<String> tags = nullSafe(cfg.getTags())
                .map(String::trim)
                .filter(tag -> !tag.isEmpty())
                .collect(toList());

            List<RpcInterfaceInfo<?>> rpcTypes = rpcs.stream().map(RpcInterface::type).collect(toList());

            serversInfo.add(new RpcServerInfo(cfg.getHandler(), rpcTypes, tags));

            rpcs.forEach(rpc -> {
                RpcInterfaceInfo type = rpc.type();

                List<Map.Entry<RpcMethodHandler, Integer>> methodIdxs = new ArrayList<>();

                // Index methods.
                rpc.methods().forEach(method -> {
                    int idx = rpcMethodIndex.size();

                    // No synchronization since this method gets called only once when the node is instantiated
                    // and before any other thread can access this list.
                    rpcMethodIndex.add(method);

                    methodIdxs.add(new SimpleEntry<>(method, idx));
                });

                // Register RPC servers so that other nodes would be able to discover which RPCs are provided by this node.
                if (tags.isEmpty()) {
                    // Register RPC without tags.
                    RpcKey key = new RpcKey(rpc.type().javaType(), null);

                    if (!uniqueRpcTypes.add(key)) {
                        throw check.fail("Can't register the same RPC interface multiple times [key=" + key + ']');
                    }

                    ctx.setIntProperty(nameProperty(type), rpc.type().minClientVersion());

                    // Register method indexes.
                    methodIdxs.forEach(e ->
                        ctx.setIntProperty(methodProperty(type, e.getKey().info()), e.getValue())
                    );
                } else {
                    // Register RPC for each tag.
                    tags.forEach(tag -> {
                        RpcKey tagKey = new RpcKey(rpc.type().javaType(), tag);

                        if (!uniqueRpcTypes.add(tagKey)) {
                            throw check.fail("Can't register the same RPC interface multiple times [key=" + tagKey + ']');
                        }

                        ctx.setIntProperty(taggedNameProperty(type, tag), rpc.type().minClientVersion());

                        // Register method indexes.
                        methodIdxs.forEach(e ->
                            ctx.setIntProperty(taggedMethodProperty(type, e.getKey().info(), tag), e.getValue())
                        );
                    });
                }
            });
        });

        this.servers = unmodifiableList(serversInfo);
    }

    @Override
    public Collection<MessagingChannelConfig<?>> configureMessaging() {
        MessagingChannelConfig<RpcProtocol> cfg = MessagingChannelConfig.of(RpcProtocol.class)
            .withName(CHANNEL_NAME)
            .withNioThreads(nioThreads)
            .withWorkerThreads(workerThreads)
            .withIdleSocketTimeout(idleSocketTimeout)
            .withBackPressure(backPressure)
            .withLogCategory(RpcService.class.getName())
            .withMessageCodec(new RpcProtocolCodecFactory(codec))
            .withInterceptor(new MessageInterceptor<RpcProtocol>() {
                @Override
                public RpcProtocol interceptOutbound(RpcProtocol msg, OutboundContext ctx) {
                    // Convert method calls to compact representations.
                    if (msg.type() == RpcProtocol.Type.CALL_REQUEST) {
                        CallRequest req = (CallRequest)msg;

                        // Use the method's index instead of the method signature.
                        String methodIdxProp;

                        if (req.rpcTag() == null) {
                            methodIdxProp = RpcUtils.methodProperty(req.rpcType(), req.rpcMethod());
                        } else {
                            methodIdxProp = RpcUtils.taggedMethodProperty(req.rpcType(), req.rpcMethod(), req.rpcTag());
                        }

                        int methodIdx = ctx.receiver().service(RpcService.class).intProperty(methodIdxProp);

                        return new CompactCallRequest(methodIdx, req.args());
                    }

                    return msg;
                }
            });

        if (!rpcMethodIndex.isEmpty()) {
            cfg.withReceiver(this::handleMessage);
        }

        return singleton(cfg);
    }

    @Override
    public void initialize(InitializationContext ctx) throws HekateException {
        guard.lockWrite();

        try {
            guard.becomeInitialized();

            if (DEBUG) {
                log.debug("Initializing...");
            }

            channel = messaging.channel(CHANNEL_NAME, RpcProtocol.class);

            clientConfigs.forEach(cfg -> {
                RpcKey key = new RpcKey(cfg.getRpcInterface(), cfg.getTag());

                RpcClientBuilder<?> client = createClient(key).withTimeout(cfg.getTimeout(), TimeUnit.MILLISECONDS);

                if (cfg.getFailover() != null) {
                    client = client.withFailover(cfg.getFailover());
                }

                if (cfg.getLoadBalancer() != null) {
                    client = client.withLoadBalancer(cfg.getLoadBalancer());
                }

                clients.put(key, client);
            });

            if (DEBUG) {
                log.debug("Initialized.");
            }
        } finally {
            guard.unlockWrite();
        }
    }

    @Override
    public void terminate() throws HekateException {
        guard.lockWrite();

        try {
            if (guard.becomeTerminated()) {
                clients.clear();

                channel = null;

                if (DEBUG) {
                    log.debug("Terminated.");
                }
            }
        } finally {
            guard.unlockWrite();
        }
    }

    @Override
    public <T> RpcClientBuilder<T> clientFor(Class<T> type) {
        return clientFor(type, null);
    }

    @Override
    public <T> RpcClientBuilder<T> clientFor(Class<T> type, String tag) {
        ArgAssert.notNull(type, "Type");

        RpcKey key = new RpcKey(type, tag);

        guard.lockReadWithStateCheck();

        try {
            @SuppressWarnings("unchecked")
            RpcClientBuilder<T> client = (RpcClientBuilder<T>)clients.computeIfAbsent(key, this::createClient);

            return client;
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public List<RpcServerInfo> servers() {
        return servers;
    }

    private void handleMessage(Message<RpcProtocol> msg) {
        RpcProtocol rpcMsg = msg.get();

        switch (rpcMsg.type()) {
            case COMPACT_CALL_REQUEST: {
                CompactCallRequest call = (CompactCallRequest)rpcMsg;

                RpcMethodHandler method = rpcMethodIndex.get(call.methodIdx());

                method.handle(msg);

                break;
            }
            case CALL_REQUEST: // <-- Should fail since it must be converted to a compact representation by the message interceptor.
            case OBJECT_RESPONSE:
            case NULL_RESPONSE:
            case ERROR_RESPONSE:
            default: {
                throw new IllegalArgumentException("Unexpected message type: " + rpcMsg);
            }
        }
    }

    private RpcClientBuilder<?> createClient(RpcKey key) {
        RpcInterfaceInfo<?> type = RpcTypeAnalyzer.analyzeType(key.type());

        MessagingChannel<RpcProtocol> clientChannel = channelForClient(key, type);

        DefaultRpcClientBuilder<?> client = new DefaultRpcClientBuilder<>(type, key.tag(), clientChannel);

        if (DEBUG) {
            log.debug("Created new RPC client builder [key={}, builder={}]", key, client);
        }

        return client;
    }

    private MessagingChannel<RpcProtocol> channelForClient(RpcKey key, RpcInterfaceInfo<?> type) {
        String rpcName;

        if (key.tag() == null) {
            rpcName = nameProperty(type);
        } else {
            rpcName = taggedNameProperty(type, key.tag());
        }

        return this.channel.filter(node -> {
            ServiceInfo service = node.service(RpcService.class);

            Integer minClientVer = service.intProperty(rpcName);

            return minClientVer != null && minClientVer <= type.version();
        });
    }

    @Override
    public String toString() {
        return ToString.format(RpcService.class, this);
    }
}
