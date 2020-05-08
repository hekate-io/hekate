/*
 * Copyright 2020 The Hekate Project
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

package io.hekate.rpc.internal;

import io.hekate.cluster.ClusterView;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecService;
import io.hekate.core.HekateException;
import io.hekate.core.inject.InjectionService;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.jmx.JmxService;
import io.hekate.core.report.ConfigReporter;
import io.hekate.core.service.ConfigurationContext;
import io.hekate.core.service.CoreService;
import io.hekate.core.service.DependencyContext;
import io.hekate.core.service.InitializationContext;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessagingBackPressureConfig;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingConfigProvider;
import io.hekate.messaging.MessagingService;
import io.hekate.messaging.intercept.ClientMessageInterceptor;
import io.hekate.messaging.intercept.ClientSendContext;
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
import io.hekate.rpc.internal.RpcProtocol.RpcCall;
import io.hekate.rpc.internal.RpcProtocol.RpcCompactCall;
import io.hekate.rpc.internal.RpcProtocol.RpcCompactSplitCall;
import io.hekate.util.StateGuard;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.core.internal.util.StreamUtils.nullSafe;
import static io.hekate.rpc.internal.RpcUtils.filterFor;
import static io.hekate.rpc.internal.RpcUtils.methodProperty;
import static io.hekate.rpc.internal.RpcUtils.taggedMethodProperty;
import static io.hekate.rpc.internal.RpcUtils.taggedVersionProperty;
import static io.hekate.rpc.internal.RpcUtils.versionProperty;
import static java.util.Collections.singleton;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

public class DefaultRpcService implements RpcService, CoreService, MessagingConfigProvider {
    private static final Logger log = LoggerFactory.getLogger(DefaultRpcService.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final String RPC_CHANNEL = "hekate.rpc";

    private final int workerThreads;

    private final int nioThreads;

    private final long idleSocketTimeout;

    private final MessagingBackPressureConfig backPressure;

    private final StateGuard guard = new StateGuard(RpcService.class);

    private final List<RpcClientConfig> clientConfigs = new ArrayList<>();

    private final List<RpcServerConfig> serverConfigs = new ArrayList<>();

    private final Map<RpcTypeKey, RpcClientBuilder<?>> clients = new ConcurrentHashMap<>();

    private List<RpcServerInfo> servers;

    private RpcMethodHandler[] methods;

    private RpcTypeAnalyzer types;

    private JmxService jmx;

    private CodecFactory<Object> codec;

    private MessagingService messaging;

    private MessagingChannel<RpcProtocol> channel;

    public DefaultRpcService(RpcServiceFactory factory) {
        ArgAssert.notNull(factory, "Factory");

        workerThreads = factory.getWorkerThreads();
        nioThreads = factory.getNioThreads();
        idleSocketTimeout = factory.getIdleSocketTimeout();
        backPressure = new MessagingBackPressureConfig(factory.getBackPressure());

        nullSafe(factory.getClients()).forEach(clientConfigs::add);
        nullSafe(factory.getServers()).forEach(serverConfigs::add);

        nullSafe(factory.getClientProviders()).forEach(provider ->
            nullSafe(provider.configureRpcClients()).forEach(clientConfigs::add)
        );

        nullSafe(factory.getServerProviders()).forEach(provider ->
            nullSafe(provider.configureRpcServers()).forEach(serverConfigs::add)
        );
    }

    @Override
    public void resolve(DependencyContext ctx) {
        messaging = ctx.require(MessagingService.class);
        codec = ctx.require(CodecService.class).codecFactory();

        jmx = ctx.optional(JmxService.class);

        InjectionService inject = ctx.optional(InjectionService.class);

        if (inject == null) {
            types = new RpcTypeAnalyzer(value -> value);
        } else {
            types = new RpcTypeAnalyzer(inject);
        }
    }

    @Override
    public void configure(ConfigurationContext ctx) {
        // Collect client configurations from providers.
        nullSafe(ctx.findComponents(RpcClientConfigProvider.class)).forEach(provider ->
            nullSafe(provider.configureRpcClients()).forEach(clientConfigs::add)
        );

        // Validate client configurations.
        clientConfigs.forEach(cfg -> {
            ConfigCheck check = ConfigCheck.get(RpcClientConfig.class);

            check.notNull(cfg.getRpcInterface(), "RPC interface");
            check.validSysName(cfg.getTag(), "tag");
        });

        // Collect server configurations from providers.
        nullSafe(ctx.findComponents(RpcServerConfigProvider.class)).forEach(provider ->
            nullSafe(provider.configureRpcServers()).forEach(serverConfigs::add)
        );

        // Register RPC servers.
        List<RpcServerInfo> serversInfo = new ArrayList<>();

        Set<RpcTypeKey> uniqueRpcTypes = new HashSet<>();

        List<RpcMethodHandler> allMethods = new ArrayList<>();

        serverConfigs.forEach(cfg -> {
            ConfigCheck check = ConfigCheck.get(RpcServerConfig.class);

            check.notNull(cfg.getHandler(), "Handler");

            List<RpcInterface<?>> rpcs = types.analyze(cfg.getHandler());

            if (rpcs.isEmpty()) {
                throw check.fail("RPC handler must implement at least one @" + Rpc.class.getSimpleName() + "-annotated public "
                    + "interface [handler=" + cfg.getHandler() + ']');
            }

            // Gather tags.
            List<String> tags = nullSafe(cfg.getTags())
                .map(String::trim)
                .filter(tag -> !tag.isEmpty())
                .collect(toList());

            List<RpcInterfaceInfo<?>> rpcTypes = rpcs.stream()
                .map(RpcInterface::type)
                .collect(toList());

            serversInfo.add(new RpcServerInfo(cfg.getHandler(), rpcTypes, tags));

            rpcs.forEach(rpc -> {
                RpcInterfaceInfo type = rpc.type();

                List<Map.Entry<RpcMethodHandler, Integer>> rpcMethods = new ArrayList<>();

                // Index methods.
                rpc.methods().forEach(method -> {
                    int idx = allMethods.size();

                    allMethods.add(method);

                    rpcMethods.add(new SimpleEntry<>(method, idx));
                });

                // Register RPC servers so that other nodes would be able to discover which RPCs are provided by this node.
                if (tags.isEmpty()) {
                    // Register RPC without tags.
                    RpcTypeKey key = new RpcTypeKey(rpc.type().javaType(), null);

                    if (!uniqueRpcTypes.add(key)) {
                        throw check.fail("Can't register the same RPC interface multiple times [key=" + key + ']');
                    }

                    ctx.setIntProperty(versionProperty(type), rpc.type().minClientVersion());

                    // Register method indexes.
                    rpcMethods.forEach(e ->
                        ctx.setIntProperty(methodProperty(type, e.getKey().method()), e.getValue())
                    );
                } else {
                    // Register RPC for each tag.
                    tags.forEach(tag -> {
                        // Verify tag format.
                        check.validSysName(tag, "tag");

                        RpcTypeKey tagKey = new RpcTypeKey(rpc.type().javaType(), tag);

                        if (!uniqueRpcTypes.add(tagKey)) {
                            throw check.fail("Can't register the same RPC interface multiple times [key=" + tagKey + ']');
                        }

                        ctx.setIntProperty(taggedVersionProperty(type, tag), rpc.type().minClientVersion());

                        // Register method indexes.
                        rpcMethods.forEach(e ->
                            ctx.setIntProperty(taggedMethodProperty(type, e.getKey().method(), tag), e.getValue())
                        );
                    });
                }
            });
        });

        if (!allMethods.isEmpty()) {
            methods = allMethods.toArray(RpcMethodHandler.EMPTY_ARRAY);
        }

        servers = unmodifiableList(serversInfo);
    }

    @Override
    public void report(ConfigReporter report) {
        guard.withReadLockIfInitialized(() -> {
            if (!servers.isEmpty()) {
                report.section("rpc", rpcSec ->
                    rpcSec.section("servers", serversSec ->
                        servers.forEach(server ->
                            serversSec.section("server", serverSec -> {
                                serverSec.value("tags", server.tags());
                                serverSec.value("implementation", server.rpc());

                                serverSec.section("interfaces", facesSec ->
                                    server.interfaces().forEach(face ->
                                        facesSec.section("interface", faceSec -> {
                                            faceSec.value("type", face.javaType().getName());
                                            faceSec.value("min-client-version", face.minClientVersion());
                                            faceSec.value("version", face.version());
                                        })
                                    )
                                );
                            })
                        )
                    )
                );
            }
        });
    }

    @Override
    public Collection<MessagingChannelConfig<?>> configureMessaging() {
        MessagingChannelConfig<RpcProtocol> cfg = MessagingChannelConfig.of(RpcProtocol.class)
            .withName(RPC_CHANNEL)
            .withNioThreads(nioThreads)
            .withWorkerThreads(workerThreads)
            .withIdleSocketTimeout(idleSocketTimeout)
            .withBackPressure(backPressure)
            .withLogCategory(RpcProtocol.class.getName())
            .withMessageCodec(new RpcProtocolCodecFactory(codec))
            .withInterceptor(new ClientMessageInterceptor<RpcProtocol>() {
                @Override
                public void interceptClientSend(ClientSendContext<RpcProtocol> ctx) {
                    // Convert method call to its compact representations.
                    if (ctx.payload() instanceof RpcCall) {
                        RpcCall<?> req = (RpcCall<?>)ctx.payload();

                        // Use the method's index instead of the method signature.
                        int methodIdx = ctx.receiver().service(RpcService.class).intProperty(req.methodIdxKey());

                        if (req.isSplit()) {
                            ctx.overrideMessage(new RpcCompactSplitCall(methodIdx, req.args()));
                        } else {
                            ctx.overrideMessage(new RpcCompactCall(methodIdx, req.args()));
                        }
                    }
                }
            });

        if (methods != null) {
            cfg.withReceiver(this::handleMessage);
        }

        return singleton(cfg);
    }

    @Override
    public void initialize(InitializationContext ctx) throws HekateException {
        if (DEBUG) {
            log.debug("Initializing...");
        }

        guard.becomeInitialized(() -> {
            // Initialize RPC messaging channel.
            channel = messaging.channel(RPC_CHANNEL, RpcProtocol.class);

            // Initialize clients.
            clientConfigs.forEach(cfg -> {
                RpcTypeKey key = new RpcTypeKey(cfg.getRpcInterface(), cfg.getTag());

                RpcClientBuilder<?> client = createClient(key, cfg)
                    .withTimeout(cfg.getTimeout(), TimeUnit.MILLISECONDS)
                    .withPartitions(cfg.getPartitions(), cfg.getBackupNodes());

                if (cfg.getLoadBalancer() != null) {
                    client = client.withLoadBalancer(cfg.getLoadBalancer());
                }

                clients.put(key, client);
            });

            // Register to JMX (optional).
            if (jmx != null) {
                for (RpcServerInfo server : servers) {
                    // Register one JMX bean per each 'RPC interface + tag' combination.
                    for (RpcInterfaceInfo<?> rpcFace : server.interfaces()) {
                        if (server.tags().isEmpty()) {
                            // Register single JMX bean if server doesn't have any tags.
                            String name = rpcFace.name();

                            ClusterView cluster = clusterOf(rpcFace.javaType());

                            jmx.register(new DefaultRpcServerJmx(rpcFace, null, server, cluster), name);
                        } else {
                            // Register one JMX bean per each tag.
                            for (String tag : server.tags()) {
                                String name = rpcFace.name() + '#' + tag;

                                ClusterView cluster = clusterOf(rpcFace.javaType(), tag);

                                jmx.register(new DefaultRpcServerJmx(rpcFace, tag, server, cluster), name);
                            }
                        }
                    }
                }
            }
        });

        if (DEBUG) {
            log.debug("Initialized.");
        }
    }

    @Override
    public void terminate() throws HekateException {
        if (DEBUG) {
            log.debug("Terminating...");
        }

        guard.becomeTerminated(() -> {
            clients.clear();

            channel = null;
        });

        if (DEBUG) {
            log.debug("Terminated.");
        }
    }

    @Override
    public <T> RpcClientBuilder<T> clientFor(Class<T> type) {
        return clientFor(type, null);
    }

    @Override
    public <T> RpcClientBuilder<T> clientFor(Class<T> type, String tag) {
        ArgAssert.notNull(type, "Type");

        return guard.withReadLockAndStateCheck(() -> {
            RpcTypeKey key = new RpcTypeKey(type, tag);

            @SuppressWarnings("unchecked")
            RpcClientBuilder<T> client = (RpcClientBuilder<T>)clients.computeIfAbsent(key, missingKey ->
                createClient(missingKey, null)
            );

            return client;
        });
    }

    @Override
    public ClusterView clusterOf(Class<?> type) {
        return clusterOf(type, null);
    }

    @Override
    public ClusterView clusterOf(Class<?> type, String tag) {
        ArgAssert.notNull(type, "Type");

        return guard.withReadLockAndStateCheck(() -> {
            RpcInterfaceInfo<?> rpcType = types.analyzeType(type);

            return channel.cluster().filter(filterFor(rpcType, tag));
        });
    }

    @Override
    public List<RpcServerInfo> servers() {
        return servers;
    }

    @Override
    public int nioThreads() {
        return nioThreads;
    }

    @Override
    public int workerThreads() {
        return workerThreads;
    }

    private void handleMessage(Message<RpcProtocol> msg) {
        RpcProtocol rpcMsg = msg.payload();

        switch (rpcMsg.type()) {
            case COMPACT_CALL_REQUEST:
            case COMPACT_SPLIT_CALL_REQUEST: {
                RpcCompactCall call = (RpcCompactCall)rpcMsg;

                RpcMethodHandler handler = methods[call.methodIdx()];

                handler.handle(msg);

                break;
            }
            case CALL_REQUEST:
            case SPLIT_CALL_REQUEST:
            case OBJECT_RESPONSE:
            case NULL_RESPONSE:
            case ERROR_RESPONSE:
            default: {
                throw new IllegalArgumentException("Unexpected message type: " + rpcMsg);
            }
        }
    }

    private RpcClientBuilder<?> createClient(RpcTypeKey key, RpcClientConfig cfg) {
        RpcInterfaceInfo<?> type = types.analyzeType(key.type());

        MessagingChannel<RpcProtocol> channel = channelForClient(type, key.tag());

        DefaultRpcClientBuilder<?> client = new DefaultRpcClientBuilder<>(
            type,
            key.tag(),
            channel,
            cfg != null ? cfg.getTimeout() : 0,
            cfg != null ? cfg.getRetryPolicy() : null
        );

        if (DEBUG) {
            log.debug("Created new RPC client builder [key={}, builder={}]", key, client);
        }

        return client;
    }

    private MessagingChannel<RpcProtocol> channelForClient(RpcInterfaceInfo<?> type, String tag) {
        return this.channel.filter(filterFor(type, tag));
    }

    @Override
    public String toString() {
        return RpcService.class.getSimpleName();
    }
}
