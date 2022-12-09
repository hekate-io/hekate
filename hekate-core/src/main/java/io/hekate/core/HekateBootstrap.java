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

package io.hekate.core;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.ClusterView;
import io.hekate.codec.Codec;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecService;
import io.hekate.codec.JdkCodecFactory;
import io.hekate.coordinate.CoordinationService;
import io.hekate.coordinate.CoordinationServiceFactory;
import io.hekate.core.internal.HekateNodeFactory;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.jmx.JmxService;
import io.hekate.core.jmx.JmxServiceFactory;
import io.hekate.core.plugin.Plugin;
import io.hekate.core.service.Service;
import io.hekate.core.service.ServiceFactory;
import io.hekate.election.ElectionService;
import io.hekate.election.ElectionServiceFactory;
import io.hekate.lock.LockService;
import io.hekate.lock.LockServiceFactory;
import io.hekate.messaging.MessagingService;
import io.hekate.messaging.MessagingServiceFactory;
import io.hekate.network.NetworkService;
import io.hekate.network.NetworkServiceFactory;
import io.hekate.rpc.RpcService;
import io.hekate.rpc.RpcServiceFactory;
import io.hekate.util.format.ToString;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static io.hekate.core.internal.util.StreamUtils.nullSafe;

/**
 * Main configuration and factory for {@link Hekate} instances.
 *
 * <p>
 * This class provides bean-style properties with getters/setters (to be used in the <a href="http://projects.spring.io/spring-framework"
 * target="_blank">Spring Framework</a> XML files) as well as <a href="https://en.wikipedia.org/wiki/Fluent_interface"
 * target="_blank">fluent</a>-style API for programmatic configuration from within the Java code.
 * </p>
 *
 * <p>
 * Configuration options are:
 * </p>
 * <ul>
 * <li>{@link #setNodeName(String) Node name}</li>
 * <li>{@link #setProperties(Map) Node properties}</li>
 * <li>{@link #setRoles(List) Node roles}</li>
 * <li>{@link #setDefaultCodec(CodecFactory) Data serialization codec}</li>
 * <li>{@link #setPlugins(List) Plugins} that should run within the node</li>
 * <li>
 *     {@link #setServices(List) Services} to be provided by the node:
 *     <ul>
 *         <li>{@link #withNetwork(Consumer) Network Service}</li>
 *         <li>{@link #withCluster(Consumer) Cluster Service}</li>
 *         <li>{@link #withMessaging(Consumer) Messaging Service}</li>
 *         <li>{@link #withRpc(Consumer) RPC Service}</li>
 *         <li>{@link #withLocks(Consumer) Lock Service}</li>
 *         <li>{@link #withElection(Consumer) Election Service}</li>
 *         <li>{@link #withCoordination(Consumer) Coordination Service}</li>
 *         <li>{@link #withJmx(Consumer) JMX Service}</li>
 *     </ul>
 * </li>
 * </ul>
 *
 * <p>
 * Once configured, the {@link #join()} method must be called in order to construct a new {@link Hekate} instance and join the cluster.
 * </p>
 *
 * <p>
 * For more details about the lifecycle and available services please see the documentation of {@link Hekate} interface.
 * </p>
 */
public class HekateBootstrap {
    /** See {@link #setNodeName(String)}. */
    private String nodeName;

    /** See {@link #setRoles(List)}. */
    private List<String> roles;

    /** See {@link #setProperties(Map)}. */
    private Map<String, String> properties;

    /** See {@link #setPropertyProviders(List)}. */
    private List<PropertyProvider> propertyProviders;

    /** See {@link #setServices(List)}. */
    private List<ServiceFactory<? extends Service>> services;

    /** See {@link #setDefaultCodec(CodecFactory)}. */
    private CodecFactory<Object> defaultCodec = new JdkCodecFactory<>();

    /** See {@link #setPlugins(List)}. */
    private List<Plugin> plugins;

    /** See {@link #setMetrics(MeterRegistry)}. */
    private MeterRegistry metrics;

    /** See {@link #setLifecycleListeners(List)}. */
    private List<Hekate.LifecycleListener> lifecycleListeners;

    /** See {@link #setConfigReport(boolean)}. */
    private boolean configReport;

    /** See {@link #setFatalErrorPolicy(HekateFatalErrorPolicy)}. */
    private HekateFatalErrorPolicy fatalErrorPolicy = HekateFatalErrorPolicy.terminate();

    /**
     * Constructs a new {@link Hekate} instance and asynchronously {@link Hekate#joinAsync() joins} the cluster.
     *
     * @return Future result of this operation.
     *
     * @throws HekateException If configuration is invalid.
     */
    public JoinFuture joinAsync() throws HekateException {
        return create().joinAsync();
    }

    /**
     * Constructs a new {@link Hekate} instance and synchronously {@link Hekate#join() joins} the cluster.
     *
     * @return new {@link Hekate} instance.
     *
     * @throws HekateException If failure occurred while initializing or joining to cluster.
     */
    public Hekate join() throws HekateException {
        return create().join();
    }

    /**
     * Constructs a new {@link Hekate} instance and synchronously {@link Hekate#initialize() initializes} it without joining the cluster.
     *
     * <p>
     * Joining to the cluster can be later performed via {@link Hekate#join()} method.
     * </p>
     *
     * @return new {@link Hekate} instance.
     *
     * @throws HekateException If failure occurred while initializing or joining to cluster.
     */
    public Hekate initialize() throws HekateException {
        return create().initialize();
    }

    /**
     * Returns the node name (see {@link #setNodeName(String)}).
     *
     * @return Node name.
     */
    public String getNodeName() {
        return nodeName;
    }

    /**
     * Sets the node name. Can contain only alpha-numeric characters and non-repeatable dots/hyphens.
     *
     * <p>
     * Node name is optional and its default value is {@code null}.
     * </p>
     *
     * @param nodeName Node name (can contain only alpha-numeric characters and non-repeatable dots/hyphens).
     *
     * @see ClusterNode#name()
     */
    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    /**
     * Fluent-style version of {@link #setNodeName(String)}.
     *
     * @param nodeName Node name.
     *
     * @return This instance.
     */
    public HekateBootstrap withNodeName(String nodeName) {
        setNodeName(nodeName);

        return this;
    }

    /**
     * Returns a list of the local node's roles (see {@link #setRoles(List)}).
     *
     * @return List of local node roles.
     */
    public List<String> getRoles() {
        return roles;
    }

    /**
     * Sets node's roles.
     *
     * <p>
     * Roles are string identifiers that can be used for logical grouping of cluster nodes. Roles of each node are visible to all other
     * cluster members via {@link ClusterNode#roles()} and can be used by applications to operate on a sub-set of cluster nodes with
     * specific role(s) (see {@link ClusterView#forRole(String)}).
     * </p>
     *
     * @param roles Set of roles.
     *
     * @see ClusterNode#roles()
     * @see ClusterTopology#filter(ClusterNodeFilter)
     */
    public void setRoles(List<String> roles) {
        this.roles = roles;
    }

    /**
     * Adds the specified {@code role} to the {@link #setRoles(List) roles set}.
     *
     * @param role Role.
     *
     * @return This instance.
     */
    public HekateBootstrap withRole(String role) {
        ConfigCheck.get(getClass()).notNull(role, "role");

        if (roles == null) {
            roles = new ArrayList<>();
        }

        roles.add(role);

        return this;
    }

    /**
     * Returns a map of local node properties (see {@link #setProperties(Map)}).
     *
     * @return Map of local node properties.
     */
    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Sets node's properties.
     *
     * <p>
     * Properties of each node are visible to all other cluster members via {@link ClusterNode#properties()}.
     * </p>
     *
     * @param properties Map of local node properties.
     *
     * @see ClusterNode#properties()
     * @see #setPropertyProviders(List)
     */
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    /**
     * Puts the specified property value into the node's {@link #setProperties(Map) properties map}.
     *
     * @param key Property key.
     * @param value Property value.
     *
     * @return This instance.
     */
    public HekateBootstrap withProperty(String key, String value) {
        if (properties == null) {
            properties = new HashMap<>();
        }

        properties.put(key, value);

        return this;
    }

    /**
     * Returns a list of node property providers (see {@link #setPropertyProviders(List)}).
     *
     * @return Node property providers.
     */
    public List<PropertyProvider> getPropertyProviders() {
        return propertyProviders;
    }

    /**
     * Sets the list of node property providers.
     *
     * <p>
     * Such providers can be used to provide properties that were dynamically obtained from some third party sources and should become part
     * of {@link #setProperties(Map) node properties}.
     * </p>
     *
     * @param propertyProviders Node property providers.
     *
     * @see #setProperties(Map)
     */
    public void setPropertyProviders(List<PropertyProvider> propertyProviders) {
        this.propertyProviders = propertyProviders;
    }

    /**
     * Fluent-style version of {@link #setPropertyProviders(List)}.
     *
     * @param propertyProvider Property provider.
     *
     * @return This instance.
     */
    public HekateBootstrap withPropertyProvider(PropertyProvider propertyProvider) {
        if (propertyProviders == null) {
            propertyProviders = new ArrayList<>();
        }

        propertyProviders.add(propertyProvider);

        return this;
    }

    /**
     * Returns services of the local node (see {@link #setServices(List)}).
     *
     * @return List of service factories.
     */
    public List<ServiceFactory<? extends Service>> getServices() {
        return services;
    }

    /**
     * Sets the list of services that should be provided by the local node.
     *
     * @param services Service factories.
     *
     * @see Service
     * @see ServiceFactory
     */
    public void setServices(List<ServiceFactory<? extends Service>> services) {
        this.services = services;
    }

    /**
     * Adds the specified factory to the list of {@link #setServices(List) service factories}.
     *
     * @param service Service factory.
     *
     * @return This instance.
     */
    public HekateBootstrap withService(ServiceFactory<? extends Service> service) {
        if (services == null) {
            services = new ArrayList<>();
        }

        services.add(service);

        return this;
    }

    /**
     * Applies the specified {@code configurer} to a service factory of the specified type. If factory is not registered yet then it will be
     * automatically registered via {@link #withService(Class)}.
     *
     * @param factoryType Service factory type.
     * @param configurer Service factory configurer.
     * @param <T> Service factory type.
     *
     * @return This instance.
     */
    public <T extends ServiceFactory<?>> HekateBootstrap withService(Class<T> factoryType, Consumer<T> configurer) {
        T factory = withService(factoryType);

        configurer.accept(factory);

        return this;
    }

    /**
     * Returns an existing service factory of the specified type or creates and registers a new one.
     *
     * <p>
     * Note that the specified type must have a public no-arg constructor.
     * </p>
     *
     * @param factoryType Service factory type.
     * @param <T> Service factory type.
     *
     * @return Service factory instance.
     */
    public <T extends ServiceFactory<?>> T withService(Class<T> factoryType) {
        Optional<T> mayBeFactory = service(factoryType);

        if (mayBeFactory.isPresent()) {
            return mayBeFactory.get();
        } else {
            T factory;

            try {
                factory = factoryType.getConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                throw new HekateConfigurationException("Failed to instantiate service factory [type=" + factoryType + ']', e);
            }

            withService(factory);

            return factory;
        }
    }

    /**
     * Configures {@link JmxService}.
     *
     * @param configurer Configuration function.
     *
     * @return This instance.
     *
     * @see JmxServiceFactory
     */
    public HekateBootstrap withJmx(Consumer<JmxServiceFactory> configurer) {
        return withService(JmxServiceFactory.class, configurer);
    }

    /**
     * Configures {@link NetworkService}.
     *
     * @param configurer Configuration function.
     *
     * @return This instance.
     *
     * @see NetworkServiceFactory
     */
    public HekateBootstrap withNetwork(Consumer<NetworkServiceFactory> configurer) {
        return withService(NetworkServiceFactory.class, configurer);
    }

    /**
     * Configures {@link ClusterService}.
     *
     * @param configurer Configuration function.
     *
     * @return This instance.
     *
     * @see ClusterServiceFactory
     */
    public HekateBootstrap withCluster(Consumer<ClusterServiceFactory> configurer) {
        return withService(ClusterServiceFactory.class, configurer);
    }

    /**
     * Configures {@link MessagingService}.
     *
     * @param configurer Configuration function.
     *
     * @return This instance.
     *
     * @see MessagingServiceFactory
     */
    public HekateBootstrap withMessaging(Consumer<MessagingServiceFactory> configurer) {
        return withService(MessagingServiceFactory.class, configurer);
    }

    /**
     * Configures {@link RpcService}.
     *
     * @param configurer Configuration function.
     *
     * @return This instance.
     *
     * @see RpcServiceFactory
     */
    public HekateBootstrap withRpc(Consumer<RpcServiceFactory> configurer) {
        return withService(RpcServiceFactory.class, configurer);
    }

    /**
     * Configures {@link LockService}.
     *
     * @param configurer Configuration function.
     *
     * @return This instance.
     *
     * @see LockServiceFactory
     */
    public HekateBootstrap withLocks(Consumer<LockServiceFactory> configurer) {
        return withService(LockServiceFactory.class, configurer);
    }

    /**
     * Configures {@link ElectionService}.
     *
     * @param configurer Configuration function.
     *
     * @return This instance.
     *
     * @see ElectionServiceFactory
     */
    public HekateBootstrap withElection(Consumer<ElectionServiceFactory> configurer) {
        return withService(ElectionServiceFactory.class, configurer);
    }

    /**
     * Configures {@link CoordinationService}.
     *
     * @param configurer Configuration function.
     *
     * @return This instance.
     *
     * @see CoordinationServiceFactory
     */
    public HekateBootstrap withCoordination(Consumer<CoordinationServiceFactory> configurer) {
        return withService(CoordinationServiceFactory.class, configurer);
    }

    /**
     * Finds a service factory of the specified type (see {@link #setServices(List)}).
     *
     * @param factoryType Service factory type.
     * @param <T> Service factory type.
     *
     * @return Optional service factory if it was registered within this instance.
     */
    public <T extends ServiceFactory<?>> Optional<T> service(Class<T> factoryType) {
        return nullSafe(services)
            .filter(factory -> factoryType.isAssignableFrom(factory.getClass()))
            .map(factoryType::cast)
            .findFirst();
    }

    /**
     * Returns the list of plugins that should run within the local node (see {@link #setPlugins(List)}).
     *
     * @return List of plugins.
     */
    public List<Plugin> getPlugins() {
        return plugins;
    }

    /**
     * Sets the list of plugins that should run within the local node.
     *
     * <p>
     * Plugins are custom extensions that run within the context of a local node and whose lifecycle is controlled by the lifecycle of that
     * node. For more info please see description of the {@link Plugin} interface.
     * </p>
     *
     * @param plugins List of plugins.
     *
     * @see Plugin
     */
    public void setPlugins(List<Plugin> plugins) {
        this.plugins = plugins;
    }

    /**
     * Adds the specified plugin to a {@link #setPlugins(List) list of plugins}.
     *
     * @param plugin Plugin.
     *
     * @return This instance.
     */
    public HekateBootstrap withPlugin(Plugin plugin) {
        if (plugins == null) {
            plugins = new ArrayList<>();
        }

        plugins.add(plugin);

        return this;
    }

    /**
     * Returns the default codec factory (see {@link #setDefaultCodec(CodecFactory)}).
     *
     * @return Default codec factory.
     */
    public CodecFactory<Object> getDefaultCodec() {
        return defaultCodec;
    }

    /**
     * Sets the codec factory that should be used by the {@link CodecService}.
     *
     * <p>
     * This parameter is mandatory and can't be {@code null}. Also, the specified codec factory must be stateless (see {@link
     * Codec#isStateful()}).
     * </p>
     *
     * <p>
     * If not configured then {@link JdkCodecFactory} will be used by default.
     * </p>
     *
     * @param defaultCodec Codec factory (must be stateless, see {@link Codec#isStateful()}) .
     *
     * @see CodecService
     */
    public void setDefaultCodec(CodecFactory<Object> defaultCodec) {
        this.defaultCodec = defaultCodec;
    }

    /**
     * Fluent-style version of {@link #setDefaultCodec(CodecFactory)}.
     *
     * @param defaultCodec Default codec factory.
     *
     * @return This instance.
     */
    public HekateBootstrap withDefaultCodec(CodecFactory<Object> defaultCodec) {
        setDefaultCodec(defaultCodec);

        return this;
    }

    /**
     * Returns the metrics registry.
     *
     * @return Metrics registry.
     */
    public MeterRegistry getMetrics() {
        return metrics;
    }

    /**
     * Sets the metrics registry.
     *
     * <p>
     * If not specified then {@link SimpleMeterRegistry} will be used by default.
     * </p>
     *
     * @param metrics Metrics registry.
     */
    public void setMetrics(MeterRegistry metrics) {
        this.metrics = metrics;
    }

    /**
     * Fluent-style version of {@link #setMetrics(MeterRegistry)}.
     *
     * @param metrics Metrics registry.
     *
     * @return This instance.
     */
    public HekateBootstrap withMetrics(MeterRegistry metrics) {
        setMetrics(metrics);

        return this;
    }

    /**
     * Returns the list of instance lifecycle listeners (see {@link #setLifecycleListeners(List)}).
     *
     * @return Lifecycle listeners.
     */
    public List<Hekate.LifecycleListener> getLifecycleListeners() {
        return lifecycleListeners;
    }

    /**
     * Sets the list of {@link Hekate} lifecycle listeners.
     *
     * @param lifecycleListeners Lifecycle listeners.
     *
     * @see Hekate#addListener(Hekate.LifecycleListener)
     */
    public void setLifecycleListeners(List<Hekate.LifecycleListener> lifecycleListeners) {
        this.lifecycleListeners = lifecycleListeners;
    }

    /**
     * Fluent-style version of {@link #setLifecycleListeners(List)}.
     *
     * @param listener Lifecycle listener.
     *
     * @return This instance.
     */
    public HekateBootstrap withLifecycleListener(Hekate.LifecycleListener listener) {
        if (lifecycleListeners == null) {
            lifecycleListeners = new ArrayList<>();
        }

        lifecycleListeners.add(listener);

        return this;
    }

    /**
     * Returns fatal error handling policy (see {@link #setFatalErrorPolicy(HekateFatalErrorPolicy)}).
     *
     * @return Fatal error handling policy.
     */
    public HekateFatalErrorPolicy getFatalErrorPolicy() {
        return fatalErrorPolicy;
    }

    /**
     * Sets the fatal error handling policy.
     *
     * <p>
     * This policy is applied if {@link Hekate} node faces an unrecoverable internal error.
     * </p>
     *
     * <p>
     * If not specified then {@link HekateFatalErrorPolicy#terminate()} policy will be used by default.
     * </p>
     *
     * @param fatalErrorPolicy Fatal error handling policy.
     */
    public void setFatalErrorPolicy(HekateFatalErrorPolicy fatalErrorPolicy) {
        this.fatalErrorPolicy = fatalErrorPolicy;
    }

    /**
     * Fluent-style version of {@link #setFatalErrorPolicy(HekateFatalErrorPolicy)}.
     *
     * @param fatalErrorPolicy Fatal error handling policy.
     *
     * @return Fatal error handling policy.
     */
    public HekateBootstrap withFatalErrorPolicy(HekateFatalErrorPolicy fatalErrorPolicy) {
        setFatalErrorPolicy(fatalErrorPolicy);

        return this;
    }

    /**
     * {@code true} if the configuration report should be logged during node initialization.
     *
     * @return {@code true} if the configuration report should be logged during node initialization.
     */
    public boolean isConfigReport() {
        return configReport;
    }

    /**
     * {@code true} if the configuration report should be logged during node initialization.
     *
     * <p>
     * Default value of this property is {@code false}.
     * </p>
     *
     * @param configReport {@code true} if the configuration report should be logged during node initialization.
     */
    public void setConfigReport(boolean configReport) {
        this.configReport = configReport;
    }

    /**
     * Fluent-style version of {@link #setConfigReport(boolean)}.
     *
     * @param configReport {@code true} if the configuration report should be logged during node initialization.
     *
     * @return This instance.
     */
    public HekateBootstrap withConfigReport(boolean configReport) {
        setConfigReport(configReport);

        return this;
    }

    /**
     * Creates a new {@link Hekate} instance.
     *
     * @return New {@link Hekate} instance.
     */
    protected Hekate create() {
        return HekateNodeFactory.create(this);
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
