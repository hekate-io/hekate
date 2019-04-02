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

package io.hekate.core;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.ClusterView;
import io.hekate.codec.AutoSelectCodecFactory;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecService;
import io.hekate.core.internal.HekateNodeFactory;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.StreamUtils;
import io.hekate.core.plugin.Plugin;
import io.hekate.core.service.Service;
import io.hekate.core.service.ServiceFactory;
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
 * <li>{@link #setClusterName(String) Cluster name}</li>
 * <li>{@link #setNodeName(String) Node name}</li>
 * <li>{@link #setProperties(Map) Node properties}</li>
 * <li>{@link #setRoles(List) Node roles}</li>
 * <li>{@link #setDefaultCodec(CodecFactory) Data serialization codec}</li>
 * <li>{@link #setServices(List) Services} to be provided by the node</li>
 * <li>{@link #setPlugins(List) Plugins} that should run within the node</li>
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
    /** Default value (={@value}) for {@link #setClusterName(String)}. */
    public static final String DEFAULT_CLUSTER_NAME = "default";

    private String nodeName;

    private String clusterName = DEFAULT_CLUSTER_NAME;

    private List<String> roles;

    private Map<String, String> properties;

    private List<PropertyProvider> propertyProviders;

    private List<ServiceFactory<? extends Service>> services;

    private CodecFactory<Object> defaultCodec = new AutoSelectCodecFactory<>();

    private List<Plugin> plugins;

    private MeterRegistry metrics;

    private List<Hekate.LifecycleListener> lifecycleListeners;

    /**
     * Constructs a new {@link Hekate} instance and asynchronously {@link Hekate#joinAsync() joins} the cluster.
     *
     * @return Future result of this operation.
     *
     * @throws HekateConfigurationException If configuration is invalid.
     */
    public JoinFuture joinAsync() throws HekateConfigurationException {
        return create().joinAsync();
    }

    /**
     * Constructs a new {@link Hekate} instance and synchronously {@link Hekate#join() joins} the cluster.
     *
     * @return new {@link Hekate} instance.
     *
     * @throws HekateConfigurationException If configuration is invalid.
     * @throws HekateFutureException If failure occurred while initializing or joining to cluster.
     * @throws InterruptedException If the current thread was interrupted while awaiting for completion of this operation.
     */
    public Hekate join() throws HekateConfigurationException, InterruptedException, HekateFutureException {
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
     * @throws HekateConfigurationException If configuration is invalid.
     * @throws HekateFutureException If failure occurred while initializing or joining to cluster.
     * @throws InterruptedException If the current thread was interrupted while awaiting for completion of this operation.
     */
    public Hekate initialize() throws HekateFutureException, InterruptedException {
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
     * Returns the cluster name (see {@link #setClusterName(String)}).
     *
     * @return Cluster name.
     */
    public String getClusterName() {
        return clusterName;
    }

    /**
     * Sets the cluster name. Can contain only alpha-numeric characters and non-repeatable dots/hyphens.
     *
     * <p>
     * Only those instances that are configured with the same cluster name can form a cluster. Instances with different cluster names will
     * form completely independent clusters.
     * </p>
     *
     * <p>
     * Default value of this property is {@value #DEFAULT_CLUSTER_NAME}.
     * </p>
     *
     * <p>
     * <b>Hint:</b> For breaking nodes into logical sub-groups within the same cluster consider using {@link #setRoles(List) node roles}
     * together with {@link ClusterView#filter(ClusterNodeFilter) nodes filtering} by role.
     * </p>
     *
     * @param clusterName Cluster name (can contain only alpha-numeric characters and non-repeatable dots/hyphens).
     */
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    /**
     * Fluent-style version of {@link #setClusterName(String)}.
     *
     * @param clusterName Cluster name.
     *
     * @return This instance.
     */
    public HekateBootstrap withClusterName(String clusterName) {
        setClusterName(clusterName);

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
     * Applies the specified {@code configurer} to a service factory of the specified type. If factory is not registered yet then it will
     * be automatically registered via {@link #withService(Class)}.
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
     * Finds a service factory of the specified type (see {@link #setServices(List)}).
     *
     * @param factoryType Service factory type.
     * @param <T> Service factory type.
     *
     * @return Optional service factory if it was registered within this instance.
     */
    public <T extends ServiceFactory<?>> Optional<T> service(Class<T> factoryType) {
        return StreamUtils.nullSafe(services)
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
     * Plugins are custom extensions that run within the context of a local node and whose lifecycle is controlled by the lifecycle of
     * that node. For more info please see description of the {@link Plugin} interface.
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
     * This parameter is mandatory and can't be {@code null}. If not configured then {@link AutoSelectCodecFactory} will be used by default.
     * </p>
     *
     * @param defaultCodec Default codec factory.
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
