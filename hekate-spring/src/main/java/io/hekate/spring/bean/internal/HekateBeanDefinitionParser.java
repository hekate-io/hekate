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

package io.hekate.spring.bean.internal;

import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.health.DefaultFailureDetector;
import io.hekate.cluster.health.DefaultFailureDetectorConfig;
import io.hekate.cluster.seed.SeedNodeProviderGroup;
import io.hekate.cluster.seed.SeedNodeProviderGroupConfig;
import io.hekate.cluster.seed.StaticSeedNodeProvider;
import io.hekate.cluster.seed.StaticSeedNodeProviderConfig;
import io.hekate.cluster.seed.fs.FsSeedNodeProvider;
import io.hekate.cluster.seed.fs.FsSeedNodeProviderConfig;
import io.hekate.cluster.seed.jclouds.BasicCredentialsSupplier;
import io.hekate.cluster.seed.jclouds.CloudSeedNodeProvider;
import io.hekate.cluster.seed.jclouds.CloudSeedNodeProviderConfig;
import io.hekate.cluster.seed.jclouds.CloudStoreSeedNodeProvider;
import io.hekate.cluster.seed.jclouds.CloudStoreSeedNodeProviderConfig;
import io.hekate.cluster.seed.jclouds.aws.AwsCredentialsSupplier;
import io.hekate.cluster.seed.jdbc.JdbcSeedNodeProvider;
import io.hekate.cluster.seed.jdbc.JdbcSeedNodeProviderConfig;
import io.hekate.cluster.seed.multicast.MulticastSeedNodeProvider;
import io.hekate.cluster.seed.multicast.MulticastSeedNodeProviderConfig;
import io.hekate.cluster.seed.zookeeper.ZooKeeperSeedNodeProvider;
import io.hekate.cluster.seed.zookeeper.ZooKeeperSeedNodeProviderConfig;
import io.hekate.cluster.split.AddressReachabilityDetector;
import io.hekate.cluster.split.HostReachabilityDetector;
import io.hekate.cluster.split.SplitBrainDetectorGroup;
import io.hekate.coordinate.CoordinationProcessConfig;
import io.hekate.coordinate.CoordinationServiceFactory;
import io.hekate.core.jmx.JmxServiceFactory;
import io.hekate.election.CandidateConfig;
import io.hekate.election.ElectionServiceFactory;
import io.hekate.lock.LockRegionConfig;
import io.hekate.lock.LockServiceFactory;
import io.hekate.messaging.MessagingBackPressureConfig;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingServiceFactory;
import io.hekate.network.NetworkConnectorConfig;
import io.hekate.network.NetworkServiceFactory;
import io.hekate.rpc.RpcClientConfig;
import io.hekate.rpc.RpcServerConfig;
import io.hekate.rpc.RpcServiceFactory;
import io.hekate.spring.bean.HekateSpringBootstrap;
import io.hekate.spring.bean.cluster.ClusterServiceBean;
import io.hekate.spring.bean.codec.CodecServiceBean;
import io.hekate.spring.bean.coordinate.CoordinationServiceBean;
import io.hekate.spring.bean.election.ElectionServiceBean;
import io.hekate.spring.bean.lock.LockBean;
import io.hekate.spring.bean.lock.LockRegionBean;
import io.hekate.spring.bean.lock.LockServiceBean;
import io.hekate.spring.bean.messaging.MessagingChannelBean;
import io.hekate.spring.bean.messaging.MessagingServiceBean;
import io.hekate.spring.bean.network.NetworkConnectorBean;
import io.hekate.spring.bean.network.NetworkServiceBean;
import io.hekate.spring.bean.rpc.RpcClientBean;
import io.hekate.spring.bean.rpc.RpcServiceBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.parsing.BeanComponentDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.support.ManagedSet;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

import static java.util.stream.Collectors.toSet;
import static org.springframework.util.xml.DomUtils.getChildElementByTagName;
import static org.springframework.util.xml.DomUtils.getChildElements;
import static org.springframework.util.xml.DomUtils.getChildElementsByTagName;
import static org.springframework.util.xml.DomUtils.getTextValue;

public class HekateBeanDefinitionParser extends AbstractSingleBeanDefinitionParser {
    private final Map<BeanDefinitionBuilder, String> deferredBaseBeans = new HashMap<>();

    @Override
    protected Class<?> getBeanClass(Element element) {
        return HekateSpringBootstrap.class;
    }

    @Override
    protected boolean shouldGenerateId() {
        return false;
    }

    @Override
    protected boolean shouldGenerateIdAsFallback() {
        return true;
    }

    @Override
    protected String resolveId(Element el, AbstractBeanDefinition def, ParserContext ctx) {
        String id = super.resolveId(el, def, ctx);

        deferredBaseBeans.forEach((baseBeanBuilder, baseBeanName) -> {
            baseBeanBuilder.addPropertyValue("source", new RuntimeBeanReference(id));

            AbstractBeanDefinition baseBean = baseBeanBuilder.getBeanDefinition();

            if (baseBeanName == null) {
                ctx.getRegistry().registerBeanDefinition(ctx.getReaderContext().generateBeanName(baseBean), baseBean);
            } else {
                ctx.getRegistry().registerBeanDefinition(baseBeanName, baseBean);
            }
        });

        return id;
    }

    @Override
    protected void doParse(Element rootEl, ParserContext ctx, BeanDefinitionBuilder boot) {
        setProperty(boot, rootEl, "nodeName", "name");
        setProperty(boot, rootEl, "clusterName", "cluster");

        parseNodeRoles(boot, rootEl);

        parseProperties(rootEl).ifPresent(props ->
            boot.addPropertyValue("properties", props)
        );

        parseNodePropertyProviders(boot, rootEl, ctx);

        parseDefaultCodec(boot, rootEl, ctx);

        ManagedList<RuntimeBeanReference> services = new ManagedList<>();

        parseJmxService(rootEl, ctx).ifPresent(services::add);
        parseClusterService(rootEl, ctx).ifPresent(services::add);
        parseNetworkService(rootEl, ctx).ifPresent(services::add);
        parseMessagingService(rootEl, ctx).ifPresent(services::add);
        parseRpcService(rootEl, ctx).ifPresent(services::add);
        parseLockService(rootEl, ctx).ifPresent(services::add);
        parseCoordinationService(rootEl, ctx).ifPresent(services::add);
        parseElectionService(rootEl, ctx).ifPresent(services::add);

        subElements(rootEl, "custom-services", "service").forEach(serviceEl ->
            parseRefOrBean(serviceEl, ctx).ifPresent(services::add)
        );

        if (!services.isEmpty()) {
            boot.addPropertyValue("services", services);
        }

        ManagedList<RuntimeBeanReference> plugins = new ManagedList<>();

        subElements(rootEl, "plugins", "plugin").forEach(pluginEl ->
            parseRefOrBean(pluginEl, ctx).ifPresent(plugins::add)
        );

        if (!plugins.isEmpty()) {
            boot.addPropertyValue("plugins", plugins);
        }
    }

    private void parseNodeRoles(BeanDefinitionBuilder def, Element el) {
        ManagedSet<String> roles = new ManagedSet<>();

        roles.addAll(subElements(el, "roles", "role").stream()
            .map(roleEl -> getTextValue(roleEl).trim())
            .filter(role -> !role.isEmpty())
            .collect(toSet())
        );

        def.addPropertyValue("roles", roles);
    }

    private void parseNodePropertyProviders(BeanDefinitionBuilder boot, Element rootEl, ParserContext ctx) {
        ManagedList<RuntimeBeanReference> propertyProviders = new ManagedList<>();

        subElements(rootEl, "property-providers", "provider").forEach(provider ->
            parseRefOrBean(provider, ctx).ifPresent(propertyProviders::add)
        );

        if (!propertyProviders.isEmpty()) {
            boot.addPropertyValue("propertyProviders", propertyProviders);
        }
    }

    private void parseDefaultCodec(BeanDefinitionBuilder boot, Element rootEl, ParserContext ctx) {
        setBeanOrRef(boot, rootEl, "defaultCodec", "default-codec", ctx);

        BeanDefinitionBuilder serviceDef = newBean(CodecServiceBean.class, rootEl);

        deferredBaseBeans.put(serviceDef, null);
    }

    private Optional<RuntimeBeanReference> parseJmxService(Element rootEl, ParserContext ctx) {
        Element jmxEl = getChildElementByTagName(rootEl, "jmx");

        if (jmxEl != null) {
            BeanDefinitionBuilder jmx = newBean(JmxServiceFactory.class, jmxEl);

            setProperty(jmx, jmxEl, "domain", "domain");

            setBeanOrRef(jmx, jmxEl, "server", "server", ctx);

            return Optional.of(registerInnerBean(jmx, ctx));
        } else {
            return Optional.empty();
        }
    }

    private Optional<RuntimeBeanReference> parseClusterService(Element rootEl, ParserContext ctx) {
        Element clusterEl = getChildElementByTagName(rootEl, "cluster");

        if (clusterEl != null) {
            BeanDefinitionBuilder cluster = newBean(ClusterServiceFactory.class, clusterEl);

            setProperty(cluster, clusterEl, "gossipInterval", "gossip-interval-ms");
            setProperty(cluster, clusterEl, "speedUpGossipSize", "gossip-speedup-size");

            parseSeedNodeProvider(cluster, clusterEl, ctx);

            parseClusterFailureDetection(cluster, clusterEl, ctx);

            parseClusterSplitBrainDetection(cluster, clusterEl, ctx);

            parseClusterAcceptors(cluster, clusterEl, ctx);

            parseClusterListeners(cluster, clusterEl, ctx);

            String id = clusterEl.getAttribute("id");

            if (!id.isEmpty()) {
                deferredBaseBeans.put(newLazyBean(ClusterServiceBean.class, clusterEl), id);
            }

            return Optional.of(registerInnerBean(cluster, ctx));
        } else {
            return Optional.empty();
        }
    }

    private void parseSeedNodeProvider(BeanDefinitionBuilder cluster, Element clusterEl, ParserContext ctx) {
        Element providerEl = getChildElementByTagName(clusterEl, "seed-node-provider");

        if (providerEl != null) {
            getChildElements(providerEl).stream().findFirst().ifPresent(el -> {
                RuntimeBeanReference provider = parseSeedNodeProviderChild(el, ctx);

                cluster.addPropertyValue("seedNodeProvider", provider);
            });
        }
    }

    private RuntimeBeanReference parseSeedNodeProviderChild(Element el, ParserContext ctx) {
        switch (el.getLocalName()) {
            case "group": {
                return parseSeedNodeProviderGroup(el, ctx);
            }
            case "multicast": {
                return parseMulticastSeedNodeProvider(el, ctx);
            }
            case "static": {
                return parseStaticSeedNodeProvider(el, ctx);
            }
            case "jdbc": {
                return parseJdbcSeedNodeprovider(el, ctx);
            }
            case "shared-folder": {
                return parseFsSeedNodeProvider(el, ctx);
            }
            case "cloud": {
                return parseCloudSeedNodeProvider(el, ctx);
            }
            case "cloud-store": {
                return parseCloudStoreSeedNodeProvider(el, ctx);
            }
            case "zookeeper": {
                return parseZooKeeperSeedNodeProvider(el, ctx);
            }
            case "custom-provider": {
                return parseRefOrBean(el, ctx).orElseGet(() -> {
                    ctx.getReaderContext().error("Malformed seed node provider element <" + el.getLocalName() + '>', el);

                    return null;
                });
            }
            default: {
                ctx.getReaderContext().error("Unsupported seed node provider element <" + el.getLocalName() + '>', el);

                return null;
            }
        }
    }

    private RuntimeBeanReference parseZooKeeperSeedNodeProvider(Element el, ParserContext ctx) {
        BeanDefinitionBuilder cfg = newBean(ZooKeeperSeedNodeProviderConfig.class, el);

        setProperty(cfg, el, "connectionString", "connection-string");
        setProperty(cfg, el, "connectTimeout", "connect-timeout-ms");
        setProperty(cfg, el, "sessionTimeout", "session-timeout-ms");
        setProperty(cfg, el, "basePath", "base-path");
        setProperty(cfg, el, "cleanupInterval", "cleanup-interval-ms");

        BeanDefinitionBuilder provider = newBean(ZooKeeperSeedNodeProvider.class, el);

        provider.addConstructorArgValue(registerInnerBean(cfg, ctx));

        return registerInnerBean(provider, ctx);
    }

    private RuntimeBeanReference parseCloudStoreSeedNodeProvider(Element el, ParserContext ctx) {
        BeanDefinitionBuilder cfg = newBean(CloudStoreSeedNodeProviderConfig.class, el);

        setProperty(cfg, el, "provider", "provider");
        setProperty(cfg, el, "container", "container");
        setProperty(cfg, el, "cleanupInterval", "cleanup-interval-ms");

        parseProperties(el).ifPresent(props ->
            cfg.addPropertyValue("properties", props)
        );

        parseCloudProviderCredentials(el, ctx).ifPresent(credRef ->
            cfg.addPropertyValue("credentials", credRef)
        );

        BeanDefinitionBuilder provider = newBean(CloudStoreSeedNodeProvider.class, el);

        provider.addConstructorArgValue(registerInnerBean(cfg, ctx));

        return registerInnerBean(provider, ctx);
    }

    private RuntimeBeanReference parseCloudSeedNodeProvider(Element el, ParserContext ctx) {
        BeanDefinitionBuilder cfg = newBean(CloudSeedNodeProviderConfig.class, el);

        setProperty(cfg, el, "provider", "provider");
        setProperty(cfg, el, "endpoint", "endpoint");

        parseProperties(el).ifPresent(props ->
            cfg.addPropertyValue("properties", props)
        );

        parseCloudProviderCredentials(el, ctx).ifPresent(credRef ->
            cfg.addPropertyValue("credentials", credRef)
        );

        // Regions.
        ManagedSet<String> regions = new ManagedSet<>();

        regions.addAll(subElements(el, "regions", "region").stream()
            .map(regionEl -> getTextValue(regionEl).trim())
            .filter(region -> !region.isEmpty())
            .collect(toSet())
        );

        if (!regions.isEmpty()) {
            cfg.addPropertyValue("regions", regions);
        }

        // Zones.
        ManagedSet<String> zones = new ManagedSet<>();

        zones.addAll(subElements(el, "zones", "zone").stream()
            .map(zoneEl -> getTextValue(zoneEl).trim())
            .filter(zone -> !zone.isEmpty())
            .collect(toSet())
        );

        if (!zones.isEmpty()) {
            cfg.addPropertyValue("zones", zones);
        }

        // Tags.
        ManagedMap<String, String> tags = new ManagedMap<>();

        subElements(el, "tags", "tag").forEach(tagEl -> {
            String name = tagEl.getAttribute("name").trim();
            String value = tagEl.getAttribute("value").trim();

            if (!name.isEmpty() && !value.isEmpty()) {
                tags.put(name, value);
            }
        });

        if (!tags.isEmpty()) {
            cfg.addPropertyValue("tags", tags);
        }

        BeanDefinitionBuilder provider = newBean(CloudSeedNodeProvider.class, el);

        provider.addConstructorArgValue(registerInnerBean(cfg, ctx));

        return registerInnerBean(provider, ctx);
    }

    private RuntimeBeanReference parseFsSeedNodeProvider(Element el, ParserContext ctx) {
        BeanDefinitionBuilder cfg = newBean(FsSeedNodeProviderConfig.class, el);

        setProperty(cfg, el, "workDir", "work-dir");
        setProperty(cfg, el, "cleanupInterval", "cleanup-interval-ms");

        BeanDefinitionBuilder provider = newBean(FsSeedNodeProvider.class, el);

        provider.addConstructorArgValue(registerInnerBean(cfg, ctx));

        return registerInnerBean(provider, ctx);
    }

    private RuntimeBeanReference parseJdbcSeedNodeprovider(Element el, ParserContext ctx) {
        BeanDefinitionBuilder cfg = newBean(JdbcSeedNodeProviderConfig.class, el);

        setProperty(cfg, el, "queryTimeout", "query-timeout-sec");
        setProperty(cfg, el, "cleanupInterval", "cleanup-interval-ms");
        setProperty(cfg, el, "table", "table");
        setProperty(cfg, el, "hostColumn", "host-column");
        setProperty(cfg, el, "portColumn", "port-column");
        setProperty(cfg, el, "clusterColumn", "cluster-column");

        setBeanOrRef(cfg, el, "dataSource", "datasource", ctx);

        BeanDefinitionBuilder provider = newBean(JdbcSeedNodeProvider.class, el);

        provider.addConstructorArgValue(registerInnerBean(cfg, ctx));

        return registerInnerBean(provider, ctx);
    }

    private RuntimeBeanReference parseStaticSeedNodeProvider(Element el, ParserContext ctx) {
        BeanDefinitionBuilder cfg = newBean(StaticSeedNodeProviderConfig.class, el);

        List<String> addresses = new ArrayList<>();

        getChildElementsByTagName(el, "address").forEach(addrEl ->
            addresses.add(getTextValue(addrEl))
        );

        if (!addresses.isEmpty()) {
            cfg.addPropertyValue("addresses", addresses);
        }

        BeanDefinitionBuilder provider = newBean(StaticSeedNodeProvider.class, el);

        provider.addConstructorArgValue(registerInnerBean(cfg, ctx));

        return registerInnerBean(provider, ctx);
    }

    private RuntimeBeanReference parseMulticastSeedNodeProvider(Element el, ParserContext ctx) {
        BeanDefinitionBuilder cfg = newBean(MulticastSeedNodeProviderConfig.class, el);

        setProperty(cfg, el, "group", "group");
        setProperty(cfg, el, "port", "port");
        setProperty(cfg, el, "ttl", "ttl");
        setProperty(cfg, el, "interval", "interval-ms");
        setProperty(cfg, el, "waitTime", "wait-time-ms");
        setProperty(cfg, el, "loopBackDisabled", "loopback-disabled");

        BeanDefinitionBuilder provider = newBean(MulticastSeedNodeProvider.class, el);

        provider.addConstructorArgValue(registerInnerBean(cfg, ctx));

        return registerInnerBean(provider, ctx);
    }

    private RuntimeBeanReference parseSeedNodeProviderGroup(Element el, ParserContext ctx) {
        BeanDefinitionBuilder cfg = newBean(SeedNodeProviderGroupConfig.class, el);

        ManagedList<RuntimeBeanReference> providers = new ManagedList<>();

        getChildElements(el).forEach(childEl ->
            providers.add(parseSeedNodeProviderChild(childEl, ctx))
        );

        setProperty(cfg, el, "policy", "policy");

        cfg.addPropertyValue("providers", providers);

        BeanDefinitionBuilder provider = newBean(SeedNodeProviderGroup.class, el);

        provider.addConstructorArgValue(registerInnerBean(cfg, ctx));

        return registerInnerBean(provider, ctx);
    }

    private Optional<RuntimeBeanReference> parseCloudProviderCredentials(Element cloudEl, ParserContext ctx) {
        String providerName = cloudEl.getAttribute("provider").trim();

        Element credEl = getChildElementByTagName(cloudEl, "credentials");

        RuntimeBeanReference credRef = null;

        if (credEl != null) {
            Element basicCredEl = getChildElementByTagName(credEl, "basic");

            if (basicCredEl != null) {
                BeanDefinitionBuilder supplier;

                if (providerName.contains("aws")) {
                    supplier = newBean(AwsCredentialsSupplier.class, basicCredEl);
                } else {
                    supplier = newBean(BasicCredentialsSupplier.class, basicCredEl);
                }

                setProperty(supplier, basicCredEl, "identity", "identity");
                setProperty(supplier, basicCredEl, "credential", "credential");

                credRef = registerInnerBean(supplier, ctx);
            } else {
                credRef = parseRefOrBean(getChildElementByTagName(credEl, "custom-supplier"), ctx).orElse(null);
            }
        }

        if (credRef == null && providerName.contains("aws")) {
            credRef = registerInnerBean(newBean(AwsCredentialsSupplier.class, cloudEl), ctx);
        }

        return Optional.ofNullable(credRef);
    }

    private void parseClusterFailureDetection(BeanDefinitionBuilder cluster, Element clusterEl, ParserContext ctx) {
        Element detectionEl = getChildElementByTagName(clusterEl, "failure-detection");

        if (detectionEl != null) {
            Element heartbeatEl = getChildElementByTagName(detectionEl, "heartbeat");

            if (heartbeatEl != null) {
                BeanDefinitionBuilder heartbeatCfg = newBean(DefaultFailureDetectorConfig.class, heartbeatEl);

                setProperty(heartbeatCfg, heartbeatEl, "heartbeatInterval", "interval-ms");
                setProperty(heartbeatCfg, heartbeatEl, "heartbeatLossThreshold", "loss-threshold");
                setProperty(heartbeatCfg, heartbeatEl, "failureDetectionQuorum", "quorum");

                BeanDefinitionBuilder heartbeat = newBean(DefaultFailureDetector.class, heartbeatEl);

                heartbeat.addConstructorArgValue(registerInnerBean(heartbeatCfg, ctx));

                cluster.addPropertyValue("failureDetector", registerInnerBean(heartbeat, ctx));
            } else {
                setBeanOrRef(cluster, detectionEl, "failureDetector", "custom-detector", ctx);
            }
        }
    }

    private void parseClusterSplitBrainDetection(BeanDefinitionBuilder cluster, Element clusterEl, ParserContext ctx) {
        Element splitBrainEl = getChildElementByTagName(clusterEl, "split-brain-detection");

        if (splitBrainEl != null) {
            setProperty(cluster, splitBrainEl, "splitBrainAction", "action");

            Element groupEl = getChildElementByTagName(splitBrainEl, "group");

            if (groupEl != null) {
                cluster.addPropertyValue("splitBrainDetector", parseClusterSplitBrainGroup(groupEl, ctx));
            }
        }
    }

    private RuntimeBeanReference parseClusterSplitBrainGroup(Element groupEl, ParserContext ctx) {
        List<Element> hostEls = getChildElementsByTagName(groupEl, "host-reachable");
        List<Element> addressEls = getChildElementsByTagName(groupEl, "address-reachable");
        List<Element> customEls = getChildElementsByTagName(groupEl, "custom-detector");
        List<Element> nestedGroupEls = getChildElementsByTagName(groupEl, "group");

        ManagedList<RuntimeBeanReference> detectors = new ManagedList<>();

        for (Element hostEl : hostEls) {
            BeanDefinitionBuilder detector = newBean(HostReachabilityDetector.class, hostEl);

            String host = hostEl.getAttribute("host").trim();
            String timeout = hostEl.getAttribute("timeout").trim();

            detector.addConstructorArgValue(host);

            if (!timeout.isEmpty()) {
                detector.addConstructorArgValue(timeout);
            }

            detectors.add(registerInnerBean(detector, ctx));
        }

        for (Element addressEl : addressEls) {
            BeanDefinitionBuilder detector = newBean(AddressReachabilityDetector.class, addressEl);

            String address = addressEl.getAttribute("address").trim();
            String timeout = addressEl.getAttribute("timeout").trim();

            detector.addConstructorArgValue(address);

            if (!timeout.isEmpty()) {
                detector.addConstructorArgValue(timeout);
            }

            detectors.add(registerInnerBean(detector, ctx));
        }

        for (Element customEl : customEls) {
            parseRefOrBean(customEl, ctx).ifPresent(detectors::add);
        }

        detectors.addAll(nestedGroupEls.stream()
            .map(nestedGroupEl -> parseClusterSplitBrainGroup(nestedGroupEl, ctx))
            .collect(Collectors.toList())
        );

        BeanDefinitionBuilder group = newBean(SplitBrainDetectorGroup.class, groupEl);

        setProperty(group, groupEl, "groupPolicy", "require");

        if (!detectors.isEmpty()) {
            group.addPropertyValue("detectors", detectors);
        }

        return registerInnerBean(group, ctx);
    }

    private void parseClusterAcceptors(BeanDefinitionBuilder cluster, Element clusterEl, ParserContext ctx) {
        ManagedList<RuntimeBeanReference> acceptors = new ManagedList<>();

        subElements(clusterEl, "acceptors", "acceptor").forEach(valEl ->
            parseRefOrBean(valEl, ctx).ifPresent(acceptors::add)
        );

        if (!acceptors.isEmpty()) {
            cluster.addPropertyValue("acceptors", acceptors);
        }
    }

    private void parseClusterListeners(BeanDefinitionBuilder cluster, Element clusterEl, ParserContext ctx) {
        ManagedList<RuntimeBeanReference> listeners = new ManagedList<>();

        subElements(clusterEl, "listeners", "listener").forEach(valEl ->
            parseRefOrBean(valEl, ctx).ifPresent(listeners::add)
        );

        if (!listeners.isEmpty()) {
            cluster.addPropertyValue("clusterListeners", listeners);
        }
    }

    private Optional<RuntimeBeanReference> parseNetworkService(Element rootEl, ParserContext ctx) {
        Element netEl = getChildElementByTagName(rootEl, "network");

        if (netEl != null) {
            BeanDefinitionBuilder net = newBean(NetworkServiceFactory.class, netEl);

            setProperty(net, netEl, "host", "host");
            setProperty(net, netEl, "port", "port");
            setProperty(net, netEl, "portRange", "port-range");
            setProperty(net, netEl, "connectTimeout", "connect-timeout-ms");
            setProperty(net, netEl, "acceptRetryInterval", "accept-retry-interval-ms");
            setProperty(net, netEl, "heartbeatInterval", "heartbeat-interval-ms");
            setProperty(net, netEl, "heartbeatLossThreshold", "heartbeat-loss-threshold");
            setProperty(net, netEl, "nioThreads", "nio-threads");
            setProperty(net, netEl, "transport", "transport");
            setProperty(net, netEl, "tcpNoDelay", "tcp-no-delay");
            setProperty(net, netEl, "tcpReceiveBufferSize", "tcp-receive-buffer-size");
            setProperty(net, netEl, "tcpSendBufferSize", "tcp-send-buffer-size");
            setProperty(net, netEl, "tcpReuseAddress", "tcp-reuse-address");
            setProperty(net, netEl, "tcpBacklog", "tcp-backlog");

            setBeanOrRef(net, netEl, "hostSelector", "host-selector", ctx);

            ManagedList<RuntimeBeanReference> connectors = new ManagedList<>();

            for (Element connEl : subElements(netEl, "connectors", "connector")) {
                BeanDefinitionBuilder conn = newBean(NetworkConnectorConfig.class, connEl);

                setProperty(conn, connEl, "protocol", "protocol");
                setProperty(conn, connEl, "idleSocketTimeout", "idle-socket-timeout-ms");
                setProperty(conn, connEl, "nioThreads", "nio-threads");
                setProperty(conn, connEl, "logCategory", "log-category");

                setBeanOrRef(conn, connEl, "serverHandler", "server-handler", ctx);
                setBeanOrRef(conn, connEl, "messageCodec", "message-codec", ctx);

                connectors.add(registerInnerBean(conn, ctx));

                String protocol = connEl.getAttribute("protocol");

                if (!protocol.isEmpty()) {
                    BeanDefinitionBuilder connBean = newLazyBean(NetworkConnectorBean.class, netEl);

                    setProperty(connBean, connEl, "protocol", "protocol");

                    deferredBaseBeans.put(connBean, protocol);
                }
            }

            if (!connectors.isEmpty()) {
                net.addPropertyValue("connectors", connectors);
            }

            String id = netEl.getAttribute("id");

            if (!id.isEmpty()) {
                deferredBaseBeans.put(newLazyBean(NetworkServiceBean.class, netEl), id);
            }

            return Optional.of(registerInnerBean(net, ctx));
        } else {
            return Optional.empty();
        }
    }

    private Optional<RuntimeBeanReference> parseMessagingService(Element rootEl, ParserContext ctx) {
        Element msgEl = getChildElementByTagName(rootEl, "messaging");

        if (msgEl != null) {
            BeanDefinitionBuilder msg = newBean(MessagingServiceFactory.class, msgEl);

            ManagedList<RuntimeBeanReference> channels = new ManagedList<>();

            for (Element channelEl : getChildElementsByTagName(msgEl, "channel")) {
                BeanDefinitionBuilder channel = newBean(MessagingChannelConfig.class, channelEl);

                // Channel type.
                String type = channelEl.getAttribute("base-type").trim();

                if (!type.isEmpty()) {
                    channel.addConstructorArgValue(type);
                }

                // Attributes.
                setProperty(channel, channelEl, "name", "name");
                setProperty(channel, channelEl, "workerThreads", "worker-threads");
                setProperty(channel, channelEl, "backupNodes", "backup-nodes");
                setProperty(channel, channelEl, "partitions", "partitions");
                setProperty(channel, channelEl, "logCategory", "log-category");
                setProperty(channel, channelEl, "messagingTimeout", "messaging-timeout-ms");

                // Nested elements.
                setBeanOrRef(channel, channelEl, "receiver", "receiver", ctx);
                setBeanOrRef(channel, channelEl, "loadBalancer", "load-balancer", ctx);
                setBeanOrRef(channel, channelEl, "messageCodec", "message-codec", ctx);
                setBeanOrRef(channel, channelEl, "clusterFilter", "cluster-filter", ctx);

                parseCommonMessagingConfig(channelEl, channel, ctx);

                // Register channel bean definition.
                channels.add(registerInnerBean(channel, ctx));

                String name = channelEl.getAttribute("name");

                if (!name.isEmpty()) {
                    BeanDefinitionBuilder channelBean = newLazyBean(MessagingChannelBean.class, channelEl);

                    setProperty(channelBean, channelEl, "channel", "name");

                    deferredBaseBeans.put(channelBean, name);
                }
            }

            if (!channels.isEmpty()) {
                msg.addPropertyValue("channels", channels);
            }

            String id = msgEl.getAttribute("id");

            if (!id.isEmpty()) {
                deferredBaseBeans.put(newLazyBean(MessagingServiceBean.class, msgEl), id);
            }

            return Optional.of(registerInnerBean(msg, ctx));
        } else {
            return Optional.empty();
        }
    }

    private void parseCommonMessagingConfig(Element el, BeanDefinitionBuilder bean, ParserContext ctx) {
        setProperty(bean, el, "nioThreads", "nio-threads");
        setProperty(bean, el, "idleSocketTimeout", "idle-socket-timeout-ms");

        // Back pressure element.
        parseMessagingBackPressure(el, ctx).ifPresent(backPressureRef ->
            bean.addPropertyValue("backPressure", backPressureRef)
        );
    }

    private Optional<RuntimeBeanReference> parseMessagingBackPressure(Element el, ParserContext ctx) {
        Element backPressureEl = getChildElementByTagName(el, "back-pressure");

        if (backPressureEl != null) {
            BeanDefinitionBuilder backPressure = newBean(MessagingBackPressureConfig.class, backPressureEl);

            Element outboundEl = getChildElementByTagName(backPressureEl, "outbound");
            Element inboundEl = getChildElementByTagName(backPressureEl, "inbound");

            if (outboundEl != null) {
                setProperty(backPressure, outboundEl, "outLowWatermark", "low-watermark");
                setProperty(backPressure, outboundEl, "outHighWatermark", "high-watermark");
                setProperty(backPressure, outboundEl, "outOverflowPolicy", "overflow");
            }

            if (inboundEl != null) {
                setProperty(backPressure, inboundEl, "inLowWatermark", "low-watermark");
                setProperty(backPressure, inboundEl, "inHighWatermark", "high-watermark");
            }

            return Optional.of(registerInnerBean(backPressure, ctx));
        } else {
            return Optional.empty();
        }
    }

    private Optional<RuntimeBeanReference> parseRpcService(Element rootEl, ParserContext ctx) {
        Element rpcEl = getChildElementByTagName(rootEl, "rpc");

        if (rpcEl != null) {
            BeanDefinitionBuilder rpc = newBean(RpcServiceFactory.class, rpcEl);

            parseCommonMessagingConfig(rpcEl, rpc, ctx);

            setProperty(rpc, rpcEl, "workerThreads", "worker-threads");

            // RPC clients.
            ManagedList<RuntimeBeanReference> clients = new ManagedList<>();

            subElements(rpcEl, "clients", "client").forEach(clientEl -> {
                BeanDefinitionBuilder client = newBean(RpcClientConfig.class, clientEl);

                // Attributes.
                setProperty(client, clientEl, "rpcInterface", "interface");
                setProperty(client, clientEl, "tag", "tag");
                setProperty(client, clientEl, "timeout", "timeout-ms");

                // Nested elements.
                setBeanOrRef(client, clientEl, "loadBalancer", "load-balancer", ctx);

                String name = clientEl.getAttribute("name");

                if (!name.isEmpty()) {
                    BeanDefinitionBuilder clientBean = newLazyBean(RpcClientBean.class, clientEl);

                    setProperty(clientBean, clientEl, "rpcInterface", "interface");
                    setProperty(clientBean, clientEl, "tag", "tag");

                    deferredBaseBeans.put(clientBean, name);
                }

                clients.add(registerInnerBean(client, ctx));
            });

            if (!clients.isEmpty()) {
                rpc.addPropertyValue("clients", clients);
            }

            // RPC servers.
            ManagedList<RuntimeBeanReference> servers = new ManagedList<>();

            subElements(rpcEl, "servers", "server").forEach(serverEl -> {
                BeanDefinitionBuilder server = newBean(RpcServerConfig.class, serverEl);

                // Attributes.
                setProperty(server, serverEl, "rpcInterface", "interface");
                setProperty(server, serverEl, "tag", "tag");
                setProperty(server, serverEl, "timeout", "timeout-ms");

                // Nested elements.
                setBeanOrRef(server, serverEl, "handler", "handler", ctx);

                ManagedSet<String> tags = new ManagedSet<>();

                subElements(serverEl, "tags", "tag").stream()
                    .map(DomUtils::getTextValue)
                    .filter(it -> !it.isEmpty())
                    .forEach(tags::add);

                if (!tags.isEmpty()) {
                    server.addPropertyValue("tags", tags);
                }

                servers.add(registerInnerBean(server, ctx));
            });

            if (!servers.isEmpty()) {
                rpc.addPropertyValue("servers", servers);
            }

            String id = rpcEl.getAttribute("id");

            if (!id.isEmpty()) {
                deferredBaseBeans.put(newLazyBean(RpcServiceBean.class, rpcEl), id);
            }

            return Optional.of(registerInnerBean(rpc, ctx));
        } else {
            return Optional.empty();
        }
    }

    private Optional<RuntimeBeanReference> parseLockService(Element rootEl, ParserContext ctx) {
        Element locksEl = getChildElementByTagName(rootEl, "locks");

        if (locksEl != null) {
            BeanDefinitionBuilder locks = newBean(LockServiceFactory.class, locksEl);

            setProperty(locks, locksEl, "retryInterval", "retry-interval-ms");
            setProperty(locks, locksEl, "nioThreads", "nio-threads");
            setProperty(locks, locksEl, "workerThreads", "worker-threads");

            ManagedList<RuntimeBeanReference> regions = new ManagedList<>();

            getChildElementsByTagName(locksEl, "region").forEach(regionEl -> {
                BeanDefinitionBuilder region = newBean(LockRegionConfig.class, regionEl);

                setProperty(region, regionEl, "name", "name");

                regions.add(registerInnerBean(region, ctx));

                String name = regionEl.getAttribute("name");

                if (!name.isEmpty()) {
                    BeanDefinitionBuilder regionBean = newLazyBean(LockRegionBean.class, regionEl);

                    setProperty(regionBean, regionEl, "region", "name");

                    deferredBaseBeans.put(regionBean, name);
                }

                getChildElementsByTagName(regionEl, "lock").forEach(lockEl -> {
                    String lockName = lockEl.getAttribute("name");

                    if (!lockName.isEmpty()) {
                        BeanDefinitionBuilder lockBean = newLazyBean(LockBean.class, lockEl);

                        setProperty(lockBean, regionEl, "region", "name");
                        setProperty(lockBean, lockEl, "name", "name");

                        deferredBaseBeans.put(lockBean, lockName);
                    }
                });
            });

            if (!regions.isEmpty()) {
                locks.addPropertyValue("regions", regions);
            }

            String id = locksEl.getAttribute("id");

            if (!id.isEmpty()) {
                deferredBaseBeans.put(newLazyBean(LockServiceBean.class, locksEl), id);
            }

            return Optional.of(registerInnerBean(locks, ctx));
        } else {
            return Optional.empty();
        }
    }

    private Optional<RuntimeBeanReference> parseCoordinationService(Element rootEl, ParserContext ctx) {
        Element coordinationEl = getChildElementByTagName(rootEl, "coordination");

        if (coordinationEl != null) {
            BeanDefinitionBuilder coordination = newBean(CoordinationServiceFactory.class, coordinationEl);

            setProperty(coordination, coordinationEl, "nioThreads", "nio-threads");
            setProperty(coordination, coordinationEl, "retryInterval", "retry-interval-ms");
            setProperty(coordination, coordinationEl, "idleSocketTimeout", "idle-socket-timeout-ms");

            ManagedList<RuntimeBeanReference> processes = new ManagedList<>();

            getChildElementsByTagName(coordinationEl, "process").forEach(processEl -> {
                BeanDefinitionBuilder process = newBean(CoordinationProcessConfig.class, processEl);

                setProperty(process, processEl, "name", "name");

                setBeanOrRef(process, processEl, "handler", "handler", ctx);
                setBeanOrRef(process, processEl, "messageCodec", "message-codec", ctx);

                processes.add(registerInnerBean(process, ctx));
            });

            if (!processes.isEmpty()) {
                coordination.addPropertyValue("processes", processes);
            }

            String id = coordinationEl.getAttribute("id");

            if (!id.isEmpty()) {
                deferredBaseBeans.put(newLazyBean(CoordinationServiceBean.class, coordinationEl), id);
            }

            return Optional.of(registerInnerBean(coordination, ctx));
        } else {
            return Optional.empty();
        }
    }

    private Optional<RuntimeBeanReference> parseElectionService(Element rootEl, ParserContext ctx) {
        Element leaderEl = getChildElementByTagName(rootEl, "election");

        if (leaderEl != null) {
            BeanDefinitionBuilder leader = newBean(ElectionServiceFactory.class, leaderEl);

            ManagedList<RuntimeBeanReference> candidates = new ManagedList<>();

            getChildElementsByTagName(leaderEl, "candidate").forEach(candidateEl -> {
                BeanDefinitionBuilder candidate = newBean(CandidateConfig.class, candidateEl);

                setProperty(candidate, candidateEl, "group", "group");

                parseRefOrBean(candidateEl, ctx).ifPresent(bean ->
                    candidate.addPropertyValue("candidate", bean)
                );

                candidates.add(registerInnerBean(candidate, ctx));
            });

            if (!candidates.isEmpty()) {
                leader.addPropertyValue("candidates", candidates);
            }

            String id = leaderEl.getAttribute("id");

            if (!id.isEmpty()) {
                deferredBaseBeans.put(newLazyBean(ElectionServiceBean.class, leaderEl), id);
            }

            return Optional.of(registerInnerBean(leader, ctx));
        } else {
            return Optional.empty();
        }
    }

    private RuntimeBeanReference registerInnerBean(BeanDefinitionBuilder def, ParserContext ctx) {
        String name = ctx.getReaderContext().generateBeanName(def.getRawBeanDefinition());

        ctx.registerBeanComponent(new BeanComponentDefinition(def.getBeanDefinition(), name));

        return new RuntimeBeanReference(name);
    }

    private void setProperty(BeanDefinitionBuilder def, Element el, String beanProperty, String xmlAttribute) {
        String val = el.getAttribute(xmlAttribute).trim();

        if (!val.isEmpty()) {
            def.addPropertyValue(beanProperty, val);
        }
    }

    private List<Element> subElements(Element root, String name, String subName) {
        Element elem = getChildElementByTagName(root, name);

        if (elem != null) {
            return getChildElementsByTagName(elem, subName);
        }

        return Collections.emptyList();
    }

    private Optional<Element> setBeanOrRef(BeanDefinitionBuilder def, Element parent, String propName, String elemName, ParserContext ctx) {
        Element elem = getChildElementByTagName(parent, elemName);

        parseRefOrBean(elem, ctx).ifPresent(ref ->
            def.addPropertyValue(propName, ref)
        );

        return Optional.ofNullable(elem);
    }

    private Optional<Map<String, String>> parseProperties(Element el) {
        List<Element> propEls = subElements(el, "properties", "prop");

        if (!propEls.isEmpty()) {
            Map<String, String> props = new ManagedMap<>();

            for (Element propEl : propEls) {
                String name = propEl.getAttribute("name").trim();
                String value = getTextValue(propEl).trim();

                props.put(name, value);
            }

            return Optional.of(props);
        }

        return Optional.empty();
    }

    private Optional<RuntimeBeanReference> parseRefOrBean(Element elem, ParserContext ctx) {
        if (elem != null) {
            String ref = elem.getAttribute("ref").trim();

            Element beanEl = getChildElementByTagName(elem, "bean");

            if (!ref.isEmpty() && beanEl != null) {
                String name = elem.getLocalName();

                ctx.getReaderContext().error('<' + name + ">'s 'ref' attribute can't be mixed with nested <bean> element.", elem);
            } else if (!ref.isEmpty()) {
                return Optional.of(new RuntimeBeanReference(ref));
            } else if (beanEl != null) {
                BeanDefinitionHolder holder = ctx.getDelegate().parseBeanDefinitionElement(beanEl);

                if (holder != null) {
                    ctx.registerBeanComponent(new BeanComponentDefinition(holder.getBeanDefinition(), holder.getBeanName()));

                    return Optional.of(new RuntimeBeanReference(holder.getBeanName()));
                }
            }
        }

        return Optional.empty();
    }

    private BeanDefinitionBuilder newLazyBean(Class<?> type, Element element) {
        return newBean(type, element).setLazyInit(true);
    }

    private BeanDefinitionBuilder newBean(Class<?> type, Element element) {
        BeanDefinitionBuilder def = BeanDefinitionBuilder.genericBeanDefinition(type);

        def.getRawBeanDefinition().setSource(element);

        return def;
    }
}
