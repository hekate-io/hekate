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

package io.hekate.rpc;

import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.ServiceFactory;
import io.hekate.messaging.MessagingConfigBase;
import io.hekate.rpc.internal.DefaultRpcService;
import io.hekate.util.format.ToString;
import java.util.ArrayList;
import java.util.List;

/**
 * Factory for {@link RpcService}.
 *
 * <p>
 * This class represents a configurable factory for {@link RpcService}. Instances of this class can be
 * {@link HekateBootstrap#withService(ServiceFactory) registered} within the {@link HekateBootstrap} in order to customize options of the
 * {@link RpcService}.
 * </p>
 *
 * <p>
 * For more details about the Remote Procedure Call API and its capabilities please see the documentation of the {@link RpcService}
 * interface.
 * </p>
 */
public class RpcServiceFactory extends MessagingConfigBase<RpcServiceFactory> implements ServiceFactory<RpcService> {
    private List<RpcClientConfig> clients;

    private List<RpcClientConfigProvider> clientProviders;

    private List<RpcServerConfig> servers;

    private List<RpcServerConfigProvider> serverProviders;

    private int workerThreads = Runtime.getRuntime().availableProcessors();

    /**
     * Returns the list of RPC client configurations (see {@link #setClients(List)}).
     *
     * @return RPC clients configuration.
     */
    public List<RpcClientConfig> getClients() {
        return clients;
    }

    /**
     * Sets the list of RPC client configurations.
     *
     * @param clients RPC clients configuration.
     */
    public void setClients(List<RpcClientConfig> clients) {
        this.clients = clients;
    }

    /**
     * Fluent-style version of {@link #setClients(List)}.
     *
     * @param client RPC client configuration.
     *
     * @return This instance.
     */
    public RpcServiceFactory withClient(RpcClientConfig client) {
        if (this.clients == null) {
            this.clients = new ArrayList<>();
        }

        clients.add(client);

        return this;
    }

    /**
     * Returns the list of RPC client configuration providers (see {@link #setClientProviders(List)}).
     *
     * @return RPC client configuration providers.
     */
    public List<RpcClientConfigProvider> getClientProviders() {
        return clientProviders;
    }

    /**
     * Sets the list of RPC client configuration providers.
     *
     * @param clientProviders RPC client configuration providers.
     *
     * @see RpcClientConfigProvider
     */
    public void setClientProviders(List<RpcClientConfigProvider> clientProviders) {
        this.clientProviders = clientProviders;
    }

    /**
     * Fluent-style version of {@link #setClientProviders(List)}.
     *
     * @param clientProvider RPC client configuration provider.
     *
     * @return This instance.
     */
    public RpcServiceFactory withClientProvider(RpcClientConfigProvider clientProvider) {
        if (this.clientProviders == null) {
            this.clientProviders = new ArrayList<>();
        }

        this.clientProviders.add(clientProvider);

        return this;
    }

    /**
     * Returns the list of RPC server configurations (see {@link #setServers(List)}).
     *
     * @return RPC servers configuration.
     */
    public List<RpcServerConfig> getServers() {
        return servers;
    }

    /**
     * Sets the list of RPC server configurations.
     *
     * @param servers RPC servers configuration.
     */
    public void setServers(List<RpcServerConfig> servers) {
        this.servers = servers;
    }

    /**
     * Fluent-style version of {@link #setServers(List)}.
     *
     * @param server RPC server configuration.
     *
     * @return This instance.
     */
    public RpcServiceFactory withServer(RpcServerConfig server) {
        if (this.servers == null) {
            this.servers = new ArrayList<>();
        }

        servers.add(server);

        return this;
    }

    /**
     * Returns the list of RPC server configuration providers (see {@link #setServerProviders(List)}).
     *
     * @return RPC server configuration providers.
     */
    public List<RpcServerConfigProvider> getServerProviders() {
        return serverProviders;
    }

    /**
     * Sets the list of RPC server configuration providers.
     *
     * @param serverProviders RPC server configuration providers.
     *
     * @see RpcServerConfigProvider
     */
    public void setServerProviders(List<RpcServerConfigProvider> serverProviders) {
        this.serverProviders = serverProviders;
    }

    /**
     * Fluent-style version of {@link #setServerProviders(List)}.
     *
     * @param serverProvider RPC server configuration provider.
     *
     * @return This instance.
     */
    public RpcServiceFactory withServerProvider(RpcServerConfigProvider serverProvider) {
        if (this.serverProviders == null) {
            this.serverProviders = new ArrayList<>();
        }

        serverProviders.add(serverProvider);

        return this;
    }

    /**
     * Returns the worker thread pool size (see {@link #setWorkerThreads(int)}).
     *
     * @return Worker thread pool size
     */
    public int getWorkerThreads() {
        return workerThreads;
    }

    /**
     * Sets the worker thread pool size for RPCs execution.
     *
     * <p>
     * The thread pool of this size will be used by the {@link RpcService} to perform all RPCs executions and callbacks notification.
     * </p>
     *
     * <p>
     * If not directly specified then the number of available CPUs (see {@link Runtime#availableProcessors()}) will be used as the default
     * value. If zero value is specified then all RPCs will be executed directly on NIO threads (see {@link #setNioThreads(int)}).
     * </p>
     *
     * @param workerThreads Worker thread pool size.
     */
    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    /**
     * Fluent-style version of {@link #setWorkerThreads(int)}.
     *
     * @param workerThreads Worker thread pool size.
     *
     * @return This instance.
     */
    public RpcServiceFactory withWorkerThreads(int workerThreads) {
        setWorkerThreads(workerThreads);

        return this;
    }

    @Override
    public RpcService createService() {
        return new DefaultRpcService(this);
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
