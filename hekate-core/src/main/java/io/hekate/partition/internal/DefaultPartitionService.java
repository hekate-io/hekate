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

package io.hekate.partition.internal;

import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterView;
import io.hekate.cluster.event.ClusterEvent;
import io.hekate.core.HekateException;
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
import io.hekate.partition.PartitionConfigProvider;
import io.hekate.partition.PartitionMapper;
import io.hekate.partition.PartitionMapperConfig;
import io.hekate.partition.PartitionService;
import io.hekate.partition.PartitionServiceFactory;
import io.hekate.util.StampedStateGuard;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultPartitionService implements PartitionService, DependentService, ConfigurableService, InitializingService,
    TerminatingService {
    private static final Logger log = LoggerFactory.getLogger(DefaultPartitionService.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final StampedStateGuard guard = new StampedStateGuard(PartitionService.class);

    private final Map<String, DefaultPartitionMapper> mappers = new HashMap<>();

    private final List<PartitionMapperConfig> mappersConfig = new LinkedList<>();

    private ClusterService cluster;

    public DefaultPartitionService(PartitionServiceFactory factory) {
        assert factory != null : "Factory is null.";

        Utils.nullSafe(factory.getMappers()).forEach(mappersConfig::add);

        Utils.nullSafe(factory.getConfigProviders()).forEach(provider ->
            Utils.nullSafe(provider.configurePartitions()).forEach(mappersConfig::add)
        );
    }

    @Override
    public void resolve(DependencyContext ctx) {
        cluster = ctx.require(ClusterService.class);
    }

    @Override
    public void configure(ConfigurationContext ctx) {
        // Collect configurations from providers.
        Collection<PartitionConfigProvider> providers = ctx.findComponents(PartitionConfigProvider.class);

        Utils.nullSafe(providers).forEach(provider -> {
            Collection<PartitionMapperConfig> regions = provider.configurePartitions();

            Utils.nullSafe(regions).forEach(mappersConfig::add);
        });

        // Validate configs.
        ConfigCheck check = ConfigCheck.get(PartitionMapperConfig.class);

        Set<String> uniqueNames = new HashSet<>();

        mappersConfig.forEach(cfg -> {
            check.notEmpty(cfg.getName(), "name");
            check.positive(cfg.getPartitions(), "partitions");
            check.isPowerOfTwo(cfg.getPartitions(), "partitions");

            String name = cfg.getName().trim();

            check.unique(name, uniqueNames, "name");

            uniqueNames.add(name);
        });
    }

    @Override
    public void initialize(InitializationContext ctx) throws HekateException {
        if (DEBUG) {
            log.debug("Initializing...");
        }

        long writeLock = guard.lockWrite();

        try {
            guard.becomeInitialized();

            if (!mappersConfig.isEmpty()) {
                mappersConfig.forEach(this::doRegisterMapper);

                cluster.addListener(this::updateTopology);
            }
        } finally {
            guard.unlockWrite(writeLock);
        }

        if (DEBUG) {
            log.debug("Initialized.");
        }
    }

    @Override
    public void terminate() throws HekateException {
        if (DEBUG) {
            log.debug("Terminating...");
        }

        long writeLock = guard.lockWrite();

        try {
            if (guard.becomeTerminated()) {
                mappers.clear();
            }
        } finally {
            guard.unlockWrite(writeLock);
        }

        if (DEBUG) {
            log.debug("Terminated");
        }
    }

    @Override
    public List<PartitionMapper> allMappers() {
        long readLock = guard.lockReadWithStateCheck();

        try {
            return new ArrayList<>(mappers.values());
        } finally {
            guard.unlockRead(readLock);
        }
    }

    @Override
    public PartitionMapper mapper(String name) {
        PartitionMapper mapper;

        long readLock = guard.lockReadWithStateCheck();

        try {
            mapper = mappers.get(name);
        } finally {
            guard.unlockRead(readLock);
        }

        ArgAssert.check(mapper != null, "No such mapper [name=" + name + ']');

        return mapper;
    }

    @Override
    public boolean hasMapper(String name) {
        long readLock = guard.lockReadWithStateCheck();

        try {
            return mappers.containsKey(name);
        } finally {
            guard.unlockRead(readLock);
        }
    }

    private PartitionMapper doRegisterMapper(PartitionMapperConfig cfg) {
        if (DEBUG) {
            log.debug("Registering mapper [config={}]", cfg);
        }

        String name = cfg.getName().trim();
        int partitions = cfg.getPartitions();
        int backupNodes = cfg.getBackupNodes();
        ClusterNodeFilter filter = cfg.getFilter();

        ClusterView clusterView = filter == null ? cluster : cluster.filter(filter);

        DefaultPartitionMapper mapper = new DefaultPartitionMapper(name, partitions, Math.max(0, backupNodes), clusterView);

        mappers.put(name, mapper);

        return mapper;
    }

    private void updateTopology(ClusterEvent event) {
        if (DEBUG) {
            log.debug("Updating mappers for the cluster event [event={}]", event);
        }

        long readLock = guard.lockRead();

        try {
            if (guard.isInitialized()) {
                for (DefaultPartitionMapper mapper : mappers.values()) {
                    mapper.update();
                }
            }
        } finally {
            guard.unlockRead(readLock);
        }
    }

    @Override
    public String toString() {
        return PartitionService.class.getSimpleName() + "[mappers=" + Utils.toString(mappersConfig, PartitionMapperConfig::getName) + ']';
    }
}
