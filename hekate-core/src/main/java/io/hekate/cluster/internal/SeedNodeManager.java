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

package io.hekate.cluster.internal;

import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.network.NetworkService;
import io.hekate.util.async.AsyncUtils;
import io.hekate.util.async.Waiting;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.synchronizedSet;

class SeedNodeManager {
    interface AliveAddressProvider {
        Set<InetSocketAddress> getAliveAddresses();
    }

    private static final Logger log = LoggerFactory.getLogger(SeedNodeManager.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final SeedNodeProvider provider;

    private final String namespace;

    private final Object cleanupMux = new Object();

    private final long cleanupInterval;

    private final boolean seedNodeFailFast;

    private final AtomicBoolean started = new AtomicBoolean();

    private ScheduledExecutorService cleaner;

    private NetworkService net;

    private AliveAddressProvider aliveAddressProvider;

    public SeedNodeManager(String namespace, SeedNodeProvider provider, boolean seedNodeFailFast) {
        this.namespace = namespace;
        this.provider = provider;
        this.seedNodeFailFast = seedNodeFailFast;
        this.cleanupInterval = provider.cleanupInterval();
    }

    public List<InetSocketAddress> getSeedNodes() throws HekateException {
        if (DEBUG) {
            log.debug("Getting seed nodes to join....");
        }

        List<InetSocketAddress> nodes = provider.findSeedNodes(namespace);

        nodes = nodes != null ? nodes : emptyList();

        if (DEBUG) {
            log.debug("Got a seed nodes list [seed-nodes={}]", nodes);
        }

        return nodes;
    }

    public void startDiscovery(InetSocketAddress address) throws HekateException {
        if (DEBUG) {
            log.debug("Starting discovery of seed nodes [namespace={}, address={}]", namespace, address);
        }

        started.set(true);

        provider.startDiscovery(namespace, address);

        if (DEBUG) {
            log.debug("Started discovery of seed nodes [namespace={}, address={}]", namespace, address);
        }
    }

    public void suspendDiscovery() {
        if (DEBUG) {
            log.debug("Suspending discovery of seed nodes...");
        }

        try {
            provider.suspendDiscovery();

            if (DEBUG) {
                log.debug("Suspended discovery of seed nodes.");
            }
        } catch (Throwable t) {
            log.error("Failed to suspend discovery of seed nodes.", t);
        }
    }

    public void stopDiscovery(InetSocketAddress address) {
        if (started.compareAndSet(true, false)) {
            try {
                if (DEBUG) {
                    log.debug("Stopping discovery of seed nodes [namespace={}, address={}]", namespace, address);
                }

                provider.stopDiscovery(namespace, address);

                if (DEBUG) {
                    log.debug("Done stopping discovery of seed nodes [namespace={}, address={}]", namespace, address);
                }
            } catch (Throwable t) {
                log.error("Failed to stop discovery of seed nodes [namespace={}, address={}]", namespace, address, t);
            }
        }
    }

    public void startCleaning(NetworkService net, AliveAddressProvider aliveAddressProvider) {
        synchronized (cleanupMux) {
            if (cleaner == null && cleanupInterval > 0) {
                if (DEBUG) {
                    log.debug("Scheduling seed nodes cleanup task [namespace={}, interval={}]", namespace, cleanupInterval);
                }

                this.net = net;
                this.aliveAddressProvider = aliveAddressProvider;

                Set<InetSocketAddress> pingInProgress = synchronizedSet(new HashSet<>());

                cleaner = Executors.newSingleThreadScheduledExecutor(new HekateThreadFactory("SeedNodeCleaner"));

                cleaner.scheduleWithFixedDelay(() -> doCleanup(pingInProgress), cleanupInterval, cleanupInterval, TimeUnit.MILLISECONDS);
            }
        }
    }

    public Waiting stopCleaning() {
        ExecutorService cleaner;

        synchronized (cleanupMux) {
            cleaner = this.cleaner;

            if (cleaner != null) {
                if (DEBUG) {
                    log.debug("Canceling seed nodes cleanup task [namespace={}]", namespace);
                }

                this.cleaner = null;
                this.net = null;
                this.aliveAddressProvider = null;
            }
        }

        return AsyncUtils.shutdown(cleaner);
    }

    private void doCleanup(Set<InetSocketAddress> pingInProgress) {
        try {
            NetworkService net;
            AliveAddressProvider aliveAddressProvider;

            synchronized (cleanupMux) {
                net = this.net;
                aliveAddressProvider = this.aliveAddressProvider;
            }

            if (net == null) {
                // Cleanup task was cancelled.
                return;
            }

            if (DEBUG) {
                log.debug("Running seed nodes cleanup task [namespace={}]", namespace);
            }

            List<InetSocketAddress> provided = provider.findSeedNodes(namespace);

            Set<InetSocketAddress> seeds;

            if (provided == null || provided.isEmpty()) {
                seeds = emptySet();
            } else {
                seeds = new HashSet<>(provided);
            }

            Set<InetSocketAddress> alive = aliveAddressProvider.getAliveAddresses();

            // Make sure that all known addresses are really registered.
            for (InetSocketAddress addr : alive) {
                if (!seeds.contains(addr)) {
                    if (DEBUG) {
                        log.debug("Re-registering the missing seed node address [namespace={}, address={}]", namespace, addr);
                    }

                    provider.registerRemote(namespace, addr);
                }
            }

            // Check seed nodes and unregister the failed ones.
            for (InetSocketAddress addr : seeds) {
                if (alive.contains(addr)) {
                    // Ignore address that is known to be alive.
                    continue;
                }

                if (pingInProgress.contains(addr)) {
                    // Prevent concurrent pinging of that same address.
                    continue;
                }

                pingInProgress.add(addr);

                net.ping(addr, (checkedAddr, result) -> {
                    pingInProgress.remove(addr);

                    switch (result) {
                        case FAILURE: {
                            if (DEBUG) {
                                log.debug("Failed seed node address detected [address={}]", addr);
                            }

                            tryUnregisterRemoteAddress(addr, aliveAddressProvider);

                            break;
                        }
                        case TIMEOUT: {
                            if (seedNodeFailFast) {
                                if (DEBUG) {
                                    log.debug("Timed out seed node address detected [address={}]", addr);
                                }

                                tryUnregisterRemoteAddress(addr, aliveAddressProvider);
                            }

                            break;
                        }
                        case SUCCESS: {
                            // Node is alive. Do nothing.
                            break;
                        }
                        default: {
                            throw new IllegalArgumentException("Unexpected result: " + result);
                        }
                    }
                });
            }

            if (DEBUG) {
                log.debug("Done running seed nodes cleanup task [namespace={}]", namespace);
            }
        } catch (Throwable e) {
            log.error("Got an error while cleaning stale seed nodes.", e);
        }
    }

    private void tryUnregisterRemoteAddress(InetSocketAddress addr, AliveAddressProvider aliveAddress) {
        // Synchronize only for the asynchronous task submission.
        synchronized (cleanupMux) {
            if (cleaner == null) {
                if (DEBUG) {
                    log.debug("Skipped seed node address cleaning since cleanup task was cancelled [address={}]", addr);
                }
            } else {
                cleaner.execute(() -> {
                    // We are not synchronized here anymore.
                    try {
                        // Double check that node is not alive.
                        Set<InetSocketAddress> currentAlive = aliveAddress.getAliveAddresses();

                        if (currentAlive.contains(addr)) {
                            if (DEBUG) {
                                log.debug("Skipped seed node address cleaning since it is alive [address={}]", addr);
                            }

                            return;
                        }

                        if (DEBUG) {
                            log.debug("Unregistering failed seed node address "
                                + "[namespace={}, address={}]", namespace, addr);
                        }

                        provider.unregisterRemote(namespace, addr);
                    } catch (Throwable e) {
                        if (log.isErrorEnabled()) {
                            log.error("Failed to unregister seed node address "
                                + "[namespace={}, address={}]", namespace, addr, e);
                        }
                    }
                });
            }
        }
    }

    @Override
    public String toString() {
        return provider.toString();
    }
}
