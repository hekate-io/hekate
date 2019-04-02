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

package io.hekate.cluster.internal;

import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.network.NetworkService;
import io.hekate.util.async.AsyncUtils;
import io.hekate.util.async.Waiting;
import java.net.InetSocketAddress;
import java.util.Collections;
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

class SeedNodeManager {
    interface AliveAddressProvider {
        Set<InetSocketAddress> getAliveAddresses();
    }

    private static final Logger log = LoggerFactory.getLogger(SeedNodeManager.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final SeedNodeProvider provider;

    private final String cluster;

    private final Object cleanupMux = new Object();

    private final long cleanupInterval;

    private final AtomicBoolean started = new AtomicBoolean();

    private ScheduledExecutorService cleaner;

    private NetworkService net;

    private AliveAddressProvider aliveAddressProvider;

    public SeedNodeManager(String cluster, SeedNodeProvider provider) {
        assert provider != null : "Provider is null.";

        this.cluster = cluster;
        this.provider = provider;
        this.cleanupInterval = provider.cleanupInterval();
    }

    public List<InetSocketAddress> getSeedNodes() throws HekateException {
        if (DEBUG) {
            log.debug("Getting seed nodes to join....");
        }

        List<InetSocketAddress> nodes = provider.findSeedNodes(cluster);

        nodes = nodes != null ? nodes : Collections.emptyList();

        if (DEBUG) {
            log.debug("Got a seed nodes list [seed-nodes={}]", nodes);
        }

        return nodes;
    }

    public void startDiscovery(InetSocketAddress address) throws HekateException {
        if (DEBUG) {
            log.debug("Starting seed nodes discovery [cluster={}, address={}]", cluster, address);
        }

        started.set(true);

        provider.startDiscovery(cluster, address);

        if (DEBUG) {
            log.debug("Started seed nodes discovery [cluster={}, address={}]", cluster, address);
        }
    }

    public void suspendDiscovery() {
        if (DEBUG) {
            log.debug("Suspending seed nodes discovery...");
        }

        try {
            provider.suspendDiscovery();

            if (DEBUG) {
                log.debug("Suspended seed nodes discovery.");
            }
        } catch (Throwable t) {
            log.error("Failed to suspend seed nodes discovery.", t);
        }
    }

    public void stopDiscovery(InetSocketAddress address) {
        if (started.compareAndSet(true, false)) {
            try {
                if (DEBUG) {
                    log.debug("Stopping seed nodes discovery [cluster={}, address={}]", cluster, address);
                }

                provider.stopDiscovery(cluster, address);

                if (DEBUG) {
                    log.debug("Done stopping seed nodes discovery [cluster={}, address={}]", cluster, address);
                }
            } catch (Throwable t) {
                log.error("Failed to stop seed nodes discovery [cluster={}, address={}]", cluster, address, t);
            }
        }
    }

    public void startCleaning(NetworkService net, AliveAddressProvider aliveAddressProvider) {
        assert net != null : "Network service is null.";
        assert aliveAddressProvider != null : "Alive address provider is null.";

        synchronized (cleanupMux) {
            if (cleaner == null && cleanupInterval > 0) {
                if (DEBUG) {
                    log.debug("Scheduling seed nodes cleanup task [cluster={}, interval={}]", cluster, cleanupInterval);
                }

                this.net = net;
                this.aliveAddressProvider = aliveAddressProvider;

                Set<InetSocketAddress> pingInProgress = Collections.synchronizedSet(new HashSet<>());

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
                    log.debug("Canceling seed nodes cleanup task [cluster={}]", cluster);
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
                log.debug("Running seed nodes cleanup task [cluster={}]", cluster);
            }

            List<InetSocketAddress> provided = provider.findSeedNodes(cluster);

            Set<InetSocketAddress> seeds;

            if (provided == null || provided.isEmpty()) {
                seeds = Collections.emptySet();
            } else {
                seeds = new HashSet<>(provided);
            }

            Set<InetSocketAddress> alive = aliveAddressProvider.getAliveAddresses();

            // Make sure that all known addresses are really registered.
            for (InetSocketAddress addr : alive) {
                if (!seeds.contains(addr)) {
                    if (DEBUG) {
                        log.debug("Re-registering the missing seed node address [cluster={}, address={}]", cluster, addr);
                    }

                    provider.registerRemote(cluster, addr);
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

                            // Synchronize only for an asynchronous task submission.
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
                                            Set<InetSocketAddress> currentAlive = aliveAddressProvider.getAliveAddresses();

                                            if (currentAlive.contains(addr)) {
                                                if (DEBUG) {
                                                    log.debug("Skipped seed node address cleaning since it is alive [address={}]", addr);
                                                }

                                                return;
                                            }

                                            if (DEBUG) {
                                                log.debug("Unregistering failed seed node address [cluster={}, address={}]", cluster, addr);
                                            }

                                            provider.unregisterRemote(cluster, addr);
                                        } catch (HekateException e) {
                                            if (log.isWarnEnabled()) {
                                                log.warn("Failed to unregister failed seed node address "
                                                    + "[cluster={}, address={}]", cluster, addr, e);
                                            }
                                        } catch (RuntimeException | Error e) {
                                            log.error("Got an unexpected runtime error while unregistering failed seed node address "
                                                + "[address={}]", addr, e);
                                        }
                                    });
                                }
                            }

                            break;
                        }
                        case SUCCESS: {
                            // Node is alive. Do nothing.
                            break;
                        }
                        case TIMEOUT: {
                            // We can't decide whether node is alive or not. Do nothing.
                            break;
                        }
                        default: {
                            throw new IllegalArgumentException("Unexpected result: " + result);
                        }
                    }
                });
            }

            if (DEBUG) {
                log.debug("Done running seed nodes cleanup task [cluster={}]", cluster);
            }
        } catch (HekateException e) {
            log.warn("Failed to cleanup stale seed nodes.", e);
        } catch (RuntimeException | Error e) {
            log.error("Got an unexpected runtime error while cleaning stale stale seed nodes.", e);
        }
    }

    @Override
    public String toString() {
        return provider.toString();
    }
}
