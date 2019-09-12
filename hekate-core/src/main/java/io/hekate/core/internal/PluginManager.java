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

package io.hekate.core.internal;

import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.StreamUtils;
import io.hekate.core.plugin.Plugin;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;

class PluginManager {
    private static class PluginState {
        private final Plugin plugin;

        private boolean started;

        public PluginState(Plugin plugin) {
            this.plugin = plugin;
        }

        public void install(HekateBootstrap boot) {
            if (DEBUG) {
                log.debug("Installing [plugin={}]", plugin);
            }

            plugin.install(boot);
        }

        public void start(Hekate instance) throws HekateException {
            if (DEBUG) {
                log.debug("Starting [plugin={}]", plugin);
            }

            started = true;

            plugin.start(instance);
        }

        public void stop() {
            if (started) {
                if (DEBUG) {
                    log.debug("Stopping [plugin={}]", plugin);
                }

                try {
                    plugin.stop();
                } catch (HekateException e) {
                    log.error("Failed to stop plugin [plugin={}]", plugin, e);
                } finally {
                    started = false;
                }
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(PluginManager.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final List<PluginState> plugins;

    private final HekateBootstrap boot;

    public PluginManager(HekateBootstrap boot) {
        assert boot != null : "Bootstrap is null";

        this.boot = boot;

        this.plugins = StreamUtils.nullSafe(boot.getPlugins()).map(PluginState::new).collect(toList());
    }

    public void install() {
        if (DEBUG) {
            log.debug("Installing plugins...");
        }

        for (PluginState plugin : plugins) {
            plugin.install(boot);
        }

        if (DEBUG) {
            log.debug("Done installing plugins.");
        }
    }

    public void start(Hekate instance) throws HekateException {
        if (DEBUG) {
            log.debug("Starting plugins...");
        }

        for (PluginState plugin : plugins) {
            plugin.start(instance);
        }

        if (DEBUG) {
            log.debug("Done starting plugins.");
        }
    }

    public void stop() {
        if (DEBUG) {
            log.debug("Stopping plugins...");
        }

        plugins.forEach(PluginState::stop);

        if (DEBUG) {
            log.debug("Done stopping plugins.");
        }
    }

    // Package level for testing purposes.
    HekateBootstrap bootstrap() {
        return boot;
    }
}
