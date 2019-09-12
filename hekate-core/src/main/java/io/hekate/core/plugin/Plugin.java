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

package io.hekate.core.plugin;

import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.HekateException;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.InitializingService;
import java.util.List;

/**
 * <span class="startHere">&laquo; start here</span>Entry point to core API of {@link Hekate} plugins.
 *
 * <h2>Overview</h2>
 * <p>
 * {@link Plugin} is a kind of a micro application who's lifecycle is bound to the lifecycle of a {@link Hekate} instance.
 * Each {@link Plugin} has full access to {@link Hekate} services API and can customize services configuration during its startup (f.e.
 * register messaging channels, cluster event listeners, custom metrics, etc) as well as register custom services.
 * </p>
 *
 * <h2>Registration</h2>
 * <p>
 * Plugins can be registered to a {@link Hekate} instance via {@link HekateBootstrap#setPlugins(List)}.
 * </p>
 *
 * <h2>Lifecycle</h2>
 * <ul>
 * <li>
 * When {@link Hekate} instance is created it calls {@link #install(HekateBootstrap)} method on all of its plugins in their registration
 * order. Plugins can use the provided {@link HekateBootstrap} instance to customize {@link Hekate} instance configuration. Note that
 * this method <b>gets called only once</b> during the whole lifetime of {@link Hekate} instance.
 * </li>
 * <li>
 * When {@link Hekate} instance starts {@link Hekate#join() joining} to a cluster it calls {@link Plugin#start(Hekate)} method on all of
 * its plugins in their registration order. This method gets called after services
 * {@link InitializingService#initialize(InitializationContext) initialization} but before actual joining to the cluster begins.
 * </li>
 * <li>
 * When {@link Hekate} instance starts {@link Hekate#leave()} leaving the cluster it calls {@link Plugin#stop()} method on all of its
 * plugins in the reveres order.
 * </li>
 * </ul>
 * <p>
 * <b>Note:</b> {@link Plugin} should be implemented in such a way so that its {@link #start(Hekate)} and {@link #stop()} methods could
 * be called multiple times on the same instance. Those methods are not required to be thread safe since {@link #stop()} is always called by
 * the same thread that called {@link #start(Hekate)}.
 * </p>
 *
 * <h2>Example</h2>
 * <p>
 * Below is a simple example of {@link Plugin} that manages cluster topology information within a text file:
 * ${source: plugin/PluginJavadocTest.java#plugin}
 * </p>
 * <p>
 * ... and register this plugin to a {@link Hekate} node:
 * ${source: plugin/PluginJavadocTest.java#register}
 * </p>
 *
 * @see HekateBootstrap#setPlugins(List)
 */
public interface Plugin {
    /**
     * Gets called when {@link Hekate} instance is constructed.
     * <p>
     * Implementation of this method can use the provided {@link HekateBootstrap} to modify {@link Hekate} instance configuration.
     * </p>
     *
     * @param boot {@link HekateBootstrap}.
     */
    void install(HekateBootstrap boot);

    /**
     * Called when {@link Hekate} instance is about to start {@link Hekate#join() joining} the cluster.
     *
     * @param hekate Instance that this plugin is bound to.
     *
     * @throws HekateException If plugin failed to start.
     */
    void start(Hekate hekate) throws HekateException;

    /**
     * Called before {@link Hekate} instance is about to {@link Hekate#leave() leave} the cluster.
     *
     * @throws HekateException If plugin failed to stop.
     */
    void stop() throws HekateException;
}
