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

package io.hekate.cluster.seed.multicast;

import io.hekate.util.format.ToString;

/**
 * Configuration for {@link MulticastSeedNodeProvider}.
 *
 * @see MulticastSeedNodeProvider
 */
public class MulticastSeedNodeProviderConfig {
    /** Default value (={@value}) for {@link #setGroup(String)}. */
    public static final String DEFAULT_GROUP = "224.1.2.12";

    /** Default value (={@value}) for {@link #setPort(int)}. */
    public static final int DEFAULT_PORT = 45454;

    /** Default value (={@value}) for {@link #setTtl(int)}. */
    public static final int DEFAULT_TTL = 3;

    /** Default value (={@value}) for {@link #setInterval(long)}. */
    public static final int DEFAULT_INTERVAL = 200;

    /** Default value (={@value}) for {@link #setWaitTime(long)}. */
    public static final int DEFAULT_WAIT_TIME = 1000;

    /** Default value (={@value}) for {@link #setLoopBackDisabled(boolean)}. */
    public static final boolean DEFAULT_LOOP_BACK_DISABLED = false;

    private String group = DEFAULT_GROUP;

    private int port = DEFAULT_PORT;

    private int ttl = DEFAULT_TTL;

    private long interval = DEFAULT_INTERVAL;

    private long waitTime = DEFAULT_WAIT_TIME;

    private boolean loopBackDisabled = DEFAULT_LOOP_BACK_DISABLED;

    /**
     * Returns the multicast group address (see {@link #setGroup(String)}).
     *
     * @return Multicast group address.
     */
    public String getGroup() {
        return group;
    }

    /**
     * Sets the multicast group address.
     *
     * <p>
     * Must be a valid multicast address as described <a href="https://en.wikipedia.org/wiki/Multicast_address" target="_blank">here</a>.
     * </p>
     *
     * <p>
     * Default value of this parameter is {@value #DEFAULT_GROUP}.
     * </p>
     *
     * @param group Multicast group address.
     */
    public void setGroup(String group) {
        this.group = group;
    }

    /**
     * Fluent-style version of {@link #setGroup(String)}.
     *
     * @param group Multicast group address.
     *
     * @return This instance.
     */
    public MulticastSeedNodeProviderConfig withGroup(String group) {
        setGroup(group);

        return this;
    }

    /**
     * Returns the multicast port (see {@link #setPort(int)}).
     *
     * @return Multicast port.
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets the multicast port.
     *
     * <p>
     * Default value of this parameter is {@value #DEFAULT_PORT}.
     * </p>
     *
     * @param port Multicast port.
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Fluent-style version of {@link #setPort(int)}.
     *
     * @param port Multicast port.
     *
     * @return This instance.
     */
    public MulticastSeedNodeProviderConfig withPort(int port) {
        setPort(port);

        return this;
    }

    /**
     * Returns the multicast TTL (see {@link #setTtl(int)}).
     *
     * @return Multicast TTL.
     */
    public int getTtl() {
        return ttl;
    }

    /**
     * Sets the multicast TTL (<a href="https://en.wikipedia.org/wiki/Time_to_live" target="_blank">Time To Live</a>).
     *
     * <p>
     * Default value of this parameter is {@value #DEFAULT_TTL}.
     * </p>
     *
     * @param ttl Multicast TTL (see <a href="https://en.wikipedia.org/wiki/Time_to_live" target="_blank">Time To Live</a>).
     */
    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    /**
     * Fluent-style version of {@link #setTtl(int)}.
     *
     * @param ttl Multicast TTL.
     *
     * @return This instance.
     */
    public MulticastSeedNodeProviderConfig withTtl(int ttl) {
        setTtl(ttl);

        return this;
    }

    /**
     * Returns the timeout in millisecond to await for responses from remote nodes (see {@link #setWaitTime(long)}).
     *
     * @return Timeout in milliseconds.
     */
    public long getWaitTime() {
        return waitTime;
    }

    /**
     * Sets the timeout in millisecond to await for responses from remote nodes.
     *
     * <p>
     * Default value of this parameter is {@value #DEFAULT_WAIT_TIME}.
     * </p>
     *
     * @param waitTime Timeout in millisecond to await for responses from remote nodes.
     */
    public void setWaitTime(long waitTime) {
        this.waitTime = waitTime;
    }

    /**
     * Fluent-style version of {@link #setWaitTime(long)}.
     *
     * @param waitTime Timeout in millisecond to await for responses from remote nodes.
     *
     * @return This instance.
     */
    public MulticastSeedNodeProviderConfig withWaitTime(long waitTime) {
        setWaitTime(waitTime);

        return this;
    }

    /**
     * Returns the time interval in milliseconds between discovery messages multicasting (see {@link #setInterval(long)}).
     *
     * @return Time interval in milliseconds between discovery messages multicasting.
     */
    public long getInterval() {
        return interval;
    }

    /**
     * Sets the time interval in milliseconds between discovery messages multicasting.
     *
     * <p>
     * Default value of this parameter is {@value #DEFAULT_INTERVAL}.
     * </p>
     *
     * @param interval Time interval in milliseconds between discovery messages multicasting.
     */
    public void setInterval(long interval) {
        this.interval = interval;
    }

    /**
     * Fluent-style version of {@link #setInterval(long)}.
     *
     * @param interval Time interval in milliseconds between discovery messages multicasting.
     *
     * @return This instance.
     */
    public MulticastSeedNodeProviderConfig withInterval(long interval) {
        setInterval(interval);

        return this;
    }

    /**
     * Returns {@code true} if receiving of multicast messages on the loopback address should be disabled (see {@link
     * #setLoopBackDisabled(boolean)}).
     *
     * @return {@code true} if receiving of multicast messages on the loopback address should be disabled.
     */
    public boolean isLoopBackDisabled() {
        return loopBackDisabled;
    }

    /**
     * Sets the flag indicating whether receiving of multicast messages on the loop back address should be disabled or enabled.
     *
     * <p>
     * Default value of this parameter is {@value #DEFAULT_LOOP_BACK_DISABLED}.
     * </p>
     *
     * @param loopBackDisabled {@code true} if receiving of multicast messages on the loopback address should be disabled.
     */
    public void setLoopBackDisabled(boolean loopBackDisabled) {
        this.loopBackDisabled = loopBackDisabled;
    }

    /**
     * Fluent-style version of {@link #setLoopBackDisabled(boolean)}.
     *
     * @param loopBackDisabled {@code true} if receiving of multicast messages on the loopback address should be disabled.
     *
     * @return This instance.
     */
    public MulticastSeedNodeProviderConfig withLoopBackDisabled(boolean loopBackDisabled) {
        setLoopBackDisabled(loopBackDisabled);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
