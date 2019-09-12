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

package io.hekate.messaging;

import io.hekate.util.format.ToString;

/**
 * Back pressure configuration of a {@link MessagingChannel}.
 *
 * @see MessagingChannelConfig#setBackPressure(MessagingBackPressureConfig)
 */
public class MessagingBackPressureConfig {
    private int inLowWatermark;

    private int inHighWatermark;

    private int outLowWatermark;

    private int outHighWatermark;

    private MessagingOverflowPolicy outOverflowPolicy = MessagingOverflowPolicy.IGNORE;

    /**
     * Constructs a new instance.
     */
    public MessagingBackPressureConfig() {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param src Copy from.
     */
    public MessagingBackPressureConfig(MessagingBackPressureConfig src) {
        inLowWatermark = src.getInLowWatermark();
        inHighWatermark = src.getInHighWatermark();
        outLowWatermark = src.getOutLowWatermark();
        outHighWatermark = src.getOutHighWatermark();
        outOverflowPolicy = src.getOutOverflowPolicy();
    }

    /**
     * Returns the low watermark of inbound (receiving) queue size (see {@link #setInLowWatermark(int)}).
     *
     * @return Low watermark of inbound queue size.
     */
    public int getInLowWatermark() {
        return inLowWatermark;
    }

    /**
     * Sets the low watermark of inbound (receiving) queue size.
     *
     * <p>
     * Value of this parameter must be less than the value of {@link #setInHighWatermark(int)} parameter.
     * </p>
     *
     * @param inLowWatermark Low watermark of inbound queue size.
     *
     * @see #setInHighWatermark(int)
     */
    public void setInLowWatermark(int inLowWatermark) {
        this.inLowWatermark = inLowWatermark;
    }

    /**
     * Fluent-style version of {@link #setInLowWatermark(int)}.
     *
     * @param inLowWatermark Low watermark of inbound queue size.
     *
     * @return This instance.
     */
    public MessagingBackPressureConfig withInLowWatermark(int inLowWatermark) {
        setInLowWatermark(inLowWatermark);

        return this;
    }

    /**
     * Returns the high watermark of inbound (receiving) queue size (see {@link #setOutHighWatermark(int)}).
     *
     * @return High watermark of inbound queue size.
     */
    public int getInHighWatermark() {
        return inHighWatermark;
    }

    /**
     * Sets the high watermark of inbound (receiving) queue size.
     *
     * <p>
     * If value of this parameter is less than or equals to zero (default) then back pressure will not be applied to inbound message queue.
     * </p>
     *
     * @param inHighWatermark High watermark of inbound queue size.
     *
     * @see #setInLowWatermark(int)
     */
    public void setInHighWatermark(int inHighWatermark) {
        this.inHighWatermark = inHighWatermark;
    }

    /**
     * Fluent-style version of {@link #setInHighWatermark(int)}.
     *
     * @param inHighWatermark High watermark of inbound queue size.
     *
     * @return This instance.
     */
    public MessagingBackPressureConfig withInHighWatermark(int inHighWatermark) {
        setInHighWatermark(inHighWatermark);

        return this;
    }

    /**
     * Returns the low watermark of outbound (sending) queue size (see {@link #setOutLowWatermark(int)}).
     *
     * @return Low watermark of outbound queue size.
     */
    public int getOutLowWatermark() {
        return outLowWatermark;
    }

    /**
     * Sets the low watermark of outbound (sending) queue size.
     *
     * <p>
     * This parameter is mandatory only if {@link #setOutOverflowPolicy(MessagingOverflowPolicy)} is set to any other value besides
     * {@link MessagingOverflowPolicy#IGNORE}. Value of this parameter must be less than the value of {@link #setOutHighWatermark(int)}
     * parameter.
     * </p>
     *
     * @param outLowWatermark Low watermark of outbound queue size.
     *
     * @see #setOutHighWatermark(int)
     * @see #setOutOverflowPolicy(MessagingOverflowPolicy)
     */
    public void setOutLowWatermark(int outLowWatermark) {
        this.outLowWatermark = outLowWatermark;
    }

    /**
     * Fluent-style version of {@link #setOutLowWatermark(int)}.
     *
     * @param outLowWatermark Low watermark of outbound queue size.
     *
     * @return This instance.
     */
    public MessagingBackPressureConfig withOutLowWatermark(int outLowWatermark) {
        setOutLowWatermark(outLowWatermark);

        return this;
    }

    /**
     * Returns the high watermark of outbound (sending) queue size (see {@link #setOutHighWatermark(int)}).
     *
     * @return High watermark of outbound queue size.
     */
    public int getOutHighWatermark() {
        return outHighWatermark;
    }

    /**
     * Sets the high watermark of outbound (sending) queue size.
     *
     * <p>
     * This parameter is mandatory only if {@link #setOutOverflowPolicy(MessagingOverflowPolicy)} is set to any other value besides
     * {@link MessagingOverflowPolicy#IGNORE}.
     * </p>
     *
     * @param outHighWatermark High watermark of outbound queue size.
     *
     * @see #setOutLowWatermark(int)
     * @see #setOutOverflowPolicy(MessagingOverflowPolicy)
     */
    public void setOutHighWatermark(int outHighWatermark) {
        this.outHighWatermark = outHighWatermark;
    }

    /**
     * Fluent-style version of {@link #setOutHighWatermark(int)}.
     *
     * @param outHighWatermark High watermark of outbound queue size.
     *
     * @return This instance.
     */
    public MessagingBackPressureConfig withOutHighWatermark(int outHighWatermark) {
        setOutHighWatermark(outHighWatermark);

        return this;
    }

    /**
     * Returns the policy that should be applied when {@link MessagingChannel}'s outbound (sending) queue size exceeds the
     * {@link #setOutHighWatermark(int) limit} (see {@link #setOutOverflowPolicy(MessagingOverflowPolicy)}).
     *
     * @return Policy.
     */
    public MessagingOverflowPolicy getOutOverflowPolicy() {
        return outOverflowPolicy;
    }

    /**
     * Sets the policy that should be applied when {@link MessagingChannel}'s outbound (sending) queue size exceeds the
     * {@link #setOutHighWatermark(int) limit}.
     *
     * <p>
     * Default value of this parameter is {@link MessagingOverflowPolicy#IGNORE}. If any other value is specified then {@link
     * #setOutHighWatermark(int)} must be set to a value that is greater than zero.
     * </p>
     *
     * @param outOverflowPolicy Policy.
     */
    public void setOutOverflowPolicy(MessagingOverflowPolicy outOverflowPolicy) {
        this.outOverflowPolicy = outOverflowPolicy;
    }

    /**
     * Fluent-style version of {@link #setOutOverflowPolicy(MessagingOverflowPolicy)}.
     *
     * @param outOverflow Policy.
     *
     * @return This instance.
     */
    public MessagingBackPressureConfig withOutOverflowPolicy(MessagingOverflowPolicy outOverflow) {
        setOutOverflowPolicy(outOverflow);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
