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

package io.hekate.cluster.split;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.util.format.ToString;
import java.util.ArrayList;
import java.util.List;

/**
 * Combination of multiple split-brain detectors.
 *
 * <p>
 * This class represents a combination of multiple {@link SplitBrainDetector}s. During each {@link #isValid(ClusterNode)} call this class
 * invokes each of its {@link #setDetectors(List) detectors} and uses a {@link #setGroupPolicy(GroupPolicy) group policy} to decide on
 * whether the whole group check was successful or failed.
 * </p>
 *
 * @see ClusterServiceFactory#setSplitBrainDetector(SplitBrainDetector)
 */
public class SplitBrainDetectorGroup implements SplitBrainDetector {
    /**
     * Group policy for {@link SplitBrainDetectorGroup}.
     *
     * @see SplitBrainDetectorGroup#setGroupPolicy(GroupPolicy)
     */
    public enum GroupPolicy {
        /**
         * If at least one of {@link SplitBrainDetectorGroup#setDetectors(List) detectors} reported success then the whole group check is
         * considered to be successful.
         */
        ANY_VALID,

        /**
         * If at least one of {@link SplitBrainDetectorGroup#setDetectors(List) detectors} reported failure then the whole group check is
         * considered to be failed.
         */
        ALL_VALID
    }

    private GroupPolicy groupPolicy = GroupPolicy.ANY_VALID;

    private List<SplitBrainDetector> detectors;

    @Override
    public boolean isValid(ClusterNode localNode) {
        if (detectors != null) {
            int valid = 0;
            int invalid = 0;

            for (SplitBrainDetector detector : detectors) {
                if (detector != null) {
                    if (detector.isValid(localNode)) {
                        valid++;
                    } else {
                        invalid++;
                    }
                }
            }

            switch (groupPolicy) {
                case ANY_VALID: {
                    return invalid == 0 || valid > 0;
                }
                case ALL_VALID: {
                    return invalid == 0;
                }
                default: {
                    throw new IllegalArgumentException("Unexpected policy: " + groupPolicy);
                }
            }
        }

        return true;
    }

    /**
     * Returns the list of registered detectors (see {@link #setDetectors(List)}).
     *
     * @return Detectors.
     */
    public List<SplitBrainDetector> getDetectors() {
        return detectors;
    }

    /**
     * Sets the list of detectors that should be invoked during each {@link #isValid(ClusterNode)} check.
     *
     * <p>
     * If the specified list is {@code null} or empty then all {@link #isValid(ClusterNode)} checks will always return {@code true}.
     * </p>
     *
     * @param detectors Detectors.
     */
    public void setDetectors(List<SplitBrainDetector> detectors) {
        this.detectors = detectors;
    }

    /**
     * Fluent-style version of {@link #setDetectors(List)}.
     *
     * @param detector Detector.
     *
     * @return This instance.
     */
    public SplitBrainDetectorGroup withDetector(SplitBrainDetector detector) {
        if (detectors == null) {
            detectors = new ArrayList<>();
        }

        detectors.add(detector);

        return this;
    }

    /**
     * Returns the policy of this group (see {@link #setGroupPolicy(GroupPolicy)}).
     *
     * @return Policy.
     */
    public GroupPolicy getGroupPolicy() {
        return groupPolicy;
    }

    /**
     * Sets the group policy.
     *
     * <p>
     * This parameter is mandatory and can't be {@code null}. Default value is {@link GroupPolicy#ANY_VALID}.
     * </p>
     *
     * @param groupPolicy Group policy.
     */
    public void setGroupPolicy(GroupPolicy groupPolicy) {
        ConfigCheck.get(SplitBrainDetectorGroup.class).notNull(groupPolicy, "group policy");

        this.groupPolicy = groupPolicy;
    }

    /**
     * Fluent-style version of {@link #setGroupPolicy(GroupPolicy)}.
     *
     * @param groupPolicy Group policy.
     *
     * @return This instance.
     */
    public SplitBrainDetectorGroup withGroupPolicy(GroupPolicy groupPolicy) {
        setGroupPolicy(groupPolicy);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
