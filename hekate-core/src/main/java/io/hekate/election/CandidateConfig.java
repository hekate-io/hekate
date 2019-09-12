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

package io.hekate.election;

import io.hekate.util.format.ToString;
import java.util.List;

/**
 * Configuration for {@link Candidate}.
 *
 * <p>
 * Instances of this class can be {@link ElectionServiceFactory#setCandidates(List) registered} within the {@link
 * ElectionServiceFactory}.
 * </p>
 *
 * <p>
 * For more details about leader elections please see the documentation of the {@link ElectionService} interface.
 * </p>
 *
 * @see ElectionServiceFactory#withCandidate(CandidateConfig)
 */
public class CandidateConfig {
    private String group;

    private Candidate candidate;

    /**
     * Default constructor.
     */
    public CandidateConfig() {
        // No-op.
    }

    /**
     * Constructs new instance.
     *
     * @param group Group name (see {@link #setGroup(String)}).
     */
    public CandidateConfig(String group) {
        this.group = group;
    }

    /**
     * Returns the election group name (see {@link #setGroup(String)}).
     *
     * @return Election group name.
     */
    public String getGroup() {
        return group;
    }

    /**
     * Sets the election group name. Can contain only alpha-numeric characters and non-repeatable dots/hyphens.
     *
     * <p>
     * Only one {@link Candidate candidate} among all candidates that are registered with the same group name will be
     * elected as a leader and all other candidates will remain in the follower state. If candidates are registered with different group
     * names then they will form completely independent groups with each group having its own leader.
     * </p>
     *
     * <p>
     * Value of this parameter is mandatory and must be unique across all configurations registered within the {@link
     * ElectionServiceFactory}.
     * </p>
     *
     * @param group Group name (can contain only alpha-numeric characters and non-repeatable dots/hyphens).
     */
    public void setGroup(String group) {
        this.group = group;
    }

    /**
     * Fluent-style version of {@link #setGroup(String)}.
     *
     * @param group Group name.
     *
     * @return This instance.
     */
    public CandidateConfig withGroup(String group) {
        setGroup(group);

        return this;
    }

    /**
     * Returns the leader election candidate (see {@link #setCandidate(Candidate)}).
     *
     * @return Leader election candidate.
     */
    public Candidate getCandidate() {
        return candidate;
    }

    /**
     * Sets the leader election candidate that should be notified upon {@link #setGroup(String) group} leader election results.
     *
     * @param candidate Leader election candidate.
     */
    public void setCandidate(Candidate candidate) {
        this.candidate = candidate;
    }

    /**
     * Fluent-style version of {@link #setCandidate(Candidate)}.
     *
     * @param candidate Leader election candidate.
     *
     * @return This instance.
     */
    public CandidateConfig withCandidate(Candidate candidate) {
        setCandidate(candidate);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
