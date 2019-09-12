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

import io.hekate.cluster.ClusterNodeJmx;
import io.hekate.core.jmx.JmxTypeName;
import javax.management.MXBean;

/**
 * JMX interface for {@link Candidate}.
 */
@MXBean
@JmxTypeName("Candidate")
public interface CandidateJmx {
    /**
     * Returns the value of {@link CandidateConfig#setGroup(String)}.
     *
     * @return Value of {@link CandidateConfig#setGroup(String)}.
     */
    String getGroup();

    /**
     * Returns the class name of {@link CandidateConfig#setCandidate(Candidate)}.
     *
     * @return Class name of {@link CandidateConfig#setCandidate(Candidate)}.
     */
    String getCandidateType();

    /**
     * Returns {@code true} if local node is the leader of this group.
     *
     * @return {@code true} if local node is the leader of this group.
     */
    boolean isLeader();

    /**
     * Returns the leader node or {@code null} if leader is not elected yet.
     *
     * @return Leader node or {@code null} if leader is not elected yet.
     */
    ClusterNodeJmx getLeaderNode();
}
