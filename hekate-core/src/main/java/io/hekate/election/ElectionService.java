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

import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.DefaultServiceFactory;
import io.hekate.core.service.Service;
import io.hekate.lock.LockService;
import java.util.List;

/**
 * <span class="startHere">&laquo; start here</span>Main entry point to leader election API.
 *
 * <h2>Overview</h2>
 * <p>
 * This service provides support for electing a leader among a set of cluster nodes that belong to the same election group. This service
 * guarantees that only one node within each group will be elected as a leader and all other nodes will become followers. If leader node
 * goes down or decides to yield its leadership then some other node will be elected as a new leader.
 * </p>
 *
 * <p>
 * In order to participate in leader election process, each node must {@link ElectionServiceFactory#setCandidates(List) register} an
 * implementation of the {@link Candidate} interface and specify its {@link CandidateConfig#setGroup(String) group name}. Each node can
 * register candidates to multiple groups and {@link ElectionService} will make sure that only one node within each group will become a
 * leader.
 * </p>
 *
 * <h2>Service Configuration</h2>
 * <p>
 * {@link ElectionService} can be registered and configured in {@link HekateBootstrap} with the help of {@link
 * ElectionServiceFactory} as shown in the example below:
 * </p>
 *
 * <div class="tabs">
 * <ul>
 * <li><a href="#configure-java">Java</a></li>
 * <li><a href="#configure-xsd">Spring XSD</a></li>
 * <li><a href="#configure-bean">Spring bean</a></li>
 * </ul>
 * <div id="configure-java">
 * ${source: election/ElectionServiceJavadocTest.java#configure}
 * </div>
 * <div id="configure-xsd">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: election/service-xsd.xml#example}
 * </div>
 * <div id="configure-bean">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: election/service-bean.xml#example}
 * </div>
 * </div>
 *
 * <h2>Leader Election</h2>
 * <p>
 * Leader election process starts right after the node joins the cluster. If leader has been already elected by that time then {@link
 * Candidate} switches to the {@link Candidate#becomeFollower(FollowerContext) folower} state. If this is the first node in the election
 * group then {@link Candidate} switches to the {@link Candidate#becomeLeader(LeaderContext) leader} state. If multiple nodes are joining
 * the cluster at the same time then only one of them will be elected as a leader and all other nodes will switch to the follower state.
 * </p>
 *
 * <p>
 * If {@link Candidate} won elections and became a group leader then it will remain in this state until its cluster node is
 * {@link Hekate#leave() stopped} or until it yields its leadership by calling {@link LeaderContext#yieldLeadership()}. In the first case
 * some other node will be elected as a new leader and will be notified via {@link Candidate#becomeLeader(LeaderContext)} method. In the
 * second case leadership will be withdrawn and with high probability some other node will become a new leader. If no other node could win
 * elections then the same node will become leader again and its {@link Candidate#becomeLeader(LeaderContext)} method will be called.
 * </p>
 *
 * <p>
 * When {@link Candidate} switches to the follower state then it can optionally register a leader change listener via {@link
 * FollowerContext#addListener(LeaderChangeListener)}. This listener will be notified every time when leadership gets
 * transferred from one remote node to another remote node.
 * </p>
 *
 * <p>
 * Below is the example of {@link Candidate} interface implementation:
 * ${source: election/ElectionServiceJavadocTest.java#candidate}
 * </p>
 *
 * <h2>Leader Election Details</h2>
 * <p>
 * Leader elections are based on the {@link LockService} capabilities. During the startup {@link ElectionService} tries to asynchronously
 * acquire a distributed lock for each of its registered groups. If lock acquisition is successful then {@link Candidate} of such group
 * gets notified on becoming a leader. If lock is busy then {@link Candidate} get notified on becoming a follower and continues to await for
 * the lock to be acquired.
 * </p>
 *
 * @see ElectionServiceFactory
 */
@DefaultServiceFactory(ElectionServiceFactory.class)
public interface ElectionService extends Service {
    /**
     * Returns a future object that can be used to get the current leader of the specified group.
     *
     * @param group Group name (see {@link CandidateConfig#setGroup(String)}).
     *
     * @return Future object for getting the current leader.
     *
     * @throws IllegalStateException If service is stopped.
     */
    LeaderFuture leader(String group) throws IllegalStateException;
}
