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

package io.hekate.coordinate;

import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.DefaultServiceFactory;
import io.hekate.core.service.Service;
import java.util.List;

/**
 * <span class="startHere">&laquo; start here</span>Main entry point to distributed coordination API.
 *
 * <h2>Overview</h2>
 * <p>
 * {@link CoordinationService} provides support for implementing different coordination protocols among a set of cluster members. Such
 * protocols can perform application-specific rebalancing of a distributed data, node roles assignment or any other logic that requires a
 * coordinated agreement among multiple cluster members based on the consistent cluster topology (i.e. when all members have the same
 * consistent view of the cluster topology).
 * </p>
 *
 * <p>
 * Coordination process is triggered by the {@link CoordinationService} every time when cluster topology changes (i.e. when a new node
 * joins or an existing node leaves the cluster). Upon such event one of the cluster members is selected to be the process coordinator and
 * starts exchanging messages with all other participating nodes until coordination process is finished or is interrupted by a concurrent
 * cluster event.
 * </p>
 *
 * <ul>
 * <li><a href="#service_configuration">Service Configuration</a></li>
 * <li><a href="#coordination_handler">Coordination Handler</a></li>
 * <li><a href="#messaging">Messaging</a></li>
 * <li><a href="#topology_changes">Topology Changes</a></li>
 * <li><a href="#awaiting_for_initial_coordination">Awaiting for Initial Coordination</a></li>
 * <li><a href="#thread_management">Thread Management</a></li>
 * </ul>
 *
 * <a name="service_configuration"></a>
 * <h2>Service Configuration</h2>
 * <p>
 * {@link CoordinationService} can be registered and configured in {@link HekateBootstrap} with the help of {@link
 * CoordinationServiceFactory} as shown in the example below:
 * </p>
 *
 * <div class="tabs">
 * <ul>
 * <li><a href="#configure-java">Java</a></li>
 * <li><a href="#configure-xsd">Spring XSD</a></li>
 * <li><a href="#configure-bean">Spring bean</a></li>
 * </ul>
 * <div id="configure-java">
 * ${source: coordinate/CoordinationServiceJavadocTest.java#configure}
 * </div>
 * <div id="configure-xsd">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: coordinate/service-xsd.xml#example}
 * </div>
 * <div id="configure-bean">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: coordinate/service-bean.xml#example}
 * </div>
 * </div>
 *
 * <a name="coordination_handler"></a>
 * <h2>Coordination Handler</h2>
 * <p>
 * Application-specific logic of a distributed coordination process must be encapsulated into an implementation of {@link
 * CoordinationHandler} interface.
 * </p>
 *
 * <p>
 * When {@link CoordinationService} start a new coordination process, it selects one of cluster nodes to be the coordinator and calls its
 * {@link CoordinationHandler#coordinate(CoordinatorContext)} method together with a coordination context object. This object provides
 * information about the coordination participants and provides methods for sending/receiving coordination requests to/from them.
 * All other nodes besides the coordinator will stay idle and will wait for requests from the coordinator.
 * </p>
 *
 * <p>
 * Once coordination is completed and each of coordination participants reaches its final state (according to an application logic) then
 * coordinator must explicitly notify the {@link CoordinationService} by calling the {@link CoordinatorContext#complete()} method.
 * </p>
 *
 * <h3>Example</h3>
 * <p>
 * The code example below shows how {@link CoordinationHandler} can be implemented in order to perform distributed coordination of cluster
 * members. For the sake of brevity the coordination scenario is very simple and has the goal of executing some application-specific logic
 * on each of the coordinated members with each member holding an exclusive local lock. The coordination protocol can be described as
 * follows:
 * </p>
 * <ol>
 * <li>Ask all members to acquire local lock and await for confirmation from each member</li>
 * <li>Ask all members to execute their application-specific logic and await for confirmation from each member</li>
 * <li>Ask all members to release their local locks</li>
 * </ol>
 *
 * <p>
 * ${source: coordinate/CoordinationServiceJavadocTest.java#handler}
 * </p>
 *
 * <a name="messaging"></a>
 * <h2>Messaging</h2>
 * <p>
 * {@link CoordinationService} provides support for asynchronous message exchange among the coordination participants. It can be done via
 * the following methods:
 * </p>
 * <ul>
 * <li>
 * {@link CoordinationContext#broadcast(Object, CoordinationBroadcastCallback)} - broadcast the same request to all members at once
 * </li>
 * <li>{@link CoordinationMember#request(Object, CoordinationRequestCallback)} - send request to individual member</li>
 * </ul>
 *
 * <p>
 * When some node receives a request (either from the coordinator or from some other node) then its
 * {@link CoordinationHandler#process(CoordinationRequest, CoordinationContext)} method gets called. Implementations of this method must
 * perform their application-specific logic based on the request payload and send back a response via the
 * {@link CoordinationRequest#reply(Object)} method.
 * </p>
 *
 * <p>
 * All messages of a coordination process are guaranteed to be send and received with the same consistent cluster topology (i.e.
 * both sender and receiver has exactly the same cluster topology view). If topology mismatch is detected between the sender and the
 * receiver then {@link CoordinationService} will transparently send a retry response back to the sender so that it could retry sending
 * later once its topology gets consistent with the receiver or cancel the coordination process and restart it with a more up to date
 * cluster topology.
 * </p>
 *
 * <a name="topology_changes"></a>
 * <h2>Topology Changes</h2>
 * <p>
 * If topology change happens while coordination process is still running then {@link CoordinationService} will try to cancel the current
 * process via {@link CoordinationHandler#cancel(CoordinationContext)} method and will start a new coordination process.
 * Implementations of the {@link CoordinationHandler} interface are required to stop all activities of the current coordination process as
 * soon as possible.
 * </p>
 *
 * <p>
 * In order to perform early detection of a cancelled coordination process please consider using the {@link
 * CoordinationContext#isCancelled()} method. If this method returns {@code true} then this context is not valid and should not be used
 * any more.
 * </p>
 *
 * <p>
 * In order to simplify handling of concurrent coordination processes it is recommended for implementations of the {@link
 * CoordinationHandler} interface to minimize state that should be held in each handler instance. If coordination logic requires some
 * transitional state to be kept during the coordination process then please consider keeping it as an attachment object of {@link
 * CoordinationContext} instance (see {@link CoordinationContext#setAttachment(Object)}/{@link CoordinationContext#getAttachment()}).
 * </p>
 *
 * <a name="awaiting_for_initial_coordination"></a>
 * <h2>Awaiting for Initial Coordination</h2>
 * <p>
 * Sometimes it is required for applications to await for initial coordination process to complete before proceeding to their main
 * tasks (f.e. if application needs to know which data partitions or roles were assigned to its node by some imaginary
 * coordination process when node joined the cluster).
 * </p>
 *
 * <p>
 * This can be done by obtaining a future object via {@link #futureOf(String)} method. This future object will be notified right after the
 * coordination process gets executed for the first time and can be used to {@link CoordinationFuture#get()} await} for its completion as
 * in the example below:
 * ${source: coordinate/CoordinationServiceJavadocTest.java#future}
 * </p>
 *
 * <a name="thread_management"></a>
 * <h2>Thread Management</h2>
 * <p>
 * Each {@link CoordinationHandler} instance is bound to a single thread that is managed by the {@link CoordinationService}. All
 * coordination and messaging callbacks get processed on that thread sequentially in order to simplify asynchronous operations handling and
 * prevent concurrency issues.
 * </p>
 *
 * <p>
 * If particular {@link CoordinationHandler}'s operation takes long time to complete then it is recommended to use a separate thread
 * pool to offload such operations from the main coordination thread. Otherwise such operations will block subsequent notification from the
 * {@link CoordinationService} and will negatively impact on the overall coordination performance.
 * </p>
 *
 * @see CoordinationServiceFactory
 */
@DefaultServiceFactory(CoordinationServiceFactory.class)
public interface CoordinationService extends Service {
    /**
     * Returns all processes that are {@link CoordinationServiceFactory#setProcesses(List) registered} within this service.
     *
     * @return Processes or an empty list if there are no registered processes.
     */
    List<CoordinationProcess> allProcesses();

    /**
     * Returns a coordination process for the specified name.
     *
     * @param name Process name (see {@link CoordinationProcessConfig#setName(String)}).
     *
     * @return Coordination process.
     */
    CoordinationProcess process(String name);

    /**
     * Returns {@code true} if this service has a coordination process with the specified name.
     *
     * @param name Process name (see {@link CoordinationProcessConfig#setName(String)}).
     *
     * @return {@code true} if process exists.
     */
    boolean hasProcess(String name);

    /**
     * Returns an initial coordination future for the specified process name. The returned future object will be completed once the
     * coordination processes gets executed for the very first time on this node.
     *
     * @param process Process name (see {@link CoordinationProcessConfig#setName(String)}).
     *
     * @return Initial coordination future.
     */
    CoordinationFuture futureOf(String process);
}
