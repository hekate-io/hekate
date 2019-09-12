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

package io.hekate.core.jmx;

import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.DefaultServiceFactory;
import io.hekate.core.service.Service;
import java.util.List;
import java.util.Optional;
import javax.management.MBeanServer;
import javax.management.MXBean;
import javax.management.ObjectName;

/**
 * <span class="startHere">&laquo; start here</span>Java Management Extensions service.
 *
 * <h2>Overview</h2>
 * <p>
 * The {@link JmxService} is the utility service that provides an abstraction layer on top of JMX facilities that are provided by the JMV.
 * This service is primarily intended to be used by the {@link Hekate}'s internal components and services.
 * </p>
 *
 * <h2>Service Configuration</h2>
 * <p>
 * {@link JmxService} can be registered and configured in {@link HekateBootstrap} with the help of {@link JmxServiceFactory} as shown in
 * the example below:
 * </p>
 * <div class="tabs">
 * <ul>
 * <li><a href="#configure-java">Java</a></li>
 * <li><a href="#configure-xsd">Spring XSD</a></li>
 * <li><a href="#configure-bean">Spring bean</a></li>
 * </ul>
 * <div id="configure-java">
 * ${source: core/jmx/JmxServiceJavadocTest.java#configure}
 * </div>
 * <div id="configure-xsd">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: core/jmx/service-xsd.xml#example}
 * </div>
 * <div id="configure-bean">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: core/jmx/service-bean.xml#example}
 * </div>
 * </div>
 *
 * <h2>Accessing the Service</h2>
 * <p>
 * {@link JmxService} is an optional service that is available only if application registers an instance of {@link JmxServiceFactory}
 * within
 * the {@link HekateBootstrap}. Availability of this service can be checked at runtime via the {@link Hekate#has(Class)} method. If service
 * is available then it can be accessed via the {@link Hekate#get(Class)} method as shown in the example below:
 * ${source: core/jmx/JmxServiceJavadocTest.java#access}
 * </p>
 *
 * <h2>Registering MXBeans</h2>
 * <p>
 * JMX objects can be registered to the {@link JmxService} via {@link #register(Object)} method. Such objects must implement exactly one
 * {@link MXBean}-annotated interface (i.e. 'MXBean' suffix in interface name is not supported). Alternatively, instead of implementing
 * such interface directly, objects can implement {@link JmxSupport} interface. In such case the object that is returned by the {@link
 * JmxSupport#jmx()} method will be registered as an MX bean.
 * </p>
 *
 * <h2>JMX Object Names</h2>
 * <p>
 * {@link ObjectName}s of all registered beans are constructed based on the {@link HekateBootstrap#setClusterName(String) cluster name},
 * {@link HekateBootstrap#setNodeName(String) node name} and bean's class name as follows:
 * </p>
 *
 * <p>
 * {@code [domain]:type=[simple_class_name],name=[name_attribute]}
 * </p>
 *
 * <ul>
 * <li>
 * {@code [domain]} - value of {@link JmxServiceFactory#setDomain(String)}
 * </li>
 * <li>
 * {@code [simple_class_name]} - {@link Class#getSimpleName()} of the JMX bean object
 * </li>
 * <li>
 * {@code [name_attribute]} - (optional) value of {@code nameAttribute} if bean is registered via {@link #register(Object, String)}.
 * </li>
 * </ul>
 */
@DefaultServiceFactory(JmxServiceFactory.class)
public interface JmxService extends Service {
    /**
     * Returns the JMX domain of this service.
     *
     * @return JMX domain.
     *
     * @see JmxServiceFactory#setDomain(String)
     */
    String domain();

    /**
     * Returns the list of names of all MBeans that are registered to this service.
     *
     * @return List of all registered MBean names or an empty list if none are registered.
     */
    List<ObjectName> names();

    /**
     * Constructs a new object name for the specified JMX interface.
     *
     * @param jmxInterface JMX interface.
     *
     * @return Object name.
     */
    ObjectName nameFor(Class<?> jmxInterface);

    /**
     * Constructs a new object name for the specified JMX interface and an additional {@code 'name'} attribute.
     *
     * @param jmxInterface JMX interface.
     * @param nameAttribute Optional value for the {@code 'name'} attribute of the resulting object name. If omitted then {@code 'name'}
     * attribute will not be added to the object name.
     *
     * @return Object name.
     */
    ObjectName nameFor(Class<?> jmxInterface, String nameAttribute);

    /**
     * Registers the specified object as {@link MXBean}.
     *
     * <p>
     * The specified object is expected to implement exactly one interface that is marked with @{@link MXBean} annotation or implement the
     * {@link JmxSupport} interface. If object doesn't implement any of those interfaces then nothing will happen and registration operation
     * will be ignored.
     * </p>
     *
     * @param mxBean JMX bean.
     *
     * @return Object name if registration took place.
     *
     * @throws JmxServiceException Signals that JMX registration failed.
     */
    Optional<ObjectName> register(Object mxBean) throws JmxServiceException;

    /**
     * Registers the specified object as {@link MXBean}.
     *
     * <p>
     * The specified object is expected to implement exactly one interface that is marked with @{@link MXBean} annotation or implement the
     * {@link JmxSupport} interface. If object doesn't implement any of those interfaces then nothing will happen and registration operation
     * will be ignored.
     * </p>
     *
     * @param mxBean JMX bean.
     * @param nameAttribute Optional value for the {@code 'name'} attribute of the resulting object name. If omitted then {@code 'name'}
     * attribute will not be added to the object name.
     *
     * @return Object name if registration took place.
     *
     * @throws JmxServiceException Signals that JMX registration failed.
     */
    Optional<ObjectName> register(Object mxBean, String nameAttribute) throws JmxServiceException;

    /**
     * Returns the MBean server.
     *
     * @return MBean server.
     *
     * @see JmxServiceFactory#setServer(MBeanServer)
     */
    MBeanServer server();
}
