/*
 * Copyright 2017 The Hekate Project
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

package io.hekate.spring.boot.task;

import io.hekate.core.Hekate;
import io.hekate.spring.bean.task.TaskServiceBean;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import io.hekate.spring.boot.HekateConfigurer;
import io.hekate.task.TaskService;
import io.hekate.task.TaskServiceFactory;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * <span class="startHere">&laquo; start here</span>Auto-configuration for {@link TaskService}.
 *
 * <h2>Overview</h2>
 * <p>
 * This auto-configuration constructs a {@link Bean} of {@link TaskServiceFactory} type. This auto-configuration is disabled by default and
 * should be explicitly enabled by setting {@code 'hekate.task.enable'} configuration property to {@code true}.
 * </p>
 *
 * <h2>Configuration properties</h2>
 * <p>
 * It is possible to configure {@link TaskServiceFactory} via application properties prefixed with {@code 'hekate.task'}.
 * For example:
 * </p>
 * <ul>
 * <li>{@link TaskServiceFactory#setWorkerThreads(int) 'hekate.task.worker-thread-pool-size'}</li>
 * <li>{@link TaskServiceFactory#setSockets(int) 'hekate.task.socket-pool-size'}</li>
 * <li>{@link TaskServiceFactory#setNioThreads(int) 'hekate.task.nio-thread-pool-size'}</li>
 * <li>{@link TaskServiceFactory#setIdleSocketTimeout(int) 'hekate.task.idle-socket-pool-timeout'}</li>
 * </ul>
 *
 * @see TaskService
 * @see HekateConfigurer
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateConfigurer.class)
@ConditionalOnMissingBean(TaskServiceFactory.class)
@ConditionalOnProperty(value = "hekate.task.enable", havingValue = "true")
public class HekateTaskServiceConfigurer {
    /**
     * Constructs the {@link TaskServiceFactory}.
     *
     * @return Service factory.
     */
    @Bean
    @ConfigurationProperties(prefix = "hekate.task")
    public TaskServiceFactory taskServiceFactory() {
        return new TaskServiceFactory();
    }

    /**
     * Returns the factory bean that makes it possible to inject {@link TaskService} directly into other beans instead of accessing it via
     * {@link Hekate#get(Class)} method.
     *
     * @return Service bean.
     */
    @Bean
    public TaskServiceBean taskService() {
        return new TaskServiceBean();
    }
}
