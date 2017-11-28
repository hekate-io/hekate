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
import io.hekate.messaging.MessagingBackPressureConfig;
import io.hekate.messaging.MessagingOverflowPolicy;
import io.hekate.spring.bean.task.TaskServiceBean;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import io.hekate.spring.boot.HekateConfigurer;
import io.hekate.task.TaskService;
import io.hekate.task.TaskServiceFactory;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * <span class="startHere">&laquo; start here</span>Auto-configuration for {@link TaskService}.
 *
 * <h2>Configuration properties</h2>
 * <p>
 * It is possible to configure {@link TaskServiceFactory} via application properties prefixed with {@code 'hekate.task'}.
 * For example:
 * </p>
 * <ul>
 * <li>{@link TaskServiceFactory#setServerMode(boolean) 'hekate.task.server-mode'}</li>
 * <li>{@link TaskServiceFactory#setWorkerThreads(int) 'hekate.task.worker-threads'}</li>
 * <li>{@link TaskServiceFactory#setNioThreads(int) 'hekate.task.nio-threads'}</li>
 * <li>{@link TaskServiceFactory#setIdleSocketTimeout(long) 'hekate.task.idle-socket-timeout'}</li>
 * <li>{@link MessagingBackPressureConfig#setInLowWatermark(int) 'hekate.task.back-bressure.in-low-watermark'}</li>
 * <li>{@link MessagingBackPressureConfig#setInHighWatermark(int) 'hekate.task.back-bressure.in-high-watermark'}</li>
 * <li>{@link MessagingBackPressureConfig#setOutLowWatermark(int) 'hekate.task.back-bressure.out-low-watermark'}</li>
 * <li>{@link MessagingBackPressureConfig#setOutHighWatermark(int) 'hekate.task.back-bressure.out-high-watermark'}</li>
 * <li>
 * {@link MessagingBackPressureConfig#setOutOverflowPolicy(MessagingOverflowPolicy) 'hekate.task.back-bressure.out-overflow-policy'}
 * </li>
 * </ul>
 *
 * @see TaskService
 * @see HekateConfigurer
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateConfigurer.class)
@ConditionalOnMissingBean(TaskServiceFactory.class)
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
     * {@link Hekate#tasks()} method.
     *
     * @return Service bean.
     */
    @Bean
    public TaskServiceBean taskService() {
        return new TaskServiceBean();
    }
}
